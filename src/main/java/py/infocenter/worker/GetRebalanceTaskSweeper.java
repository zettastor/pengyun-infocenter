/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.infocenter.worker;

import com.google.common.collect.Multimap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.common.Constants;
import py.common.PyService;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.InformationCenterAppConfig;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.struct.BaseRebalanceTask;
import py.infocenter.rebalance.struct.SendRebalanceTask;
import py.infocenter.rebalance.struct.SimulateSegmentUnit;
import py.infocenter.store.VolumeStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.thrift.share.ArbiterMigrateThrift;
import py.thrift.share.PrimaryMigrateThrift;
import py.thrift.share.RebalanceTaskListThrift;
import py.thrift.share.SecondaryMigrateThrift;
import py.thrift.share.SegIdThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * the worker will get all instance rebalance task.
 */
public class GetRebalanceTaskSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(GetRebalanceTaskSweeper.class);

  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private VolumeInformationManger volumeInformationManger;
  private VolumeStore volumeStore;
  private InstanceStore instanceStore;
  private InformationCenterAppConfig informationCenterAppConfig;
  private Map<Long, GetRebalanceTasks> instance2RebalanceTaskListMap;



  public GetRebalanceTaskSweeper(SegmentUnitsDistributionManager segmentUnitsDistributionManager,
      VolumeInformationManger volumeInformationManger, VolumeStore volumeStore,
      InstanceStore instanceStore, InformationCenterAppConfig informationCenterAppConfig) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
    this.volumeInformationManger = volumeInformationManger;
    this.volumeStore = volumeStore;
    this.instanceStore = instanceStore;
    this.informationCenterAppConfig = informationCenterAppConfig;

    this.instance2RebalanceTaskListMap = new ConcurrentHashMap<>();
  }

  @Override
  public void doWork() throws Exception {
    if (informationCenterAppConfig.appContext().getStatus() != InstanceStatus.HEALTHY) {
      logger.info("i'm fellow now, cannot do rebalance task sweeper");
      return;
    }

    logger.debug("get rebalance task sweeper running");

    //remove expired tasks
    removeExpiredTasks();

    Set<Instance> datanodeInstanceSet = instanceStore
        .getAll(PyService.DATANODE.getServiceName(), InstanceStatus.HEALTHY);
    for (Instance instance : datanodeInstanceSet) {
      long instanceId = instance.getId().getId();
      if (instance2RebalanceTaskListMap.containsKey(instanceId)) {
        logger.debug("instance:{} already have rebalance tasks", instanceId);
        continue;
      }

      //if task not exists, get rebalance tasks
      RebalanceTaskListThrift rebalanceTaskListThrift = getRebalanceTasks(instance.getId());
      logger.debug("get instance:{} rebalance tasks thrift:{}", instance.getId(),
          rebalanceTaskListThrift);

      //save
      instance2RebalanceTaskListMap
          .putIfAbsent(instanceId, new GetRebalanceTasks(instanceId, rebalanceTaskListThrift));
    }
  }

  /**
   * pull the tasks.
   *
   * @param instanceId instance is
   * @return the rebalance task list
   */
  public RebalanceTaskListThrift pullTask(long instanceId) {
    if (!instance2RebalanceTaskListMap.containsKey(instanceId)) {
      logger.debug("not found rebalance tasks of instance:{}", instanceId);
      return new RebalanceTaskListThrift();
    }

    GetRebalanceTasks getRebalanceTasks = instance2RebalanceTaskListMap.remove(instanceId);
    if (getRebalanceTasks == null) {
      logger.warn(
          "found rebalance tasks of instance:{}, but cannot pull any tasks. may be pull it by "
              + "other",
          instanceId);
      return new RebalanceTaskListThrift();
    }

    return getRebalanceTasks.getRebalanceTaskListThrift();
  }

  private void removeExpiredTasks() {
    Iterator it = instance2RebalanceTaskListMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long, GetRebalanceTasks> entry = (Map.Entry<Long, GetRebalanceTasks>) it.next();
      long instanceId = entry.getKey();
      GetRebalanceTasks getRebalanceTasks = entry.getValue();

      if (getRebalanceTasks == null) {
        logger.warn(
            "found rebalance tasks of instance:{}, but cannot get any tasks, may be pull it by "
                + "other",
            instanceId);
        instance2RebalanceTaskListMap.remove(instanceId);
      } else if (getRebalanceTasks.isExpired()) {
        logger.warn("the get rebalance task of instance:{} is expired, will drop these tasks",
            instanceId);
        instance2RebalanceTaskListMap.remove(instanceId);
      }
    }
  }

  /**
   * get rebalance tasks of the migrate source as instance.
   *
   * @param instanceId migrate source instance
   * @return rebalance task list
   */
  private RebalanceTaskListThrift getRebalanceTasks(InstanceId instanceId) {
    RebalanceTaskListThrift rebalanceTaskListThrift = new RebalanceTaskListThrift();
    logger.warn("now will get rebalance tasks of instance:{}", instanceId);

    try {
      Multimap<Long, SendRebalanceTask> rebalanceTaskList = segmentUnitsDistributionManager
          .selectRebalanceTasks(instanceId);
      for (Entry<Long, SendRebalanceTask> entry : rebalanceTaskList.entries()) {
        long volumeId = entry.getKey();
        SendRebalanceTask rebalanceTask = entry.getValue();
        SimulateSegmentUnit sourceSegmentUnit = rebalanceTask.getSourceSegmentUnit();
        SegId segId = sourceSegmentUnit.getSegId();
        SegIdThrift segIdThrift = RequestResponseHelper.buildThriftSegIdFrom(segId);
        long srcId = rebalanceTask.getInstanceToMigrateFrom().getId();
        long destId = rebalanceTask.getDestInstanceId().getId();

        if (rebalanceTask.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {
          rebalanceTaskListThrift
              .addToPrimaryMigrateList(new PrimaryMigrateThrift(segIdThrift, srcId, destId));
        } else if (rebalanceTask.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PSRebalance) {
          VolumeMetadata volumeMetadata = null;
          try {

            //check volume status
            volumeMetadata = volumeStore.getVolume(volumeId);
            if (VolumeStatus.Available.equals(volumeMetadata.getVolumeStatus())
                || VolumeStatus.Stable
                .equals(volumeMetadata.getVolumeStatus())) {
              volumeMetadata = volumeInformationManger
                  .getVolumeNew(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
            } else {
              logger.warn("when getRebalanceTasks, the volume :{} {} status :{}, no need check",
                  volumeId,
                  volumeMetadata.getName(), volumeMetadata.getVolumeStatus());
              continue;
            }

          } catch (VolumeNotFoundException e) {
            logger.warn("when getRebalanceTasks, can not find the volume :{}", volumeId);
            continue;
          }

          SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(segIdThrift,
              srcId,
              destId, sourceSegmentUnit.getVolumeType().getVolumeTypeThrift(),
              null,
              volumeMetadata.getStoragePoolId(),
              sourceSegmentUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
              volumeMetadata.getSegmentWrappCount(),
              sourceSegmentUnit.isEnableLaunchMultiDrivers(),
              sourceSegmentUnit.getVolumeSource().getVolumeSourceThrift());

          secondaryMigrateThrift.setInitMembership(RequestResponseHelper
              .buildThriftMembershipFrom(sourceSegmentUnit.getSegId(),
                  volumeMetadata.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

          rebalanceTaskListThrift.addToSecondaryMigrateList(secondaryMigrateThrift);
        } else if (rebalanceTask.getTaskType()
            == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance) {
          VolumeMetadata volumeMetadata = null;
          try {
            volumeMetadata = volumeInformationManger
                .getVolumeNew(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
          } catch (VolumeNotFoundException e) {
            logger.warn("when getRebalanceTasks, can not find the volume :{}", volumeId);
            continue;
          }

          ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(segIdThrift,
              srcId, destId,
              sourceSegmentUnit.getVolumeType().getVolumeTypeThrift(),
              null,
              volumeMetadata.getStoragePoolId(),
              sourceSegmentUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
              volumeMetadata.getSegmentWrappCount(),
              sourceSegmentUnit.isEnableLaunchMultiDrivers(),
              sourceSegmentUnit.getVolumeSource().getVolumeSourceThrift());

          arbiterMigrateThrift.setInitMembership(RequestResponseHelper
              .buildThriftMembershipFrom(sourceSegmentUnit.getSegId(),
                  volumeMetadata.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

          rebalanceTaskListThrift.addToArbiterMigrateList(arbiterMigrateThrift);
        }
      }
    } catch (NoNeedToRebalance e) {
      logger.debug("instance:{} has no task of rebalance", instanceId);
    }

    return rebalanceTaskListThrift;
  }


  public class GetRebalanceTasks {

    private long timestamp; //the task generate timestamp
    private long instanceId;
    private RebalanceTaskListThrift rebalanceTaskListThrift;



    public GetRebalanceTasks(long instanceId, RebalanceTaskListThrift rebalanceTaskListThrift) {
      this.timestamp = System.currentTimeMillis();
      this.instanceId = instanceId;
      this.rebalanceTaskListThrift = rebalanceTaskListThrift;
    }

    public long getInstanceId() {
      return instanceId;
    }

    public RebalanceTaskListThrift getRebalanceTaskListThrift() {
      return rebalanceTaskListThrift;
    }



    public boolean isExpired() {
      long currentTime = System.currentTimeMillis();
      if ((currentTime - timestamp) > informationCenterAppConfig
          .getGetRebalanceTaskExpireTimeMs()) {
        logger.warn("the rebalance task of instance:{} is expired", instanceId);
        return true;
      }
      return false;
    }
  }

}
