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

package py.infocenter.rebalance;

import static py.icshare.InstanceMetadata.DatanodeStatus.OK;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitType;
import py.common.Constants;
import py.common.PyService;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.exception.VolumeNotFoundException;
import py.icshare.qos.RebalanceRule;
import py.icshare.qos.RebalanceRuleInformation;
import py.icshare.qos.RebalanceRuleStore;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.builder.InstanceInfoCollectionBuilder;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.exception.TooManyTasksException;
import py.infocenter.rebalance.exception.VolumeCreatingException;
import py.infocenter.rebalance.selector.RebalanceSelector;
import py.infocenter.rebalance.selector.SegmentUnitReserver;
import py.infocenter.rebalance.selector.VolumeRebalanceSelector;
import py.infocenter.rebalance.struct.ComparableRebalanceTask;
import py.infocenter.rebalance.struct.RebalanceTaskManager;
import py.infocenter.rebalance.struct.SendRebalanceTask;
import py.infocenter.rebalance.struct.SimpleDatanodeManager;
import py.infocenter.rebalance.struct.SimpleRebalanceTask;
import py.infocenter.rebalance.struct.SimulateInstanceInfo;
import py.infocenter.rebalance.struct.SimulatePool;
import py.infocenter.rebalance.struct.SimulateVolume;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.io.qos.RebalanceAbsoluteTime;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.querylog.eventdatautil.EventDataWorker;
import py.rebalance.RebalanceTask;
import py.thrift.infocenter.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.DatanodeTypeNotSetExceptionThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.MemberShipChangedExceptionThrift;
import py.thrift.share.NotEnoughGroupExceptionThrift;
import py.thrift.share.NotEnoughNormalGroupExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class SegmentUnitsDistributionManagerImpl implements SegmentUnitsDistributionManager {

  private static final Logger logger = LoggerFactory
      .getLogger(SegmentUnitsDistributionManagerImpl.class);

  private final long segmentSize;
  private final VolumeStore volumeStore;
  private final StorageStore storageStore;
  private final StoragePoolStore storagePoolStore;
  private final RebalanceRuleStore rebalanceRuleStore;
  private final RebalanceConfiguration config = RebalanceConfiguration.getInstance();
  private final DomainStore domainStore;
  private final long poolCanNotRebuildForReserveSegUnit = 0L;
  /*
   * the rebalance task, instance and archives backup,
   * if instance and archives not changed, the rebalance task not be changed
   */
  private final RebalanceTaskManager rebalanceTaskManager;
  @Deprecated
  private final Multimap<InstanceId, SimpleRebalanceTask> movingOutTasks;
  @Deprecated
  private final Multimap<InstanceId, SimpleRebalanceTask> movingInTasks;
  @Deprecated
  private final Multimap<InstanceId, SimpleRebalanceTask> insideTasks;
  public Timer rebalanceTriggerTimer = new Timer("rebalance-trigger-timer-");
  private int segmentWrappCount;
  private InstanceStore instanceStore;
  private long initTime;
  private boolean isRebalanceEnable = false;
  private AppContext appContext;



  private SimpleDatanodeManager simpleDatanodeManager;
  private VolumeInformationManger volumeInformationManger;



  public SegmentUnitsDistributionManagerImpl(long segmentSize, VolumeStore volumeStore,
      StorageStore storageStore, StoragePoolStore storagePoolStore,
      RebalanceRuleStore rebalanceRuleStore, DomainStore domainStore) {
    this.segmentSize = segmentSize;
    this.volumeStore = volumeStore;
    this.storageStore = storageStore;
    this.storagePoolStore = storagePoolStore;
    this.rebalanceRuleStore = rebalanceRuleStore;
    this.domainStore = domainStore;

    this.movingOutTasks = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());
    this.movingInTasks = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());
    this.insideTasks = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());

    this.initTime = System.currentTimeMillis();

    this.simpleDatanodeManager = new SimpleDatanodeManager();
    this.rebalanceTaskManager = new RebalanceTaskManager();

    //check has volume need to do rebalance
    rebalanceTriggerTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          logger.warn("rebalance trigger checker, period:{}s", config.getRebalanceTriggerPeriod());
          selectRebalanceTasks(null);
        } catch (NoNeedToRebalance ignored) {
          logger.info("caught exception", ignored);
        }
      }
    }, config.getRebalanceTriggerPeriod() * 1000, config.getRebalanceTriggerPeriod() * 1000);
  }

  public void forceStart() {
    initTime = 0;
  }

  private boolean justStarted() {
    if (initTime != 0) {
      if (System.currentTimeMillis() - initTime < 30000) {
        return true;
      } else {
        initTime = 0;
        return false;
      }
    }
    return false;
  }

  /**
   * update simple datanode group id and instance map(groupId2InstanceIdMap), when any datanode's
   * type is SIMPLE,.
   *
   * @param instanceMetadata datanode information
   */
  @Override
  public void updateSimpleDatanodeInfo(InstanceMetadata instanceMetadata) {
    if (instanceMetadata == null) {
      logger.warn("[updateSimpleDatanodeGroupIdSet] instanceMetadata is null");
      return;
    }

    logger.debug("[updateSimpleDatanodeGroupIdSet] instanceMetadata: {}", instanceMetadata);
    boolean isSimpleDatanode = (instanceMetadata.getDatanodeType()
        == InstanceMetadata.DatanodeType.SIMPLE);

    simpleDatanodeManager.updateSimpleDatanodeInfo(instanceMetadata.getInstanceId().getId(),
        instanceMetadata.getGroup().getGroupId(), isSimpleDatanode);
  }

  /**
   * get simple datanode's instance id set.
   *
   * @return all simple datanode's instance id set
   */
  @Override
  public Set<Long> getSimpleDatanodeInstanceIdSet() {
    return simpleDatanodeManager.getSimpleDatanodeInstanceIdSet();
  }

  /**
   * just for test.
   *
   * @return simple datanode manager
   */
  public SimpleDatanodeManager getSimpleDatanodeManager() {
    return simpleDatanodeManager;
  }

  /**
   * just for test has any rebalance task of all volume.
   *
   * @return simple datanode manager
   */
  public boolean hasAnyRebalanceTasks() {
    return rebalanceTaskManager.hasAnyTasks();
  }

  /**
   * just for test get result simulate volume after rebalance.
   *
   * @return simulate volume after rebalance
   */
  public SimulateVolume getResultSimulateVolume(long volumeId) {
    return rebalanceTaskManager.getResultOfRebalance(volumeId);
  }

  /**
   * just for test is rebalance task list can used.
   *
   * @return return true, if is calculating
   */
  public boolean isRebalanceTaskUsable(long volumeId) {
    return rebalanceTaskManager.getRebalanceCalcStatus(volumeId)
        == RebalanceTaskManager.RebalanceCalcStatus.STABLE;
  }

  /**
   * just for test.
   */
  public RebalanceTaskManager getRebalanceTaskManager() {
    return rebalanceTaskManager;
  }

  /**
   * select rebalance tasks in all pools, when migrate source is instanceId.
   *
   * @param instanceId migrate source instance id; if it is null, just checking rebalance is or not
   *                   need to do
   * @return rebalance tasks (volumeId, task)
   * @throws NoNeedToRebalance if no need to rebalance, this exception will be caught
   */
  @Override
  public Multimap<Long, SendRebalanceTask> selectRebalanceTasks(InstanceId instanceId)
      throws NoNeedToRebalance {
    if (justStarted()) { // we will throw NoNeedToRebalance when info center just started cause 
      // there might be some
      // data nodes or archives or segment units not reported yet.
      throw new NoNeedToRebalance();
    }
    List<StoragePool> storagePools;
    try {
      storagePools = storagePoolStore.listAllStoragePools();
    } catch (Exception e) {
      logger.error("can not list all storage pools", e);
      throw new NoNeedToRebalance();
    }
    Validate.notNull(storagePools);

    logger.debug("now to select rebalance tasks of instance:{} if necessary", instanceId);

    Multimap<Long, SendRebalanceTask> volumeId2TaskMap = HashMultimap.create();
    for (StoragePool storagePool : storagePools) {
      try {
        volumeId2TaskMap.putAll(selectInStoragePool(storagePool, instanceId));
      } catch (NoNeedToRebalance ignored) {
        logger.info("in pool:{}, {}, not need do reBalance", storagePool.getPoolId(),
            storagePool.getName());
      }
    }

    if (volumeId2TaskMap.isEmpty()) {
      throw new NoNeedToRebalance();
    } else {
      return volumeId2TaskMap;
    }
  }

  /**
   * select rebalance tasks in pool, when migrate source is instanceId.
   *
   * @param storagePool pool
   * @param instanceId  migrate source instance id; if it is null, just checking rebalance is or not
   *                    need to do
   * @return rebalance tasks (volumeId, task)
   * @throws NoNeedToRebalance if no need to rebalance, this exception will be caught
   */
  private Multimap<Long, SendRebalanceTask> selectInStoragePool(StoragePool storagePool,
      InstanceId instanceId)
      throws NoNeedToRebalance {
    //if instance is not in this pool, return empty map
    Multimap<Long, Long> datanode2ArchiveId = storagePool.getArchivesInDataNode();
    if (instanceId != null && !datanode2ArchiveId.containsKey(instanceId.getId())) {
      throw new NoNeedToRebalance();
    }

    Multimap<Long, SendRebalanceTask> volumeId2TaskMap = HashMultimap.create();

    //Stores all volumes in the current pool, and the volume which has task is placed at the 
    // forefront.
    LinkedList<VolumeMetadata> volumeListByOrder = new LinkedList<>();

    //get all volumes in db, and get has task volume in memory
    List<VolumeMetadata> allVolumes = volumeStore.listVolumes();
    Set<Long> hasTaskVolumeIdSet = new HashSet<>(storagePool.getVolumeIds());
    hasTaskVolumeIdSet.retainAll(rebalanceTaskManager.getHasTaskVolumes());


    allVolumes.removeIf(volume -> {
      if (volume.getStoragePoolId() != (long) storagePool.getPoolId()) {
        return true;
      }
      if (!volume.isVolumeAvailable()) {
        return true;
      }

      if (hasTaskVolumeIdSet.contains(volume.getVolumeId())) {
        volumeListByOrder.add(volume);
        return true;
      }
      return false;
    });
    volumeListByOrder.addAll(allVolumes);

    for (VolumeMetadata volume : volumeListByOrder) {
      try {
        List<SendRebalanceTask> taskList = selectForVolume(storagePool, volume, instanceId);
        volumeId2TaskMap.putAll(volume.getVolumeId(), taskList);

        // There is a maximum volume limit in each pool
        if (volumeId2TaskMap.keySet().size() >= config.getMaxRebalanceVolumeCountPerPool()) {
          break;
        }
      } catch (NoNeedToRebalance ignore) {
        logger.debug("volume:{} no need to rebalance", volume.getVolumeId());
      } catch (Exception e) {
        logger.warn(
            "volume:{} caught exception when get rebalance task, instance id :{}, storage pool "
                + ":{} ",
            volume.getVolumeId(), instanceId, storagePool, e);
      }
    }
    if (volumeId2TaskMap.isEmpty()) {
      throw new NoNeedToRebalance();
    } else {
      logger.warn("there are ({}) rebalance steps of instance:{}. refer to {} volumes of pool:{}.",
          volumeId2TaskMap.size(), instanceId, volumeId2TaskMap.keySet(), storagePool.getPoolId());
      return volumeId2TaskMap;
    }
  }

  @Deprecated
  private Collection<ComparableRebalanceTask> selectInStoragePool(StoragePool storagePool,
      boolean record)
      throws NoNeedToRebalance {
    TreeSet<ComparableRebalanceTask> tasks = new TreeSet<>();
    List<VolumeMetadata> volumes = volumeInformationManger.listVolumes();
    Collections.shuffle(volumes);
    for (VolumeMetadata volume : volumes) {
      if (volume.getStoragePoolId() == (long) storagePool.getPoolId()) {
        try {
          tasks.add(selectForVolume(storagePool, volume, record));
        } catch (NoNeedToRebalance ignore) {
          logger.info("in pool:{}, {}, not need do reBalance",
              storagePool.getPoolId(), storagePool.getName());
        } catch (VolumeCreatingException e) {
          logger.info("volume is being created", e);
          throw new NoNeedToRebalance();
        } catch (TooManyTasksException e) {
          logger.info("too many tasks processing");
          throw new NoNeedToRebalance();
        } catch (Throwable t) {
          logger.warn("throwable caught", t);
          throw new NoNeedToRebalance();
        }
      }
    }
    if (tasks.isEmpty()) {
      throw new NoNeedToRebalance();
    } else {
      return tasks;
    }
  }

  /**
   * select rebalance tasks in volume, when migrate source is instanceId.
   *
   * @param storagePool pool
   * @param volume      volume
   * @param instanceId  migrate source instance id; if it is null, just checking rebalance is or not
   *                    need to do
   * @return rebalance tasks
   * @throws NoNeedToRebalance              if no need to rebalance, this exception will be caught
   * @throws SegmentNotFoundExceptionThrift segment not found
   */
  private List<SendRebalanceTask> selectForVolume(StoragePool storagePool, VolumeMetadata volume,
      InstanceId instanceId)
      throws NoNeedToRebalance, SegmentNotFoundExceptionThrift, DatanodeTypeNotSetExceptionThrift,
      VolumeNotFoundException {
    long volumeId = volume.getVolumeId();

    if (!canRebalance(volume)) {
      throw new NoNeedToRebalance();
    }

    logger.warn("volume:{} rebalance info:{}, stable time:{}", volumeId, volume.getRebalanceInfo(),
        volume.getStableTime());

    //get volume detail info
    volume = volumeInformationManger.getVolumeNew(volumeId, Constants.SUPERADMIN_ACCOUNT_ID);

    //if volume is balanced after rebalance, and volume pool is not changed, means no need to do 
    // rebalance
    RebalanceTaskManager.VolumeRebalanceStatus volumeRebalanceStatus = rebalanceTaskManager
        .getRebalanceStatus(volumeId);
    if (volumeRebalanceStatus == RebalanceTaskManager.VolumeRebalanceStatus.BALANCED) {

      if (instanceId == null) {

        SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
            storageStore, segmentSize, volume.getVolumeType())
            .collectionInstance();
        VolumeRebalanceSelector selector = new VolumeRebalanceSelector(simulateInstanceBuilder,
            volume, storageStore.list());



        if (selector.parseVolumeInfo().isNeedToDoRebalance()) {
          logger.warn(
              "rebalance trigger checker say: volume:{} is not balance, need to do rebalance, now"
                  + " put rebalance status to CHECKING, and set force calculate rebalance task "
                  + "steps",
              volumeId);

          rebalanceTaskManager.setForceCalcTaskStep(volumeId, true);
          volumeRebalanceStatus = RebalanceTaskManager.VolumeRebalanceStatus.CHECKING;
          rebalanceTaskManager.setRebalanceStatus(volumeId, volumeRebalanceStatus);
        }

        logger.debug("now is just rebalance trigger checked, no need to do rebalance actual.");
        //just check
        throw new NoNeedToRebalance();
      }

      if (rebalanceTaskManager.isPoolEnvChanged(storagePool, volume)
          == SimulatePool.PoolChangedStatus.NO_CHANGE) {
        logger.debug("volume:{} environment in pool:{} not changed, no need to do rebalance",
            volumeId, storagePool.getPoolId());
        throw new NoNeedToRebalance();
      }

      //change volume rebalance status to CHECKING, when volume environment changed
      logger.warn(
          "volume:{} environment in pool:{} has changed, now put rebalance status to CHECKING. "
              + "stable time:{} rebalanceVersion:{}",
          volumeId, storagePool.getPoolId(), volume.getStableTime(),
          volume.getRebalanceInfo().getRebalanceVersion());
      volumeRebalanceStatus = RebalanceTaskManager.VolumeRebalanceStatus.CHECKING;
      rebalanceTaskManager.setRebalanceStatus(volumeId, volumeRebalanceStatus);
    } else if (instanceId == null) {
      logger.warn("rebalance status is CHECKING or WORKING, no need to do rebalance trigger check");
      throw new NoNeedToRebalance();
    }



    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize, volume.getVolumeType())
        .collectionInstance();
    if (simulateInstanceBuilder.getSimpleDatanodeMap().size() > 0) {
      logger.warn("pool:{} has simple datanode:{}, cannot do rebalance", storagePool.getPoolId(),
          simulateInstanceBuilder.getSimpleDatanodeMap().keySet());
      throw new NoNeedToRebalance();
    }
    logger.warn(">>>>>>>>>simulateInstanceBuilder");
    VolumeRebalanceSelector selector = new VolumeRebalanceSelector(simulateInstanceBuilder, volume,
        storageStore.list());




    if (volumeRebalanceStatus == RebalanceTaskManager.VolumeRebalanceStatus.CHECKING
        && !selector.parseVolumeInfo().isNeedToDoRebalance()) {
      logger.warn(
          "volume:{} is balance, no need to do rebalance, now put rebalance status to BALANCED",
          volumeId);
      volumeRebalanceStatus = RebalanceTaskManager.VolumeRebalanceStatus.BALANCED;
      rebalanceTaskManager.updateEnvironment(volumeId, storagePool);
      rebalanceTaskManager.setRebalanceStatus(volumeId, volumeRebalanceStatus);
      throw new NoNeedToRebalance();
    }

    if (volumeRebalanceStatus != RebalanceTaskManager.VolumeRebalanceStatus.WORKING) {
      //change volume rebalance status to WORKING, when P or PS combination over threshold
      logger.warn(
          "volume:{} in pool:{} is not balanced, now put rebalance status to WORKING. stable "
              + "time:{} rebalanceVersion:{}",
          volumeId, storagePool.getPoolId(), volume.getStableTime(),
          volume.getRebalanceInfo().getRebalanceVersion());
      volumeRebalanceStatus = RebalanceTaskManager.VolumeRebalanceStatus.WORKING;
      rebalanceTaskManager.setRebalanceStatus(volumeId, volumeRebalanceStatus);
    }



    final boolean isNeedSendNewTask = rebalanceTaskManager
        .calcRebalanceTasks(volumeStore, volume, storagePool, simulateInstanceBuilder, selector);

    logger
        .info("volume:{} is not balance, need to rebalance, now find step of instance:{}", volumeId,
            instanceId);



    rebalanceTaskManager.removeRunOverTaskFromSendList(volume, simulateInstanceBuilder, false);

    //if volume is already balance, set rebalance status to CHECKING
    if (!rebalanceTaskManager.isVolumeHasTask(volume.getVolumeId())) {
      logger.warn(
          "volume:{} environment in pool:{} tasks is run over or is stop calculating, now put "
              + "rebalance status to BALANCED. rebalance info:{}, stable time:{}",
          volumeId, storagePool.getPoolId(), volume.getRebalanceInfo(), volume.getStableTime());

      rebalanceTaskManager
          .setRebalanceStatus(volumeId, RebalanceTaskManager.VolumeRebalanceStatus.BALANCED);
      throw new NoNeedToRebalance();
    }



    List<SendRebalanceTask> sendTaskList = rebalanceTaskManager
        .selectNoDependTask(volume, instanceId, isNeedSendNewTask);

    logger.warn("instance:{} got ({}) rebalance task of volume:{}.",
        instanceId, sendTaskList.size(), volume.getVolumeId());
    return sendTaskList;
  }

  @Deprecated
  private ComparableRebalanceTask selectForVolume(StoragePool storagePool, VolumeMetadata volume,
      boolean record)
      throws NoNeedToRebalance, VolumeCreatingException, TooManyTasksException {
    RebalanceSelector selector;
    if (record) {
      selector = new RebalanceSelector(
          new InstanceInfoCollectionBuilder(storagePool, storageStore,
              Collections.singletonList(volume),
              segmentSize).setMovingInTasks(movingInTasks).setMovingOutTasks(movingOutTasks)
              .setInsideTasks(insideTasks).setValidateProcessingTaskAndVolumes(true).build());
    } else {
      selector = new RebalanceSelector(
          new InstanceInfoCollectionBuilder(storagePool, storageStore,
              Collections.singletonList(volume),
              segmentSize).setValidateProcessingTaskAndVolumes(false).build());
    }
    ComparableRebalanceTask task = null;
    try {
      logger.debug("try primary rebalance");
      task = selector.selectPrimaryRebalanceTask();
    } catch (NoNeedToRebalance ne) {
      logger.debug("no need for primary rebalance", ne);
    }

    if (task == null) {
      try {
        logger.debug("try normal rebalance");
        task = selector.selectNormalRebalanceTask();
      } catch (NoNeedToRebalance ne) {
        logger.debug("no need for rebalance between instances", ne);
      }
    }

    if (task == null) { // disable inside rebalance for now

    }

    if (task == null) {
      throw new NoNeedToRebalance();
    } else {
      return task;
    }
  }

  /**
   * update send rebalance task list according to new volume metadata if task is over, remove task
   * from task list.
   *
   * @param volumeMetadata new volume matadata
   */
  @Override
  public void updateSendRebalanceTasks(VolumeMetadata volumeMetadata)
      throws DatanodeTypeNotSetExceptionThrift {
    StoragePool storagePool = null;
    long poolId = volumeMetadata.getStoragePoolId();
    try {
      storagePool = storagePoolStore.getStoragePool(poolId);
    } catch (Exception e) {
      logger.error("get storage pool:{} failed!", poolId, e);
    }
    if (storagePool == null) {
      logger.error("get storage pool:{} failed!", poolId);
      return;
    }

    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize,
        volumeMetadata.getVolumeType()).collectionInstance();

    // remove run over task, but time out task is not removed until environment is changed
    rebalanceTaskManager
        .removeRunOverTaskFromSendList(volumeMetadata, simulateInstanceBuilder, false);
  }

  /**
   * clear all volume information, when volume is DEAD.
   *
   * @param volumeMetadata dead volume
   */
  public void clearDeadVolumeRebalanceTasks(VolumeMetadata volumeMetadata) {
    rebalanceTaskManager.clearVolume(volumeMetadata.getVolumeId());
  }

  /**
   * rebalance can do? or not?.
   *
   * @param volume volume information
   * @return if can do rebalance, return true;
   */
  public boolean canRebalance(VolumeMetadata volume) {
    if (!isRebalanceEnable()) {
      logger.warn("rebalance is be set disable!");
      return false;
    }

    /*
     * get rebalance valid period
     */
    RebalanceRuleInformation rebalanceRule = rebalanceRuleStore
        .getRuleOfPool(volume.getStoragePoolId());

    //if no rules, means can do rebalance anytime
    if (rebalanceRule == null) {
      return true;
    }

    /*
     * relative time
     */
    long volumeStableTime = volume.getStableTime();
    if (volumeStableTime == 0) {
      logger.warn(
          "rebalance is not confirm to relative rules of volume:{}! because stable time:{}. it "
              + "means volume is unstable now, than cannot as base stable time",
          volume.getVolumeId(), volumeStableTime);
      return false;
    }

    long waitTime = rebalanceRule.getWaitTime();
    if (waitTime < RebalanceRule.MIN_WAIT_TIME) {
      waitTime = RebalanceRule.MIN_WAIT_TIME;
    }
    long currentTime = System.currentTimeMillis();
    if (currentTime - volumeStableTime < waitTime * 1000) {
      logger.warn(
          "rebalance is not confirm to relative rules of volume:{}! current time:{}, stable "
              + "time:{}, rule wait time:{}",
          volume.getVolumeId(), currentTime, volumeStableTime, waitTime * 1000);
      return false;
    }


    /*
     * get current time
     */
    Date nowTime = new Date(System.currentTimeMillis());
    Calendar nowCalendar = Calendar.getInstance();
    nowCalendar.setTime(nowTime);
    int nowDayOfWeek = nowCalendar.get(Calendar.DAY_OF_WEEK) - 1;
    if (nowDayOfWeek < 0) {
      nowDayOfWeek = 0;
    }
    RebalanceAbsoluteTime.WeekDay nowWeekDay = RebalanceAbsoluteTime.WeekDay
        .findByValue(nowDayOfWeek);

    //get rebalance absolute time rules
    RebalanceRule ruleTemp = rebalanceRule.toRebalanceRule();
    List<RebalanceAbsoluteTime> absoluteTimeList = ruleTemp.getAbsoluteTimeList();
    if (absoluteTimeList == null || absoluteTimeList.isEmpty()) {
      return true;
    }

    for (RebalanceAbsoluteTime absoluteTime : absoluteTimeList) {
      if (absoluteTime == null) {
        continue;
      }

      /*
       * is now in weekday
       */
      if (absoluteTime.getWeekDaySet() != null && !absoluteTime.getWeekDaySet().isEmpty()
          && !absoluteTime.getWeekDaySet().contains(nowWeekDay)) {
        continue;
      }

      long validBeginTime = absoluteTime.getBeginTime();
      long validEndTime = absoluteTime.getEndTime();
      if (validBeginTime == 0 && validEndTime == 0) {
        return true;
      }

      //get 00:00:00 timestamp
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(new Date());
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      long zeroTimestamp = calendar.getTimeInMillis();
      ;

      Date beginDate = new Date(zeroTimestamp + validBeginTime * 1000);
      Date endDate = new Date(zeroTimestamp + validEndTime * 1000);

      Calendar validBeginCalendar = Calendar.getInstance();
      validBeginCalendar.setTime(beginDate);

      Calendar validEndCalendar = Calendar.getInstance();
      validEndCalendar.setTime(endDate);


      if (validBeginTime > validEndTime) {
        if (nowCalendar.after(validBeginCalendar)
            || nowCalendar.before(validEndCalendar)) {
          return true;
        }
      } else {
        if (nowCalendar.after(validBeginCalendar)
            && nowCalendar.before(validEndCalendar)) {
          return true;
        }
      }
    }

    logger.warn("rebalance is not confirm to all absolute rules!");
    return false;
  }

  /**
   * return rebalance is or not can work.
   *
   * @return if rebalance can work, return true
   */
  @Override
  public boolean isRebalanceEnable() {
    return isRebalanceEnable;
  }

  /**
   * set rebalance can work? or forbidden?.
   *
   * @param isEnable if can work, make it true; if forbidden rebalance, make it false
   */
  @Override
  public void setRebalanceEnable(boolean isEnable) {
    isRebalanceEnable = isEnable;
  }

  /**
   * get rebalance progress info of volume.
   *
   * @param volumeId volume id
   * @return rebalance ratio information
   */
  @Override
  public VolumeRebalanceInfo getRebalanceProgressInfo(long volumeId) {
    return rebalanceTaskManager.getRebalanceRatioInfo(volumeId);
  }

  @Deprecated
  @Override
  public synchronized RebalanceTask selectRebalanceTask(boolean record) throws NoNeedToRebalance {
    if (justStarted()) {


      throw new NoNeedToRebalance();
    }
    List<StoragePool> storagePools;
    try {
      storagePools = storagePoolStore.listAllStoragePools();
    } catch (Exception e) {
      logger.error("can not list all storage pools", e);
      throw new NoNeedToRebalance();
    }
    Validate.notNull(storagePools);

    logger.debug("now to select a rebalance task if necessary, storage pools : {}", storagePools);
    TreeSet<ComparableRebalanceTask> taskSet = new TreeSet<>();
    for (StoragePool storagePool : storagePools) {
      try {
        Collection<ComparableRebalanceTask> tasks = selectInStoragePool(storagePool, record);
        logger.warn("selectInStoragePool:{}", tasks);
        for (ComparableRebalanceTask rebalanceTask : tasks) {
          if (!rebalanceTask.getMySourceSegmentUnit().isFaked()) {
            taskSet.add(rebalanceTask);
          }
        }
      } catch (NoNeedToRebalance ignored) {
        logger.info("in pool:{}, {}, not need do reBalance",
            storagePool.getPoolId(), storagePool.getName());
      }
    }
    if (taskSet.isEmpty()) {
      throw new NoNeedToRebalance();
    } else {
      ComparableRebalanceTask task = taskSet.last();
      if (record) {
        recordProcessingTask(task);
      }
      logger.warn("got a rebalance task(record:{}) : {}", record, task);
      return task;
    }
  }

  @Deprecated
  @Override
  public synchronized boolean discardRebalanceTask(long taskId) {
    Iterator<SimpleRebalanceTask> it = movingInTasks.values().iterator();
    while (it.hasNext()) {
      SimpleRebalanceTask task = it.next();
      if (task.getTaskId() == taskId) {
        it.remove();
        movingOutTasks.values().remove(task);
        logger.warn("discarding a rebalance task {}", task);
        return true;
      }
    }

    return false;
  }

  @Deprecated
  private void recordProcessingTask(SimpleRebalanceTask processingTask) {
    movingOutTasks.put(processingTask.getInstanceToMigrateFrom(), processingTask);
    movingInTasks.put(processingTask.getDestInstanceId(), processingTask);
  }

  @Override
  public Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> reserveVolume(
      long expectedSize, VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize,
      Long storagePoolId) throws NotEnoughGroupExceptionThrift, NotEnoughSpaceExceptionThrift,
      NotEnoughNormalGroupExceptionThrift, TException {
    try {
      StoragePool storagePool = storagePoolStore.getStoragePool(storagePoolId);
      Validate.notNull(storagePool);
      SegmentUnitReserver reserver = new SegmentUnitReserver(segmentSize, storageStore);
      try {
        reserver.updateInstanceInfo(
            new SimulateInstanceBuilder(storagePool, storageStore, segmentSize, volumeType)
                .collectionInstance());
        return reserver
            .reserveVolume(expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize,
                simpleDatanodeManager, true);
      } catch (NotEnoughGroupExceptionThrift
          | NotEnoughSpaceExceptionThrift | NotEnoughNormalGroupExceptionThrift ne) {
        reserver.updateInstanceInfo(
            new SimulateInstanceBuilder(storagePool, storageStore, segmentSize, volumeType)
                .collectionInstance());
        return reserver
            .reserveVolume(expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize,
                simpleDatanodeManager, false);
      }
    } catch (TException e) {
      logger.warn("can not reserve volume", e);
      throw e;
    } catch (Exception e) {
      logger.warn("can not reserve volume", e);
      throw new TException(e);
    }
  }

  @Override
  public ReserveSegUnitResult reserveSegUnits(ReserveSegUnitsInfo reserveSegUnitsInfo)
      throws InternalErrorThrift, DatanodeTypeNotSetExceptionThrift, SegmentNotFoundExceptionThrift,
      NotEnoughSpaceExceptionThrift, NotEnoughGroupExceptionThrift, TException {

    logger.warn("reserveSegUnits request: {}", reserveSegUnitsInfo);
    //if arbiter group not enough, arbiter can create at normal datanode

    long volumeId = reserveSegUnitsInfo.getVolumeId();
    Set<Integer> excludedGroups = new HashSet<>();
    for (long instanceId : reserveSegUnitsInfo.getExcludedInstanceIds()) {
      InstanceMetadata instance = storageStore.get(instanceId);
      Validate.notNull(instance,
          "Datanode with id " + instanceId + " still in membership, but cannot find it");
      Validate.isTrue(instance.getDatanodeStatus().equals(OK));
      Group excludedGroup = instance.getGroup();
      excludedGroups.add(excludedGroup.getGroupId());
    }

    /*
     * try to get volume information
     */
    VolumeMetadata volumeMetadata = null;
    try {
      volumeMetadata = volumeInformationManger.getVolumeNew(volumeId, 0L);
    } catch (Exception e) {
      logger.error("can't get volume: {} information, the error :", volumeId, e);
    }

    if (volumeMetadata == null) {
      logger.error("can't get volume: {} information", volumeId);
      throw new InternalErrorThrift();
    }

    Long domainId = volumeMetadata.getDomainId();
    Long storagePoolId = volumeMetadata.getStoragePoolId();

    Domain domain = null;
    try {
      domain = domainStore.getDomain(domainId);
    } catch (Exception e) {
      logger.warn("can not get domain", e);
    }
    if (domain == null) {
      logger.error("domain: {} is not exist", domainId);
      throw new InternalErrorThrift();
    }
    StoragePool storagePool = null;
    try {
      storagePool = storagePoolStore.getStoragePool(storagePoolId);
    } catch (Exception e) {
      logger.warn("can not get storage pool", e);
    }
    if (storagePool == null) {
      logger.error("storage pool: {} is not exist", storagePoolId);
      throw new InternalErrorThrift();
    }
    if (!domain.getStoragePools().contains(storagePool.getPoolId())) {
      logger.error("domain: {} does not contain storage pool:{}", domain, storagePool);
      throw new InternalErrorThrift();
    }

    List<InstanceMetadata> storageList = new ArrayList<>();
    // build datanode instance with storage pool id
    logger.info("storage pool: {}", storagePool);
    for (Map.Entry<Long, Collection<Long>> entry : storagePool.getArchivesInDataNode().asMap()
        .entrySet()) {
      Long datanodeId = entry.getKey();
      Collection<Long> archiveIds = entry.getValue();
      InstanceMetadata datanode = storageStore.get(datanodeId);
      if (datanode == null) {
        logger.warn("can not get datanode by ID:{}", datanodeId);
        continue;
      }
      if (!datanode.getDatanodeStatus().equals(OK)) {
        logger.warn("datanode:{} is not OK status.", datanode);
        continue;
      }

      InstanceMetadata buildDatanode = RequestResponseHelper
          .buildInstanceWithPartOfArchives(datanode, archiveIds);
      storageList.add(buildDatanode);
    }

    logger.info("after select from storage pool, get datanodes:{}", storageList);

    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize,
        volumeMetadata.getVolumeType()).collectionInstance();
    /*
     * get data node information in pool
     */
    Map<Long, InstanceMetadata> instanceId2InstanceMap = new HashMap<>();    //all instance 
    // information in  pool
    Set<Long> normalIdSet = new HashSet<>();    //all normal data node in pool
    Set<Long> simpleIdSet = new HashSet<>();    //all simple data node in pool
    Set<Integer> allGroupIdSet = simulateInstanceBuilder
        .getAllGroupSet();    //simple datanode Group Id
    Set<Integer> simpleGroupIdSet = simulateInstanceBuilder
        .getSimpleGroupIdSet();    //simple datanode Group Id
    Set<Integer> normalGroupIdSet = simulateInstanceBuilder
        .getNormalGroupIdSet();    //normal datanode Group Id
    Set<Integer> mixGroupIdSet = simulateInstanceBuilder
        .getMixGroupIdSet();       //group which has simple datanode and has normal datanode

    for (SimulateInstanceInfo instanceInfo : simulateInstanceBuilder
        .getInstanceId2SimulateInstanceMap().values()) {
      InstanceMetadata instanceMetadata = storageStore.get(instanceInfo.getInstanceId().getId());
      instanceId2InstanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

      if (instanceInfo.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL) {
        //simple segment unit cannot be create at normal datanode
        normalIdSet.add(instanceInfo.getInstanceId().getId());
      }
    }

    //simpleDatanode may be have no archives, so it cannot add to pool
    //we will create arbiter at all simpleDatanode which be owned domain
    for (SimulateInstanceInfo instanceInfo : simulateInstanceBuilder.getSimpleDatanodeMap()
        .values()) {
      InstanceMetadata instanceMetadata = storageStore.get(instanceInfo.getInstanceId().getId());
      instanceId2InstanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

      simpleIdSet.add(instanceInfo.getInstanceId().getId());
    }




    ObjectCounter<Long> normalOfInstanceInWrapperCounter = new TreeSetObjectCounter<>();
    //secondary combinations when primary is same as current segment primary
    ObjectCounter<Long> secondaryOfPrimaryCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();      //primary combination
    ObjectCounter<Long> secondaryCounter = new TreeSetObjectCounter<>();    //secondary combination
    ObjectCounter<Long> arbiterCounter = new TreeSetObjectCounter<>();    //arbiter combination

    //volume segment traversal
    boolean isInRequestSegWrapper = false;
    Map<Integer, SegmentMetadata> segmentTable = volumeMetadata.getSegmentTable();


    if (segmentTable == null) {
      logger.error("Has no segment in volume:{}.",
          volumeMetadata.getVolumeId());
      throw new SegmentNotFoundExceptionThrift();
    } else if (segmentTable.size() != volumeMetadata.getSegmentCount()) {
      logger.error("segmentTable size:{} not equal with volume segment count:{}",
          segmentTable.size(), volumeMetadata.getSegmentCount());
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentMetadata requestSegment = segmentTable.get((int) reserveSegUnitsInfo.getSegIndex());
    if (requestSegment == null) {
      logger.error("Segment:{} not found in volume:{}.", reserveSegUnitsInfo.getSegIndex(),
          volumeMetadata.getVolumeId());
      throw new SegmentNotFoundExceptionThrift();
    }



    long requestPrimaryId = 0;
    for (Map.Entry<InstanceId, SegmentUnitMetadata> reqSegmentUnitEntry : requestSegment
        .getSegmentUnitMetadataTable().entrySet()) {
      InstanceId reqInsId = reqSegmentUnitEntry.getKey();
      if (reqSegmentUnitEntry.getValue().getMembership().isPrimary(reqInsId)) {
        requestPrimaryId = reqInsId.getId();
        break;
      }
    }

    for (Map.Entry<Integer, SegmentMetadata> segmentEntry : segmentTable.entrySet()) {
      int segmentIndex = segmentEntry.getKey();
      SegmentMetadata segmentMetadata = segmentEntry.getValue();




      if (segmentIndex / segmentWrappCount == reserveSegUnitsInfo.segIndex / segmentWrappCount) {
        isInRequestSegWrapper = true;
      }

      //segment unit traversal
      boolean isRequestPrimary = false;
      Set<Long> secondarySet = new HashSet<>();
      Map<InstanceId, SegmentUnitMetadata> instanceId2SegmentUnitMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      for (Map.Entry<InstanceId, SegmentUnitMetadata> segmentUnitEntry : instanceId2SegmentUnitMap
          .entrySet()) {
        InstanceId instanceId = segmentUnitEntry.getKey();
        SegmentUnitMetadata segmentUnitMetadata = segmentUnitEntry.getValue();

        if (segmentUnitMetadata.getMembership().isPrimary(instanceId)) {
          //primary
          primaryCounter.increment(instanceId.getId());

          //used for overload
          if (isInRequestSegWrapper) {
            normalOfInstanceInWrapperCounter.increment(instanceId.getId());
          }

          if (instanceId.getId() == requestPrimaryId) {
            isRequestPrimary = true;
          }
        } else if (segmentUnitMetadata.getMembership().isSecondary(instanceId)) {
          //secondary
          secondaryCounter.increment(instanceId.getId());

          secondarySet.add(instanceId.getId());

          //used for overload
          if (isInRequestSegWrapper) {
            normalOfInstanceInWrapperCounter.increment(instanceId.getId());
          }
        } else if (segmentUnitMetadata.getMembership().isArbiter(instanceId)) {
          //arbiter
          arbiterCounter.increment(instanceId.getId());
        }
      }   //for (Map.Entry<InstanceId, SegmentUnitMetadata> segmentUnitEntry : 
      // instanceId2SegmentUnitMap.entrySet()){

      //save secondary combinations of request P
      if (isRequestPrimary) {
        for (long secondaryIdOfReqPrimary : secondarySet) {
          secondaryOfPrimaryCounter.increment(secondaryIdOfReqPrimary);
        }
      }
    }

    logger.warn("normalOfInstanceInWrapperCounter:{}; secondaryOfPrimaryCounter:{}",
        normalOfInstanceInWrapperCounter, secondaryOfPrimaryCounter);

    /*
     * select destination instance
     */
    List<Long> selInstancesIdList;
    //if (instance.getFreeSpace() >= expectedSize && !excludedGroups.contains(instance.getGroup()))
    if (reserveSegUnitsInfo.getSegmentUnitType() == SegmentUnitType.Arbiter) {
      //get can be used group set
      Set<Integer> usedGroup = new HashSet<>(excludedGroups);



      int expectedMembers = Math.min(reserveSegUnitsInfo.getNumberOfSegUnits() + 1,
          allGroupIdSet.size() - usedGroup.size());

      //reserve arbiter segment unit
      try {
        selInstancesIdList = reserveArbiterSegmentUnit(usedGroup,
            reserveSegUnitsInfo.getNumberOfSegUnits(), expectedMembers, simpleIdSet,
            normalIdSet, instanceId2InstanceMap, arbiterCounter);
      } catch (NotEnoughGroupExceptionThrift e) {
        reportEventForReserverNotEnoughSpace(volumeMetadata, storagePool);
        logger.error("can not reserve segment unit for request:{}", reserveSegUnitsInfo, e);
        throw e;
      }
    } else {
      //get can be used group set
      Set<Integer> usedGroup = new HashSet<>(excludedGroups);
      usedGroup.addAll(simpleGroupIdSet);



      int expectedMembers = Math.min(reserveSegUnitsInfo.getNumberOfSegUnits() + 2,
          allGroupIdSet.size() - usedGroup.size());
      long expectedSize = reserveSegUnitsInfo.getSegmentSize();

      //reserve arbiter segment unit
      try {
        selInstancesIdList = reserveSecondarySegmentUnit(simulateInstanceBuilder, volumeMetadata,
            requestPrimaryId,
            primaryCounter.get(requestPrimaryId), usedGroup,
            reserveSegUnitsInfo.getNumberOfSegUnits(), expectedMembers,
            expectedSize, normalIdSet, secondaryOfPrimaryCounter, secondaryCounter,
            normalOfInstanceInWrapperCounter);
      } catch (NotEnoughSpaceExceptionThrift e) {
        reportEventForReserverNotEnoughSpace(volumeMetadata, storagePool);
        logger.error("can not reserve segment unit for request:{}", reserveSegUnitsInfo, e);
        throw e;
      } catch (TException e) {
        logger.error("can not reserve segment unit for request:{}", reserveSegUnitsInfo, e);
        throw e;
      }
    }

    Set<Long> selInsIdSet = new HashSet<>();
    List<InstanceMetadataThrift> instances = new ArrayList<>();
    for (long instanceId : selInstancesIdList) {
      Instance datanodeInstance = instanceStore.get(new InstanceId(instanceId));
      // Instance sdInstance = instanceStore.getByHostNameAndServiceName(
      // datanodeInstance.getEndPointByServiceName(PortType.CONTROL).getHostName(),
      // PyService.SYSTEMDAEMON.getServiceName());
      Instance sdInstance = null;
      if (sdInstance != null && sdInstance.isNetSubHealth()) {
        logger.warn("sD:{} net is not health.", datanodeInstance);
        continue;
      }
      instances.add(
          RequestResponseHelper.buildThriftInstanceFrom(instanceId2InstanceMap.get(instanceId)));
      selInsIdSet.add(instanceId);
    }

    logger.warn("at last get instances:{}", selInsIdSet);

    if (instances.size() >= reserveSegUnitsInfo.getNumberOfSegUnits()) {
      Set<Integer> groupSet = new HashSet<>();
      for (InstanceMetadataThrift instanceMetadataThrift : instances) {
        groupSet.add(instanceMetadataThrift.getGroup().getGroupId());
      }
      Validate.isTrue(groupSet.size() >= reserveSegUnitsInfo.getNumberOfSegUnits(),
          "group set:" + groupSet + "instances:" + instances);

      ReserveSegUnitResult reserveSegUnitResult = new ReserveSegUnitResult(instances,
          storagePoolId);
      logger.warn("reserveSegUnits result : {}", reserveSegUnitResult);
      return reserveSegUnitResult;
    } else {
      // no instance reserved
      logger.error("can not reserve segment unit for request:{}", reserveSegUnitsInfo);
      reportEventForReserverNotEnoughSpace(volumeMetadata, storagePool);
      throw new NotEnoughSpaceExceptionThrift();
    }
  }

  /**
   * put event to.
   **/
  private void reportEventForReserverNotEnoughSpace(VolumeMetadata volume,
      StoragePool storagePool) {
    if (volume.getVolumeStatus() == VolumeStatus.ToBeCreated
        || volume.getVolumeStatus() == VolumeStatus.Creating) {
      //just for the create ok volume to Reserver
      return;
    }

    Map<String, String> userDefineParams = new HashMap<>();
    userDefineParams
        .put(UserDefineName.StoragePoolID.name(), String.valueOf(storagePool.getPoolId()));
    userDefineParams.put(UserDefineName.StoragePoolName.name(), storagePool.getName());
    EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER, userDefineParams);

    Map<String, Long> counters = new HashMap<>();
    counters.put(CounterName.STORAGE_POOL_CANNOT_REBUILD.name(),
        poolCanNotRebuildForReserveSegUnit);
    eventDataWorker.work(OperationName.StoragePool.name(), counters);
  }

  /**
   * reserve arbiter segment unit will be the datanode with the least arbiter as the reserve object，
   * select from simple datanode first, when simple datanode not enough ,it will select from normal
   * datanode.
   *
   * @param usedGroup              group which had been used
   * @param necessaryCount         necessary secondary segment count
   * @param expectedMembers        expect arbiter segment count
   * @param simpleIdSet            all simple datanode
   * @param normalIdSet            all normal datanode
   * @param instanceId2InstanceMap all instance
   * @param arbiterCounter         datanode used to be arbiter count
   * @return select arbiter segment unit
   */
  private List<Long> reserveArbiterSegmentUnit(Set<Integer> usedGroup, int necessaryCount,
      int expectedMembers, Set<Long> simpleIdSet,
      Set<Long> normalIdSet, Map<Long, InstanceMetadata> instanceId2InstanceMap,
      ObjectCounter<Long> arbiterCounter) throws NotEnoughGroupExceptionThrift {
    logger
        .warn("usedGroup:{}, simpleIdSet:{}, normalIdSet:{}, necessaryCount:{}, expectedMembers:{}",
            usedGroup, simpleIdSet, normalIdSet, necessaryCount, expectedMembers);
    final List<Long> selInstancesIdList = new ArrayList<>();

    ObjectCounter<Long> bestSimpleList = new TreeSetObjectCounter<>();
    for (long instanceId : simpleIdSet) {
      InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
      //has already used in this group
      if (usedGroup.contains(instanceTemp.getGroup().getGroupId())) {
        continue;
      }

      if (arbiterCounter.get(instanceId) == 0) {
        bestSimpleList.set(instanceId, 0);
      } else {
        bestSimpleList.set(instanceId, arbiterCounter.get(instanceId));
      }
    }
    ObjectCounter<Long> bestNormalList = new TreeSetObjectCounter<>();
    for (long instanceId : normalIdSet) {
      InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
      if (usedGroup.contains(instanceTemp.getGroup().getGroupId())) {
        continue;
      }
      if (arbiterCounter.get(instanceId) == 0) {
        bestNormalList.set(instanceId, 0);
      } else {
        bestNormalList.set(instanceId, arbiterCounter.get(instanceId));
      }
    }

    logger.warn("get arbiter best simple instance list:{}", bestSimpleList);
    logger.warn("get arbiter best normal instance list:{}", bestNormalList);

    Iterator<Long> simpleListItor = bestSimpleList.iterator();
    Iterator<Long> normalListItor = bestNormalList.iterator();
    while (selInstancesIdList.size() < expectedMembers) {
      if (simpleListItor.hasNext()) {
        Long instanceId = simpleListItor.next();
        InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
        if (usedGroup.contains(instanceTemp.getGroup().getGroupId())) {
          continue;
        }
        //arbiter priority selection simple datanode to be created
        selInstancesIdList.add(instanceId);
        usedGroup.add(instanceTemp.getGroup().getGroupId());
      } else if (normalListItor.hasNext()) {
        Long instanceId = normalListItor.next();
        InstanceMetadata instanceTemp = instanceId2InstanceMap.get(instanceId);
        if (usedGroup.contains(instanceTemp.getGroup().getGroupId())) {
          continue;
        }
        //if simple datanode is not enough , arbiter can create at normal datanode
        selInstancesIdList.add(instanceId);
        usedGroup.add(instanceTemp.getGroup().getGroupId());
      } else {
        //if necessary secondary count is already selected, exception can not be cause
        if (selInstancesIdList.size() >= necessaryCount) {
          logger.warn("redundancy arbiter not created!");
          break;
        }

        logger.error("Groups not enough to reserve segment unit! expected arbiter count:{}",
            expectedMembers);
        throw new NotEnoughGroupExceptionThrift().setMinGroupsNumber(expectedMembers);
      }
    }   //while (selInstancesIdList.size() < expectedMembers)

    return selInstancesIdList;
  }

  /**
   * reserve secondary segment unit we will select the least secondary combination of primary
   * datanode to be reserved segment unit, and will consider overload in a wrapper count.
   *
   * @param simulateInstanceBuilder          simulate instance object
   * @param usedGroup                        group which had been used
   * @param necessaryCount                   necessary secondary segment count
   * @param expectedMembers                  expect secondary segment count
   * @param expectedSize                     expect size of segment
   * @param normalIdSet                      all normal datanode
   * @param secondaryOfPrimaryCounter        secondary combinations when primary is same as current
   *                                         segment primary
   * @param normalOfInstanceInWrapperCounter normal segment unit counter of data node when wrapper
   *                                         index is same as current segment(used for overload)
   * @return select secondary segment unit
   */
  private List<Long> reserveSecondarySegmentUnit(SimulateInstanceBuilder simulateInstanceBuilder,
      VolumeMetadata volumeMetadata, long primaryId,
      long primaryCount, Set<Integer> usedGroup, int necessaryCount,
      int expectedMembers, long expectedSize, Set<Long> normalIdSet,
      ObjectCounter<Long> secondaryOfPrimaryCounter, ObjectCounter<Long> secondaryDistributeCounter,
      ObjectCounter<Long> normalOfInstanceInWrapperCounter)
      throws TException {
    logger.warn(
        "usedGroup:{}, normalIdSet:{}, primaryId:{}, necessaryCount:{}, expectedMembers:{}, "
            + "expectedSize:{}",
        usedGroup, normalIdSet, primaryId, necessaryCount, expectedMembers, expectedSize);
    final List<Long> selInstancesIdList = new ArrayList<>();

    Map<Long, SimulateInstanceInfo> instanceId2InstanceMap = simulateInstanceBuilder
        .getInstanceId2SimulateInstanceMap();

    //get secondary datanode priority list
    LinkedList<Long> secondaryListOfPrimary = simulateInstanceBuilder.getSecondaryPriorityList(
        new LinkedList<>(normalIdSet), primaryId, primaryCount,
        secondaryOfPrimaryCounter, secondaryDistributeCounter, null,
        volumeMetadata.getSegmentCount(), volumeMetadata.getVolumeType().getNumSecondaries());

    logger.warn("get secondary priority instance list:{}", secondaryListOfPrimary);
    secondaryListOfPrimary
        .removeIf(value -> usedGroup.contains(instanceId2InstanceMap.get(value).getGroupId()));
    logger.warn("get secondary best normal instance list:{} after remove used group",
        secondaryListOfPrimary);


    if (secondaryListOfPrimary.isEmpty()) {
      Set<Integer> groupids = new HashSet<>();
      for (SimulateInstanceInfo simulateInstanceInfo : instanceId2InstanceMap.values()) {
        groupids.add(simulateInstanceInfo.getGroupId());
      }

      if (groupids.size() >= volumeMetadata.getVolumeType().getNumMembers()) {
        TException e = new MemberShipChangedExceptionThrift();
        logger.error(
            "Cannot reserve segment unit of secondary. May be Membership changed during this "
                + "commond execute.",
            e);
        throw e;
      }
    }

    ObjectCounter<Long> secondaryOfPrimaryCounterBac = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryDistributeCounterBac = new TreeSetObjectCounter<>();
    for (long instanceId : normalIdSet) {
      secondaryOfPrimaryCounterBac.set(instanceId, secondaryOfPrimaryCounter.get(instanceId));
      secondaryDistributeCounterBac.set(instanceId, secondaryDistributeCounter.get(instanceId));
    }

    Set<Long> overloadSet = new HashSet<>();
    Iterator<Long> normalListItor = secondaryListOfPrimary.iterator();
    Iterator<Long> overloadItor = null;
    while (selInstancesIdList.size() < expectedMembers) {
      if (normalListItor.hasNext()) {
        SimulateInstanceInfo instanceTemp = instanceId2InstanceMap.get(normalListItor.next());
        long instanceId = instanceTemp.getInstanceId().getId();

        //group had already used
        if (usedGroup.contains(instanceTemp.getGroupId())) {
          continue;
        }

        //has no enough space to reserve segment unit
        if (instanceTemp.getFreeSpace() < expectedSize) {
          logger.warn("instance:{} has not enough space:{}", instanceId, expectedSize);
          continue;
        }

        //overload
        long normalCountOnInstance = normalOfInstanceInWrapperCounter.get(instanceId);
        if (normalCountOnInstance >= instanceTemp.getDiskCount()) {
          overloadSet.add(instanceId);
          continue;
        }

        secondaryOfPrimaryCounterBac.increment(instanceId);
        secondaryDistributeCounterBac.increment(instanceId);
        selInstancesIdList.add(instanceId);
        usedGroup.add(instanceTemp.getGroupId());
      } else if (overloadItor == null) {
        overloadItor = secondaryListOfPrimary.iterator();
      } else if (overloadItor != null && overloadItor.hasNext()) {
        long overloadInsId = overloadItor.next();
        if (!overloadSet.contains(overloadInsId)) {
          continue;
        }

        SimulateInstanceInfo instanceTemp = instanceId2InstanceMap.get(overloadInsId);
        if (instanceTemp == null) {
          logger.error(
              "Groups not enough space to reserve segment unit! expected arbiter count:{}; "

                  + "expectedSize:{}", expectedMembers, expectedSize);
          throw new NotEnoughSpaceExceptionThrift();
        }

        long instanceId = instanceTemp.getInstanceId().getId();

        //group had already used
        if (usedGroup.contains(instanceTemp.getGroupId())) {
          continue;
        }

        //has no enough space to reserve segment unit
        if (instanceTemp.getFreeSpace() < expectedSize) {
          logger.warn("instance:{} has not enough space:{}", instanceId, expectedSize);
          continue;
        }

        selInstancesIdList.add(instanceId);
        usedGroup.add(instanceTemp.getGroupId());
      } else {
        //if necessary secondary count is already selected, exception can not be cause
        if (selInstancesIdList.size() >= necessaryCount) {
          logger.warn("redundancy secondary not created!");
          break;
        }

        logger.error(
            "Groups not enough space to reserve segment unit! expected secondary count:{}; "
                + "expectedSize:{}",
            expectedMembers, expectedSize);
        throw new NotEnoughSpaceExceptionThrift();
      }
    }   //while (selInstancesIdList.size() < expectedMembers){
    return selInstancesIdList;
  }

  public void setSegmentWrappCount(int segmentWrappCount) {
    this.segmentWrappCount = segmentWrappCount;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }
}
