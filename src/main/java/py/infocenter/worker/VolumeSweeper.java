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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.common.VolumeMetadataJsonParser;
import py.common.struct.EndPoint;
import py.icshare.AccountMetadata;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.TargetType;
import py.icshare.authorization.PyResource;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.informationcenter.Utils;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonRequest;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonResponse;
import py.volume.VolumeExtendStatus;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;


/**
 * This worker iterate the volume status transition store and process volume status one by one;.
 */
public class VolumeSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(VolumeSweeper.class);
  public long deadTimeToRemove;
  private VolumeStore volumeStore;
  private VolumeStatusTransitionStore volumeStatusStore;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
  private int timeout;
  private InstanceStore instanceStore;
  private int updateVolumesStatusTrigger = 0;
  private int updateVolumesStatusForNotReport = 0;
  private int updateVolumesStatusTriggerDefault = 60;
  private AppContext appContext;
  private VolumeJobStoreDb volumeJobStoreDb;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private VolumeRecycleStore volumeRecycleStore;
  private OperationStore operationStore;
  private PySecurityManager securityManager;

  public VolumeSweeper() {
  }



  public VolumeSweeper(int volumeSweeperRate) {
    int step = (int) TimeUnit.MILLISECONDS.toSeconds(volumeSweeperRate);
    logger.warn("in VolumeSweeper,get the step:{}", step);
    if (step > 1) {
      updateVolumesStatusTriggerDefault = updateVolumesStatusTriggerDefault / step;
    }
  }



  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY
        && appContext.getStatus() != InstanceStatus.SUSPEND) {
      logger.info("only the master and follower do it");
      return;
    }


    if (1 == (updateVolumesStatusTrigger++ / updateVolumesStatusTriggerDefault)) {

      passiveUpdateVolumeStatus();
      updateVolumesStatusTrigger = 0;
    }

    //update volume status, and remove the dead volume which timeout or set the dead time
    ArrayList<VolumeMetadata> volumes = new ArrayList<>();
    int count = volumeStatusStore.drainTo(volumes);

    if (count > 0) { // the queue has volume needs to deal with;
      /* active to update volume status and set dead time ***/
      for (VolumeMetadata volume : volumes) {
        logger.info("the volume status may be changed: {}", volume);
        VolumeStatus volumeStatus = volume.getVolumeStatus();
        // Volume in dead status, if this status last 6 months, delete
        // volume in the DB;
        if (volumeStatus == VolumeStatus.Dead) {
          long currentTime = System.currentTimeMillis();
          if ((volume.getDeadTime() + deadTimeToRemove * 1000L) < currentTime) {
            logger.warn(
                "volume:{} is dead, and we can delete volume DEAD TIME:{}, CURRENT TIME:{} , "
                    + "deadTimeToRemove:{}",
                volume.getVolumeId(), Utils.millsecondToString(volume.getDeadTime()),
                Utils.millsecondToString(currentTime), deadTimeToRemove);
            volumeStore.deleteVolumeForReport(volume);
            volumeStore.saveVolumesWhichToDeleteForReport(volume);
          }
          continue;
        }

        //the volume not create unit, so make the wait to create time,
        List<Long> volumeIds = volumeJobStoreDb.getAllCreateOrExtendVolumeId();
        if (volumeIds != null && volumeIds.contains(volume.getVolumeId())) {
          volume.setWaitToCreateUnitTime(
              System.currentTimeMillis() - volume.getVolumeCreatedTime().getTime());
          logger.info("the volume :{} not create segment, so make the wait time :{} ",
              volume.getVolumeId(),
              volume.getWaitToCreateUnitTime());
        }

        // get the latest VolumeStatus
        VolumeStatus newVolumeStatus = volume.updateStatus();
        // if volume status changed, we save status to memory and DB
        if (!newVolumeStatus.equals(volumeStatus)) {
          volume.setVolumeStatus(newVolumeStatus);
          // update status to DB
          volumeStore.updateStatusAndVolumeInActionForReport(volume.getVolumeId(),
              newVolumeStatus.toString(),
              volume.getInAction().name());
        }

        if ((volumeStatus != VolumeStatus.Dead) && (newVolumeStatus == VolumeStatus.Dead)) {
          setVolumeDeadTime(volume);

          //todo: remove
          processExtendVolume(volume);

          continue;
        }

        //for extend volume
        processExtendVolume(volume);
      } // for volume list
    } //if volume status changed

    //just for the volume datanode down, not unit report the HA, and the volume in deleteing or 
    // Deleted
    if (appContext.getStatus() == InstanceStatus.HEALTHY) {
      //update the the volume migrationRatio
      checkVolumeMigrateOperation();

      //remove the Resource
      removeAndUnbindVolumeResource();

      if (1 == (updateVolumesStatusForNotReport++ / updateVolumesStatusTriggerDefault)) {
        checkTheVolumeWhichNotReportFromDataNode();
        updateVolumesStatusForNotReport = 0;
      }
    }
  }



  public void checkTheVolumeWhichNotReportFromDataNode() {
    List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumes();

    Set<Long> volumeIds = new HashSet<>();
    Collection<InstanceToVolumeInfo> instanceToVolumeInfos = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap().values();
    for (InstanceToVolumeInfo instanceToVolumeInfo : instanceToVolumeInfos) {
      volumeIds.addAll(instanceToVolumeInfo.getVolumeInfo());
    }

    //check the volume in deleteing or Deleted, but not in report table
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      if (!volumeIds.contains(volumeMetadata.getVolumeId())) {
        VolumeStatus volumeStatus = volumeMetadata.getVolumeStatus();
        logger.warn(
            "when checkTheVolumeWhichNotReporFromDataNode, find the volume :{} {}, not in any "
                + "instance, the status :{}, check it",
            volumeMetadata.getVolumeId(), volumeMetadata.getName(), volumeStatus);
        if (volumeStatus.equals(VolumeStatus.Deleting) || volumeStatus
            .equals(VolumeStatus.Deleted)) {
          //set to
          logger.warn(
              "when checkTheVolumeWhichNotReporFromDataNode, set the volume :{} {}, status to Dead",
              volumeMetadata.getVolumeId(), volumeMetadata.getName(), volumeStatus);
          volumeMetadata.setVolumeStatus(VolumeStatus.Dead);
          volumeStore.saveVolume(volumeMetadata);
        }

        if (volumeStatus.equals(VolumeStatus.Available) || volumeStatus
            .equals(VolumeStatus.Stable)) {
          //set to
          logger.warn(
              "when checkTheVolumeWhichNotReporFromDataNode, find the volume :{} {}, not any "
                  + "report, set status to Unavailable",
              volumeMetadata.getVolumeId(), volumeMetadata.getName(), volumeStatus);
          volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
          volumeStore.saveVolume(volumeMetadata);
        }
      }
    }
  }


  public void passiveUpdateVolumeStatus() {
    List<VolumeMetadata> virtualVolumes = volumeStore.listVolumesForReport();
    for (VolumeMetadata volume : virtualVolumes) {
      logger
          .info("get the status , for volume :{}, current volume status :{}", volume.getVolumeId(),
              volume.getVolumeStatus());

      //the volume not create unit, so make the wait to create time,
      List<Long> volumeIds = volumeJobStoreDb.getAllCreateOrExtendVolumeId();
      if (volumeIds != null && volumeIds.contains(volume.getVolumeId())) {
        volume.setWaitToCreateUnitTime(
            System.currentTimeMillis() - volume.getVolumeCreatedTime().getTime());
        logger.info("the volume :{} not create segment, so make the wait time :{} ",
            volume.getVolumeId(),
            volume.getWaitToCreateUnitTime());
      }

      VolumeStatus volumeStatus = volume.getVolumeStatus();
      VolumeStatus newVolumeStatus = volume.updateStatus();

      // if volume status changed, we save status to memory volume,and report to master
      if (!newVolumeStatus.equals(volumeStatus)) {
        volume.setVolumeStatus(newVolumeStatus);

        /*  update status to memory for report **/
        volumeStore.updateStatusAndVolumeInActionForReport(volume.getVolumeId(),
            newVolumeStatus.toString(),
            volume.getInAction().name());
      }

      if ((volumeStatus != VolumeStatus.Dead) && (newVolumeStatus == VolumeStatus.Dead)) {
        setVolumeDeadTime(volume);
      }

      //for extend volume
      processExtendVolume(volume);

    }
  }

  /**
   * update for extend volume.
   **/
  public void processExtendVolume(VolumeMetadata volume) {
    if (volume.getExtendingSize() > 0) {
      logger.warn(
          "when processExtendVolume, the volume :{} extendingSize:{}, and status:{}, the before "
              + "segment size :{}, the extend segment size :{}",
          volume.getVolumeId(), volume.getExtendingSize(), volume.getVolumeStatus(),
          volume.getSegmentTableSize(), volume.getExtendSegmentTableSize());
    }

    //check the volume status
    if (!volume.isVolumeAvailable()) {
      return;
    }

    /*   check the volume extend ok or not and
     * check notify to data node ok or not when extend */
    if (volume.getExtendingSize() > 0) {
      //begin to Updated extend info ToDataNode,no matter extend ok or not
      volume.setUpdatedToDataNode(false);
      VolumeExtendStatus volumeExtendStatus = volume.getVolumeExtendStatus();
      VolumeExtendStatus newVolumeExtendStatus = volume.updateExtendStatus();

      if (!newVolumeExtendStatus.equals(volumeExtendStatus)) {
        volume.setVolumeExtendStatus(newVolumeExtendStatus);

        /*  update status to memory **/
        volumeStore.updateVolumeExtendStatusForReport(volume.getVolumeId(),
            newVolumeExtendStatus.toString());
      }

      /* the extend segment is ok, so current volume is extend ok **/
      if (newVolumeExtendStatus == VolumeExtendStatus.Available) {
        processExtendVolumeOk(volume);
      }

      /* the extend segment is failed, so current volume is extend failed **/
      if (newVolumeExtendStatus == VolumeExtendStatus.Deleting) {
        processExtendVolumeFailed(volume);
      }
    } else {

      //when extend ok,but notify data node failed, so update each time
      if (volume.getLastExtendedTime() != null //extend
          && !volume.isUpdatedToDataNode()) {
        logger.warn("extend volume, UpdatedToDataNode failed, try again");
        /* update the volume json which in firstSegmentMetadata to datanode ***/
        if (updateVolumeMetadataToDataNode(volume)) {
          //if this volume Updated to datanode ok, set next do not, or Updated next time
          volume.setUpdatedToDataNode(true);
        }
      }
    }
  }






  private void setVolumeDeadTime(VolumeMetadata volume) {
    long deadTime = System.currentTimeMillis();
    logger.debug("update deadTime to memory and table volumes, volumeId {}, deadTime {} ",
        volume.getVolumeId(), Utils.millsecondToString(deadTime));
    // only here can set deadTime, other place should keep this
    // value no change
    volume.setDeadTime(deadTime);

    // update dead time to DB
    volumeStore.updateDeadTimeForReport(volume.getVolumeId(), deadTime);
  }



  public void processExtendVolumeOk(VolumeMetadata volume) {

    long extendingSize = volume.getExtendingSize();
    long volumeSize = volume.getVolumeSize();

    //set the new volume size
    volume.setVolumeSize(extendingSize + volumeSize);
    volume.setExtendingSize(0);

    // save the changes of extending size
    volumeStore.updateExtendingSizeForReport(volume.getVolumeId(), volume.getExtendingSize(),
        extendingSize);
    volumeStore.updateStatusAndVolumeInActionForReport(volume.getVolumeId(),
        volume.getVolumeStatus().name(),
        volume.getInAction().name());

    volume.updateSegmentTableWhenExtendOk();
    volume.setVolumeExtendStatus(VolumeExtendStatus.ToBeCreated);
    logger.info("volume extend ok, after extend, the volume info :{}", volume);
    logger.warn(
        "volume extend ok, after extend, the volume segmentTable size :{}, extend size :{}, "
            + "status :{}",
        volume.getSegmentTable().size(), volume.getExtendingSize(), volume.getVolumeStatus());

    /* update the volume json which in firstSegmentMetadata to datanode ***/
    if (updateVolumeMetadataToDataNode(volume)) {
      //if this volume Updated to datanode ok, set next do not, or Updated next time
      volume.setUpdatedToDataNode(true);
    }
  }



  public void processExtendVolumeFailed(VolumeMetadata volume) {
    synchronized (volume) {
      logger.warn("volume extend failed, and the volume id :{}, the extendCount :{}",
          volume.getVolumeId(), volume.getExtendSegmentCount());
      final int extendCount = volume.getExtendSegmentCount();

      //set the extend size
      volume.setExtendingSize(0);

      // save the changes of extending size
      volumeStore.updateExtendingSizeForReport(volume.getVolumeId(), volume.getExtendingSize(), 0);
      volumeStore.updateStatusAndVolumeInActionForReport(volume.getVolumeId(),
          volume.getVolumeStatus().name(),
          volume.getInAction().name());

      volume.clearTheExtendSegmentTable(extendCount);
      volume.setVolumeExtendStatus(VolumeExtendStatus.ToBeCreated);

      //not UpdatedToDataNode
      volume.setUpdatedToDataNode(true);
      /* update the volume json which in firstSegmentMetadata to datanode ***/
    }
  }


  /**
   * xx.
   *
   * @return true: success false: fail this function update the volume meta data to data node where
   *          the primary instance of first segment exists.
   */
  private boolean updateVolumeMetadataToDataNode(VolumeMetadata volumeMetadata) {
    if (volumeMetadata.getSegmentTableSize() == 0) {
      logger.warn("no segment table in volume:{}", volumeMetadata);
      return false;
    }

    // Get the primary instance of first segment. And update volume meta
    // data to data node
    SegmentMetadata firstSegmentMetadata = volumeMetadata.getSegmentByIndex(0);
    if (firstSegmentMetadata == null) {
      logger.warn("no segment indexed 0 in {} ", volumeMetadata);
      return false;
    }

    InstanceId primary = null;
    for (Entry<InstanceId, SegmentUnitMetadata> entry : firstSegmentMetadata
        .getSegmentUnitMetadataTable()
        .entrySet()) {

      //get the primary InstanceId
      if (entry.getKey().equals(entry.getValue().getMembership().getPrimary())) {
        primary = entry.getKey();
        break;
      }
    }

    if (primary == null) {
      logger.warn(
          "can't update the new volume metadata because can't find the primary of the first "
              + "segment unit. The primary is null");
      return false;
    }

    // build volumemetadata as a string
    ObjectMapper mapper = new ObjectMapper();
    String volumeString = null;
    try {
      volumeString = mapper.writeValueAsString(volumeMetadata);
      logger.warn("when extend volume, the volume :{},current volume Json is:{}",
          volumeMetadata.getVolumeId(), volumeString);
    } catch (JsonProcessingException e) {
      logger.error("failed to build volumemetadata string ", e);
      return false;
    }

    VolumeMetadataJsonParser parser = new VolumeMetadataJsonParser(volumeMetadata.getVersion(),
        volumeString);
    UpdateSegmentUnitVolumeMetadataJsonRequest request = RequestResponseHelper
        .buildUpdateSegmentUnitVolumeMetadataJsonRequest(
            new SegId(firstSegmentMetadata.getVolume().getVolumeId(),
                firstSegmentMetadata.getIndex()),
            firstSegmentMetadata.getSegmentUnitMetadata(primary).getMembership(),
            parser.getCompositedVolumeMetadataJson());

    EndPoint primaryEndpoint = null;
    try {
      primaryEndpoint = instanceStore.get(primary).getEndPoint();
      if (primaryEndpoint == null) {
        logger.warn("can't update the new volume metadata because can't find the primary endpoint");
        return false;
      }

      DataNodeService.Iface dataNodeClient = dataNodeClientFactory
          .generateSyncClient(primaryEndpoint, timeout);
      UpdateSegmentUnitVolumeMetadataJsonResponse response = dataNodeClient
          .updateSegmentUnitVolumeMetadataJson(request);
      if (response != null) {
        logger.warn("successful updated segmetadata");
        return true;
      }

    } catch (Exception e) {
      logger.warn("write volume metadata json to primary: {}", primaryEndpoint, e);
    }
    return false;
  }

  //when the volume delete, so need remove the volume Resource info
  private void removeAndUnbindVolumeResource() {
    List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumes();
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Dead)) {
        try {
          PyResource resource = securityManager.getResourceById(volumeMetadata.getVolumeId());
          if (resource != null) {
            logger.warn("the volume:{} is dead, so remove and unbind volume resource :{} ",
                volumeMetadata.getVolumeId(), resource);
            securityManager.unbindResource(volumeMetadata.getVolumeId());
            securityManager.removeResource(volumeMetadata.getVolumeId());
          }
        } catch (Exception e) {
          logger.warn("the volume:{} is dead, remove and unbind resource find exception:",
              volumeMetadata.getVolumeId(), e);
        }
      }
    }
  }

  private void checkVolumeMigrateOperation() {
    Multimap<Long, Operation> volumeOperationMap = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Operation>create());

    List<Operation> operationList = operationStore.getActiveOperation();
    for (Operation operation : operationList) {
      if (operation.getOperationType().equals(OperationType.MIGRATE)) {
        volumeOperationMap.put(operation.getTargetId(), operation);
      }
    }

    List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumes();
    for (VolumeMetadata volumeMetadata : volumeMetadatas) {
      try {
        if (volumeMetadata.isDeletedByUser()) {
          continue;
        }

        long volumeId = volumeMetadata.getVolumeId();
        long totalPageToMigrate = volumeMetadata.getTotalPageToMigrate();
        double migrationRatio = volumeMetadata.getMigrationRatio();

        if (totalPageToMigrate > 0 && migrationRatio < 100) {
          //have migrationRatio, check this volume in operationStore or not
          List<Operation> volumeMigrateOperations = new ArrayList<>(
              volumeOperationMap.get(volumeId));
          if (volumeMigrateOperations.isEmpty()) {
            //save the operation
            buildOperationAndSaveToDb(Constants.SUPERADMIN_ACCOUNT_ID, volumeId,
                OperationType.MIGRATE, TargetType.VOLUME, volumeMetadata.getName(),
                volumeMetadata.getName(), OperationStatus.ACTIVITING);
          }


        }
      } catch (Exception e) {
        logger.warn("checkVolumeMigrateOperation for volume :{}, find some error:",
            volumeMetadata.getVolumeId(), e);
      }
    }
  }

  private long buildOperationAndSaveToDb(Long accountId, Long targetId, OperationType operationType,
      TargetType targetType, String operationObject, String targetName,
      OperationStatus operationStatus) {
    final long operationId = RequestIdBuilder.get();
    Operation operation = new Operation();
    long time = System.currentTimeMillis();
    operation.setStartTime(time);
    operation.setEndTime(time);
    operation.setStatus(operationStatus);
    if (operationStatus == OperationStatus.SUCCESS) {
      operation.setProgress(100L);
    } else {
      operation.setProgress(0L);
    }
    operation.setOperationId(operationId);
    operation.setAccountId(accountId);

    AccountMetadata account = securityManager.getAccountById(accountId);
    if (account != null) {
      operation.setAccountName(account.getAccountName());
    }
    operation.setTargetId(targetId);
    operation.setOperationType(operationType);
    operation.setTargetType(targetType);
    operation.setOperationObject(operationObject);
    operation.setTargetName(targetName);
    try {
      operationStore.saveOperation(operation);
      logger.warn("buildOperationAndSaveToDB for volume:{} for Migrate operation :{}", targetId,
          operation);
    } catch (Exception e) {
      logger.error("save operation {} to database fail", operation, e);
    }
    return operationId;
  }

  public void setDeadTimeToRemove(long deadTimeToRemove) {
    this.deadTimeToRemove = deadTimeToRemove;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public void setVolumeStatusTransitionStore(VolumeStatusTransitionStore store) {
    this.volumeStatusStore = store;
  }

  public void setDataNodeClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
    this.dataNodeClientFactory = dataNodeClientFactory;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
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

  public VolumeJobStoreDb getVolumeJobStoreDb() {
    return volumeJobStoreDb;
  }

  public void setVolumeJobStoreDb(VolumeJobStoreDb volumeJobStoreDb) {
    this.volumeJobStoreDb = volumeJobStoreDb;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public void setVolumeRecycleStore(VolumeRecycleStore volumeRecycleStore) {
    this.volumeRecycleStore = volumeRecycleStore;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public void setSecurityManager(PySecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  //just for unit test
  public void setUpdateVolumesStatusTrigger(int updateVolumesStatusTrigger) {
    this.updateVolumesStatusTrigger = updateVolumesStatusTrigger;
  }
}


