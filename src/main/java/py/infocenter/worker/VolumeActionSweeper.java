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

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.icshare.AccountMetadata;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.TargetType;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.periodic.Worker;
import py.thrift.share.DatanodeTypeNotSetExceptionThrift;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;


/**
 * This worker iterate the volume status transition store and process volume status one by one;.
 */
public class VolumeActionSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(VolumeActionSweeper.class);
  private VolumeStore volumeStore;
  private AppContext appContext;
  private StoragePoolStore storagePoolStore;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;
  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private VolumeInformationManger volumeInformationManger;
  private int updateVolumesStatusTrigger = 4;
  private OperationStore operationStore;
  private PySecurityManager securityManager;



  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("only the master can set and change volume action");
      return;
    }

    List<VolumeMetadata> allVolumes = volumeStore.listVolumes();
    //normal
    for (VolumeMetadata volume : allVolumes) {
      LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
          .getVolumeLockInfo(volume.getVolumeId(),
              LockForSaveVolumeInfo.BusySource.VOLUME_UPDATE_ACTION);
      try {
        VolumeInAction oldVolumeInAction = volume.getInAction();
        VolumeInAction newVolumeInAction = volume.updateAction();

        if (!oldVolumeInAction.equals(newVolumeInAction)) {
          volumeStore
              .updateStatusAndVolumeInAction(volume.getVolumeId(), volume.getVolumeStatus().name(),
                  newVolumeInAction.name());
        }
      } finally {
        lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
        lock.unlock();
      }

    }

    /* removeDeadVolumeFromStoragePool **/
    removeDeadVolumeFromStoragePool();

    //update Rebalance
    if (1 == (updateVolumesStatusTrigger++ / 4)) {

      updateRebalanceInfo();
      updateVolumesStatusTrigger = 0;
    }
  }

  /**
   * update information about rebalance.
   */
  private void updateRebalanceInfo() {
    List<VolumeMetadata> volumeMetadataList = volumeInformationManger.listVolumes();

    //update rebalance task
    try {
      for (VolumeMetadata volume : volumeMetadataList) {
        if (volume.getVolumeStatus() == VolumeStatus.Deleting
            || volume.getVolumeStatus() == VolumeStatus.Deleted
            || volume.getVolumeStatus() == VolumeStatus.Dead) {
          logger.warn("volume:{} is Dead, clear rebalance tasks now.", volume.getVolumeId());
          segmentUnitsDistributionManager.clearDeadVolumeRebalanceTasks(volume);
          buildRebalanceOperationLog(volume, 1.0);
        } else {

          segmentUnitsDistributionManager.updateSendRebalanceTasks(volume);

          //update rebalance ratio
          VolumeRebalanceInfo rebalanceRatioInfo = segmentUnitsDistributionManager
              .getRebalanceProgressInfo(volume.getVolumeId());
          rebalanceRatioInfo.setRebalanceVersion(volume.getRebalanceInfo().getRebalanceVersion());
          volumeStore.updateRebalanceInfo(volume.getVolumeId(), rebalanceRatioInfo);
          buildRebalanceOperationLog(volume, rebalanceRatioInfo.getRebalanceRatio());

          //update stable time to memory
          if (volume.getVolumeStatus().equals(VolumeStatus.Stable)) {
            if (volume.getStableTime() == 0) {
              long stableTime = System.currentTimeMillis();
              volume.setStableTime(stableTime);
              volumeStore.updateStableTime(volume.getVolumeId(), stableTime);
              logger.warn("volume:{} {} is becoming stable, stable time:{}",
                  volume.getVolumeId(), volume.getName(), stableTime);
            }
          } else {
            //not clear when rebalance is doing
            if (rebalanceRatioInfo.getRebalanceRatio() == 1.0 && volume.getStableTime() != 0) {
              volume.setStableTime(0);
              volumeStore.updateStableTime(volume.getVolumeId(), 0);
              logger.warn("volume:{} {} becoming unstable, stable time be cleared now.",
                  volume.getVolumeId(), volume.getName());
            }
          }

        }
      }
    } catch (DatanodeTypeNotSetExceptionThrift e) {
      logger.warn("datanode type not set, when update rebalance sending task list");
    }
  }

  /**
   * remove dead volume from storagePool.
   */
  private void removeDeadVolumeFromStoragePool() {
    List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumes();
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      if (volumeMetadata.getVolumeStatus() == VolumeStatus.Dead) {
        Long storagePoolId = volumeMetadata.getStoragePoolId();
        try {
          StoragePool storagePool = storagePoolStore.getStoragePool(storagePoolId);
          if (storagePool == null) {
            continue;
          }
          Set<Long> volumeIds = storagePool.getVolumeIds();
          if (volumeIds.contains(volumeMetadata.getVolumeId())) {
            storagePoolStore.deleteVolumeId(storagePoolId, volumeMetadata.getVolumeId());
            logger.warn(
                "delete volume from storagePool successfully. storagePoolId is: {}, volumeId is: "
                    + "{}",
                storagePoolId, volumeMetadata.getVolumeId());
          }
        } catch (SQLException | IOException e) {
          logger
              .error("delete volume from storagePool failed, storagePoolId is: {}, volumeId is: {}",
                  storagePoolId, volumeMetadata.getVolumeId());
        }
      }
    }
  }

  private void buildRebalanceOperationLog(VolumeMetadata volumeMetadata, double rebalanceRatio) {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("current inforcenter not master. no need do rebalance operation log");
      return;
    }

    boolean isRebalanceOver = (rebalanceRatio >= 1);
    // get operation and check whether already exits
    boolean operationExists = rebalanceActivitingOperationExists(volumeMetadata.getVolumeId());

    OperationStatus operationStatus = OperationStatus.SUCCESS;
    if (isRebalanceOver) {
      if (operationExists) {

        operationStatus = OperationStatus.SUCCESS;
        logger.info(
            "volume:{} rebalance is do over, but found activiting operation log. so will change "
                + "log status to SUCCESS",
            volumeMetadata.getVolumeId());
      } else {

        logger.info(
            "volume:{} rebalance is do over, and not found activiting operation log. so no need "
                + "do anything",
            volumeMetadata.getVolumeId());
        return;
      }
    } else {
      if (operationExists) {

        logger.info(
            "volume:{} rebalance is doing, and already have activiting operation log. so "
                + "operation log no need change",
            volumeMetadata.getVolumeId());
        return;
      } else {


        operationStatus = OperationStatus.ACTIVITING;
        logger.info(
            "volume:{} rebalance is doing, but not found activiting operation log. so will new "
                + "ACTIVITING operation log",
            volumeMetadata.getVolumeId());
      }
    }

    //save operation log
    buildOperationAndSaveToDb(Constants.SUPERADMIN_ACCOUNT_ID, volumeMetadata.getVolumeId(),
        OperationType.REBALANCE, TargetType.VOLUME, volumeMetadata.getName(),
        volumeMetadata.getName(), operationStatus, (long) (rebalanceRatio * 100));
  }

  private boolean rebalanceActivitingOperationExists(long volumeId) {
    Operation volumeRebalanceOperation = null;

    //get volume ACTIVITING rebalance operation
    List<Operation> operationList = operationStore.getActiveOperation();
    for (Operation operation : operationList) {
      if (Long.valueOf(volumeId).equals(operation.getTargetId())
          && operation.getOperationType().equals(OperationType.REBALANCE)
          && OperationStatus.ACTIVITING.equals(operation.getStatus())) {
        volumeRebalanceOperation = operation;
        break;
      }
    }
    return volumeRebalanceOperation != null;
  }

  private long buildOperationAndSaveToDb(Long accountId, Long targetId, OperationType operationType,
      TargetType targetType, String operationObject, String targetName,
      OperationStatus operationStatus, long progress) {
    final long operationId = RequestIdBuilder.get();
    Operation operation = new Operation();
    long time = System.currentTimeMillis();
    operation.setStartTime(time);
    operation.setEndTime(time);
    operation.setStatus(operationStatus);
    if (operationStatus == OperationStatus.SUCCESS) {
      operation.setProgress(100L);
    } else {
      operation.setProgress(progress);
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
      logger.warn(
          "try to save operation into db. accountId:{}, targetId:{}, operationType:{}, "
              + "targetType:{}, "

              + "operationObject:{}, targetName:{}, operationStatus:{}",
          accountId, targetId, operationType, targetType, operationObject, targetName,
          operationStatus);
      operationStore.saveOperation(operation);
    } catch (Exception e) {
      logger.error("save operation {} to database fail", operation, e);
    }
    return operationId;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public LockForSaveVolumeInfo getLockForSaveVolumeInfo() {
    return lockForSaveVolumeInfo;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }

  public SegmentUnitsDistributionManager getSegmentUnitsDistributionManager() {
    return segmentUnitsDistributionManager;
  }

  public void setSegmentUnitsDistributionManager(
      SegmentUnitsDistributionManager segmentUnitsDistributionManager) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public void setSecurityManager(PySecurityManager securityManager) {
    this.securityManager = securityManager;
  }
}


