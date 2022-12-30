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

package py.infocenter.volume.recycle;

import static py.volume.VolumeInAction.NULL;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.icshare.VolumeRecycleInformation;
import py.infocenter.service.ExceptionForOperation;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.DriverStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.thrift.share.LaunchedVolumeCannotBeDeletedExceptionThrift;
import py.thrift.share.VolumeBeingDeletedExceptionThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.thrift.share.VolumeWasRollbackingExceptionThrift;
import py.volume.ExceptionType;
import py.volume.OperationFunctionType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;


public class VolumeRecycleManager {

  private static final Logger logger = LoggerFactory.getLogger(VolumeRecycleManager.class);
  private VolumeRecycleStore volumeRecycleStore;
  private VolumeStore volumeStore;
  private DriverStore driverStore;
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
  private ExceptionForOperation exceptionForOperation;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;


  public void checkVolumeInfo(long volumeId)
      throws
      TException {
    VolumeMetadata volume = volumeStore.getVolume(volumeId);
    if (volume == null) {
      logger.error("when delete delay, can not find the volume :{}", volumeId);
      throw new VolumeNotFoundExceptionThrift();
    } else {

      //check the Operation, the same as deleteVolume
      ExceptionType exceptionType = volume.getInAction()
          .checkOperation(OperationFunctionType.deleteVolume);
      if (exceptionType != null) {
        exceptionForOperation
            .checkException(exceptionType, OperationFunctionType.recycleVolumeInfo, volumeId);
        logger.warn("when delete delay, find the volume:{}, have the :{}", volumeId, exceptionType);
      }

      // volume not in deleting or deleted status
      if (volume.isDeletedByUser() || volume.isMarkDelete()) {
        logger.warn("when delete delay, volume:{} is being deleting or deleted",
            volume.getVolumeId());
        throw new VolumeBeingDeletedExceptionThrift();
      }

      LockForSaveVolumeInfo.VolumeLockInfo lock = lockForSaveVolumeInfo
          .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_DELETING);
      try {
        // check if the volume has been launched
        List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);
        if (volumeBindingDrivers.size() > 0) {
          LaunchedVolumeCannotBeDeletedExceptionThrift
              launchedVolumeCannotBeDeletedExceptionThrift =
              new LaunchedVolumeCannotBeDeletedExceptionThrift();
          launchedVolumeCannotBeDeletedExceptionThrift.setIsDriverUnknown(false);
          for (DriverMetadata driverMetadata : volumeBindingDrivers) {
            if (driverMetadata.getDriverStatus() == DriverStatus.UNKNOWN) {
              launchedVolumeCannotBeDeletedExceptionThrift.setIsDriverUnknown(true);
            }
          }
          logger.warn(
              "when delete delay, volume :{} has been launched, deleting operation not allowed",
              volumeId);
          throw launchedVolumeCannotBeDeletedExceptionThrift;
        }

        volumeStore
            .updateStatusAndVolumeInAction(volumeId, volume.getVolumeStatus().name(), "NULL");

        /*  delete volume access rule related to the volume **/
        volumeRuleRelationshipStore.deleteByVolumeId(volumeId);
      } catch (TException e) {
        logger.error("when delete delay, caught an exception:", e);
        throw e;
      } finally {
        lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
        lock.unlock();
      }

      lockForSaveVolumeInfo.removeLockByVolumeId(volumeId);
    }
  }


  
  public void checkVolumeInRecycle(long volumeId, OperationFunctionType operationFunctionType) {
    VolumeRecycleInformation volumeRecycleInformation = volumeRecycleStore
        .getVolumeRecycleInfo(volumeId);
    if (volumeRecycleInformation != null) {
      logger.warn("in :{} for volume:{}, find it in recycle, can not do it",
          operationFunctionType.name(), volumeId);

    }
  }

  public void setVolumeRecycleStore(VolumeRecycleStore volumeRecycleStore) {
    this.volumeRecycleStore = volumeRecycleStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  public void setVolumeRuleRelationshipStore(
      VolumeRuleRelationshipStore volumeRuleRelationshipStore) {
    this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
  }

  public void setExceptionForOperation(ExceptionForOperation exceptionForOperation) {
    this.exceptionForOperation = exceptionForOperation;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }
}