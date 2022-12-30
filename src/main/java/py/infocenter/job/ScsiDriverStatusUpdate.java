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

package py.infocenter.job;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.ScsiDriverDescription;
import py.icshare.ScsiClientStore;
import py.thrift.share.AccountNotFoundExceptionThrift;
import py.thrift.share.CanNotGetPydDriverExceptionThrift;
import py.thrift.share.ConnectPydDeviceOperationExceptionThrift;
import py.thrift.share.CreateBackstoresOperationExceptionThrift;
import py.thrift.share.CreateLoopbackLunsOperationExceptionThrift;
import py.thrift.share.CreateLoopbackOperationExceptionThrift;
import py.thrift.share.DriverAmountAndHostNotFitThrift;
import py.thrift.share.DriverContainerIsIncExceptionThrift;
import py.thrift.share.DriverIsLaunchingExceptionThrift;
import py.thrift.share.DriverIsUpgradingExceptionThrift;
import py.thrift.share.DriverLaunchingExceptionThrift;
import py.thrift.share.DriverNameExistsExceptionThrift;
import py.thrift.share.DriverTypeConflictExceptionThrift;
import py.thrift.share.DriverTypeIsConflictExceptionThrift;
import py.thrift.share.DriverUnmountingExceptionThrift;
import py.thrift.share.ExistsClientExceptionThrift;
import py.thrift.share.ExistsDriverExceptionThrift;
import py.thrift.share.FailedToUmountDriverExceptionThrift;
import py.thrift.share.GetPydDriverStatusExceptionThrift;
import py.thrift.share.GetScsiClientExceptionThrift;
import py.thrift.share.GetScsiDeviceOperationExceptionThrift;
import py.thrift.share.InfocenterServerExceptionThrift;
import py.thrift.share.NetworkErrorExceptionThrift;
import py.thrift.share.NoEnoughPydDeviceExceptionThrift;
import py.thrift.share.NotRootVolumeExceptionThrift;
import py.thrift.share.PermissionNotGrantExceptionThrift;
import py.thrift.share.ScsiDeviceIsLaunchExceptionThrift;
import py.thrift.share.ScsiDeviceStatusThrift;
import py.thrift.share.ScsiVolumeLockExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.SystemMemoryIsNotEnoughThrift;
import py.thrift.share.TooManyDriversExceptionThrift;
import py.thrift.share.VolumeBeingDeletedExceptionThrift;
import py.thrift.share.VolumeLaunchMultiDriversExceptionThrift;
import py.thrift.share.VolumeNotAvailableExceptionThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.thrift.share.VolumeWasRollbackingExceptionThrift;

public class ScsiDriverStatusUpdate {

  private static final Logger logger = LoggerFactory.getLogger(ScsiDriverStatusUpdate.class);
  private static ScsiDriverStatusUpdate scsiDriverStatusUpdate;

  
  public static ScsiDriverStatusUpdate getInstance() {
    synchronized (ScsiDriverStatusUpdate.class) {
      if (scsiDriverStatusUpdate == null) {
        scsiDriverStatusUpdate = new ScsiDriverStatusUpdate();
      }
    }
    return scsiDriverStatusUpdate;
  }

  void updateDescription(ScsiClientStore scsiClientStore, String scsiIp, long volumeId,
      int snapshotId, TException e, TaskType taskType) {
    logger.warn("updateDescription, for scsiIp :{}, volume :{}, type :{} ", scsiIp, volumeId,
        taskType);

    if (e instanceof VolumeNotFoundExceptionThrift) {
      if (TaskType.LaunchDriver.equals(taskType)) {
        scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
            ScsiDeviceStatusThrift.ERROR.name(),
            ScsiDriverDescription.VolumeNotFoundException.name(), taskType.name());
      }

    } else if (e instanceof VolumeNotAvailableExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.VolumeNotAvailableException.name(), taskType.name());

    } else if (e instanceof NotRootVolumeExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.Error.name(),
          taskType.name());

    } else if (e instanceof VolumeBeingDeletedExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.VolumeBeingDeletedException.name(), taskType.name());

    } else if (e instanceof TooManyDriversExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.TooManyDriversException.name(), taskType.name());

    } else if (e instanceof ServiceHavingBeenShutdownThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.ServiceHavingBeenShutdown.name(), taskType.name());

    } else if (e instanceof DriverTypeConflictExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverTypeIsConflictException.name(), taskType.name());

    } else if (e instanceof VolumeWasRollbackingExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.VolumeWasRollbackingException.name(), taskType.name());

    } else if (e instanceof GetPydDriverStatusExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.GetPydDriverStatusException.name(), taskType.name());

    } else if (e instanceof DriverLaunchingExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverLaunchingException.name(), taskType.name());

    } else if (e instanceof DriverUnmountingExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverUnmountingException.name(), taskType.name());

    } else if (e instanceof SystemMemoryIsNotEnoughThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.SystemMemoryIsNotEnough.name(), taskType.name());

    } else if (e instanceof DriverAmountAndHostNotFitThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.Error.name(),
          taskType.name());

    } else if (e instanceof DriverIsUpgradingExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverIsUpgradingException.name(), taskType.name());

    } else if (e instanceof PermissionNotGrantExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.AccountNotFoundException.name(), taskType.name());

    } else if (e instanceof AccountNotFoundExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.AccountNotFoundException.name(), taskType.name());

    } else if (e instanceof DriverTypeIsConflictExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverTypeIsConflictException.name(), taskType.name());

    } else if (e instanceof ExistsDriverExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.Error.name(),
          taskType.name());

    } else if (e instanceof DriverNameExistsExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverNameExistsException.name(), taskType.name());

    } else if (e instanceof VolumeLaunchMultiDriversExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.VolumeLaunchMultiDriversException.name(), taskType.name());

    } else if (e instanceof NetworkErrorExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.NetworkErrorException.name(),
          taskType.name());

    } else if (e instanceof GetScsiClientExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.GetScsiClientException.name(),
          taskType.name());

    } else if (e instanceof NoEnoughPydDeviceExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.NoEnoughPydDeviceException.name(), taskType.name());

    } else if (e instanceof ConnectPydDeviceOperationExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.ConnectPydDeviceOperationException.name(), taskType.name());

    } else if (e instanceof CreateBackstoresOperationExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.CreateBackstoresOperationException.name(), taskType.name());

    } else if (e instanceof CreateLoopbackOperationExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.CreateLoopbackOperationException.name(), taskType.name());

    } else if (e instanceof CreateLoopbackLunsOperationExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.CreateLoopbackLunsOperationException.name(), taskType.name());

    } else if (e instanceof GetScsiDeviceOperationExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.GetScsiDeviceOperationException.name(), taskType.name());

    } else if (e instanceof ScsiDeviceIsLaunchExceptionThrift) {
      // this exception, not update the
      logger.warn("for volumeId :{} in scsi :{}, the task has process ok, so not update", volumeId,
          scsiIp);


    } else if (e instanceof InfocenterServerExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.InfocenterServerException.name(), taskType.name());

    } else if (e instanceof ScsiVolumeLockExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.InfocenterServerException.name(), taskType.name());

    } else if (e instanceof FailedToUmountDriverExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.FailedToUmountDriverException.name(), taskType.name());

    } else if (e instanceof ExistsClientExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.ExistsClientException.name(),
          taskType.name());

    } else if (e instanceof DriverIsLaunchingExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverIsLaunchingException.name(), taskType.name());

    } else if (e instanceof CanNotGetPydDriverExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.CanNotGetPydDriverException.name(), taskType.name());

    } else if (e instanceof DriverContainerIsIncExceptionThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(),
          ScsiDriverDescription.DriverContainerIsINCException.name(), taskType.name());

    } else if (e instanceof ServiceIsNotAvailableThrift) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.ServiceIsNotAvailable.name(),
          taskType.name());

    } else if (e instanceof TException) {
      scsiClientStore.updateScsiDriverStatusAndDescription(scsiIp, volumeId, snapshotId,
          ScsiDeviceStatusThrift.ERROR.name(), ScsiDriverDescription.Error.name(),
          taskType.name());
    }
  }
}
