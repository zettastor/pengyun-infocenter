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

package py.infocenter.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.thrift.share.VolumeCyclingExceptionThrift;
import py.thrift.share.VolumeDeletingExceptionThrift;
import py.thrift.share.VolumeInExtendingExceptionThrift;
import py.thrift.share.VolumeInMoveOnlineDoNotHaveOperationExceptionThrift;
import py.thrift.share.VolumeIsBeginMovedExceptionThrift;
import py.thrift.share.VolumeIsCloningExceptionThrift;
import py.thrift.share.VolumeIsCopingExceptionThrift;
import py.thrift.share.VolumeIsMovingExceptionThrift;
import py.thrift.share.VolumeNotAvailableExceptionThrift;
import py.volume.ExceptionType;
import py.volume.OperationFunctionType;

/**
 *when the do the Operation, throw each exception.
 */
public class ExceptionForOperation {

  private static final Logger logger = LoggerFactory.getLogger(ExceptionForOperation.class);
  private VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift;
  private VolumeDeletingExceptionThrift volumeDeletingExceptionThrift;
  private VolumeCyclingExceptionThrift volumeCyclingExceptionThrift;
  private VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift;


  public ExceptionForOperation() {
    volumeInExtendingExceptionThrift = new VolumeInExtendingExceptionThrift();
    volumeDeletingExceptionThrift = new VolumeDeletingExceptionThrift();
    volumeCyclingExceptionThrift = new VolumeCyclingExceptionThrift();
    volumeNotAvailableExceptionThrift = new VolumeNotAvailableExceptionThrift();
  }
  
  public void checkException(ExceptionType exceptionType,
      OperationFunctionType operationFunctionType, long volumeId)
      throws VolumeIsCopingExceptionThrift, VolumeIsBeginMovedExceptionThrift,
      VolumeIsMovingExceptionThrift,
      VolumeInMoveOnlineDoNotHaveOperationExceptionThrift, VolumeInExtendingExceptionThrift,
      VolumeIsCloningExceptionThrift,
      VolumeDeletingExceptionThrift, VolumeCyclingExceptionThrift,
      VolumeNotAvailableExceptionThrift {
    switch (exceptionType) {
      case VolumeInExtendingExceptionThrift:
        printLog(operationFunctionType, exceptionType, volumeId);
        throw volumeInExtendingExceptionThrift;

      case VolumeDeletingExceptionThrift:
        printLog(operationFunctionType, exceptionType, volumeId);
        throw volumeDeletingExceptionThrift;

      case VolumeCyclingExceptionThrift:
        printLog(operationFunctionType, exceptionType, volumeId);
        throw volumeCyclingExceptionThrift;

      case VolumeNotAvailableExceptionThrift:
        printLog(operationFunctionType, exceptionType, volumeId);
        throw volumeNotAvailableExceptionThrift;

      default:
        return;
    }
  }

  public void printLog(OperationFunctionType operationFunctionType, ExceptionType exceptionType,
      long volumeId) {
    logger.warn("when in :{} operation, the volume :{} is in :{} action, so can not do",
        operationFunctionType.name(), volumeId, exceptionType.name());
  }
}
