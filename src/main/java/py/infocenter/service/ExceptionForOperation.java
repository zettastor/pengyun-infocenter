/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
