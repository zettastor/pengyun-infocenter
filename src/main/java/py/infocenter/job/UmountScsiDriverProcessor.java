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

package py.infocenter.job;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.ScsiClientStore;
import py.icshare.UmountDriverRequest;
import py.infocenter.engine.DataBaseTaskEngineWorker;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.TaskRequestInfo;

public class UmountScsiDriverProcessor extends DataBaseTaskProcessor {

  private static final Logger logger = LoggerFactory.getLogger(UmountScsiDriverProcessor.class);
  private InformationCenterImpl informationCenter;

  public UmountScsiDriverProcessor(TaskRequestInfo taskRequestInfo,
      InformationCenterImpl informationCenter,
      DataBaseTaskEngineWorker.DataBaseTaskEngineCallback callback) {
    super(taskRequestInfo, callback);
    this.informationCenter = informationCenter;
  }

  @Override
  public boolean doWork() {
    ScsiClientStore scsiClientStore = informationCenter.getScsiClientStore();
    ScsiDriverStatusUpdate scsiDriverStatusUpdate = ScsiDriverStatusUpdate.getInstance();

    UmountDriverRequest umountDriverRequest = (UmountDriverRequest) taskRequestInfo.getRequest();

    logger.warn("when UmountScsiDriverProcessor, get the umountDriverRequest :{} ",
        umountDriverRequest);
    long volumeId = umountDriverRequest.getVolumeId();
    int snapshotId = umountDriverRequest.getSnapshotId();
    String scsiIp = umountDriverRequest.getScsiClientIp();

    try {
      informationCenter.beginUmountDriver(umountDriverRequest);
    } catch (TException e) {
      logger.warn("when Umount Driver for volume :{}, in client :{}, get exception:", volumeId,
          scsiIp, e);

      //if unmount, if the scsi umount, not care the pyd, will remove the volume in scsi
      if (scsiClientStore.getScsiClientInfoByVolumeIdAndSnapshotId(scsiIp, volumeId, snapshotId)
          != null) {
        logger.warn("when Umount Driver for volume :{}, in client :{}, update exception", volumeId,
            scsiIp);
        scsiDriverStatusUpdate.updateDescription(scsiClientStore, scsiIp, volumeId, snapshotId, e,
            TaskType.UmountDriver);
      } else {
        logger.warn(
            "when Umount Driver for volume :{}, in client :{}, the scsi Driver unmount, not update",
            volumeId, scsiIp);
      }
    }

    logger.warn("umount scsi Driver task :{} with volume :{}, finish", taskRequestInfo.getTaskId(),
        volumeId);
    return false;
  }
}
