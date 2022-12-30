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
