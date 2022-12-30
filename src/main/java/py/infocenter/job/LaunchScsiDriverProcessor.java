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
import py.infocenter.engine.DataBaseTaskEngineWorker;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.TaskRequestInfo;
import py.informationcenter.LaunchDriverRequest;
import py.thrift.share.ScsiVolumeLockExceptionThrift;

public class LaunchScsiDriverProcessor extends DataBaseTaskProcessor {

  private static final Logger logger = LoggerFactory.getLogger(LaunchScsiDriverProcessor.class);
  private InformationCenterImpl informationCenter;

  public LaunchScsiDriverProcessor(TaskRequestInfo taskRequestInfo,
      InformationCenterImpl informationCenter,
      DataBaseTaskEngineWorker.DataBaseTaskEngineCallback callback) {
    super(taskRequestInfo, callback);
    this.informationCenter = informationCenter;
  }

  @Override
  public boolean doWork() {
    boolean taskNeedProcessAagin = false;
    ScsiDriverStatusUpdate scsiDriverStatusUpdate = ScsiDriverStatusUpdate.getInstance();
    ScsiClientStore scsiClientStore = informationCenter.getScsiClientStore();

    LaunchDriverRequest launchDriverRequest = (LaunchDriverRequest) taskRequestInfo.getRequest();
    logger.warn("when LaunchScsiDriverProcessor, get the launchDriverRequest :{} ",
        launchDriverRequest);
    long volumeId = launchDriverRequest.getVolumeId();
    int snapshotId = launchDriverRequest.getSnapshotId();
    String scsiIp = launchDriverRequest.getScsiIp();

    try {
      informationCenter.beginLaunchDriver(launchDriverRequest);

      //update the Descriptio
      scsiClientStore.updateScsiDriverDescription(scsiIp, volumeId, snapshotId,
          ScsiDriverDescription.Normal.name());




    } catch (TException e) {
      logger
          .warn("when launchDriver for volume :{}, in client :{}, get exception:", volumeId, scsiIp,
              e);

      if (e instanceof ScsiVolumeLockExceptionThrift) {
        taskNeedProcessAagin = true;
        logger.warn(
            "when launchDriver for volume :{}, in client :{}, which lock info :{}, while try "
                + "again :",
            volumeId, scsiIp, ((ScsiVolumeLockExceptionThrift) e).getDetail());
      }

      scsiDriverStatusUpdate.updateDescription(scsiClientStore, scsiIp, volumeId, snapshotId, e,
          TaskType.LaunchDriver);
    }

    if (!taskNeedProcessAagin) {
      logger.warn("Launch scsi Driver task :{} finish with volume :{}", taskRequestInfo.getTaskId(),
          volumeId);
    }

    return taskNeedProcessAagin;
  }
}
