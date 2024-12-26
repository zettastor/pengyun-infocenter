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
