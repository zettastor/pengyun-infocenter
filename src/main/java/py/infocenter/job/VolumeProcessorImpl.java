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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import py.app.context.AppContext;
import py.infocenter.store.control.VolumeJobStore;
import py.instance.InstanceStatus;


public class VolumeProcessorImpl implements VolumeProcessor {

  private static final Logger logger = LoggerFactory.getLogger(VolumeProcessorImpl.class);

  private VolumeJobStore volumeJobStore;

  private AppContext appContext;

  @Override
  @Scheduled(fixedDelay = 1000)
  public void processCreateRequest() {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      return;
    }

    logger.info("Going to process create volume requests");
    boolean hasMore = true;
    while (hasMore) {
      hasMore = volumeJobStore.processCreateVolumeRequest();
    }
    logger.info("Processed all create volume requests");
  }


  public void setVolumeJobStore(VolumeJobStore volumeJobStore) {
    this.volumeJobStore = volumeJobStore;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }


  @Override
  @Scheduled(fixedDelay = 3000)
  public void processDeleteRequest() {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      return;
    }
    logger.info("Going to process delete volume requests");
    volumeJobStore.processDeleteVolumeRequest();
    logger.info("Processed all delete volume requests");
  }
}
