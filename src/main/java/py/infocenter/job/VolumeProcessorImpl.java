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
