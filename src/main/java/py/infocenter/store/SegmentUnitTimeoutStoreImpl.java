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

package py.infocenter.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.DelayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitMetadata;
import py.informationcenter.Utils;

/**
 * This class is just a container, which contain all segment units to check if their report time is
 * timeout it use a delay queue.
 *
 */
public class SegmentUnitTimeoutStoreImpl implements SegmentUnitTimeoutStore {

  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitTimeoutStore.class);
  private final long segUnitTimeoutInSecond; // timeout for segment unit report, currently it is 90s
  private DelayQueue<SegmentUnitTimeoutContext> timeOutQueue;

  public SegmentUnitTimeoutStoreImpl(long timeout) {
    this.segUnitTimeoutInSecond = timeout;
    timeOutQueue = new DelayQueue<SegmentUnitTimeoutContext>();
  }



  @Override
  public void addSegmentUnit(SegmentUnitMetadata segUnit) {
    SegmentUnitTimeoutContext segunitTimeoutContext = new SegmentUnitTimeoutContext(segUnit,
        segUnitTimeoutInSecond);
    timeOutQueue.put(segunitTimeoutContext);
  }

  /**
   * xx.
   *
   * @param volumes Volume whose segment unit is timeout
   */
  @Override
  public int drainTo(Collection<Long> volumes) {
    int count = 0;
    List<SegmentUnitTimeoutContext> segmentUnitCollection = new ArrayList<>();
    int timeoutNum = timeOutQueue.drainTo(segmentUnitCollection);
    if (timeoutNum == 0) {
      return 0;
    }

    // check it really timeout
    long now = System.currentTimeMillis();
    for (SegmentUnitTimeoutContext segContext : segmentUnitCollection) {
      if (now - segContext.getSegUnit().getLastReported() > segUnitTimeoutInSecond * 1000) {
        // it is really timeout
        volumes.add(segContext.getSegUnit().getSegId().getVolumeId().getId());
        count++;
        logger.debug(
            "segment is timeout: lastReportTime is {}, now is {}, timeout is {}, segmentunit is {}",
            Utils.millsecondToString(segContext.getSegUnit().getLastReported()),
            Utils.millsecondToString(now), this.segUnitTimeoutInSecond, segContext.getSegUnit());
      } else { // for the segment unit not timeout, put the delayQueue again
        segContext.resetExpiredTime();
        this.timeOutQueue.add(segContext);
      }
    }

    return count;
  }

  /**
   * Clear the segment unit data.
   */
  @Override
  public void clear() {
    this.timeOutQueue.clear();
  }

}
