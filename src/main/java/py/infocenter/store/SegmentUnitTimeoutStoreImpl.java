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
