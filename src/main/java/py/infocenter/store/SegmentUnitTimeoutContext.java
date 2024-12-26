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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import py.archive.segment.SegmentUnitMetadata;


public class SegmentUnitTimeoutContext implements Delayed {

  /**
   * the segment unit included.
   */
  private SegmentUnitMetadata segUnit;
  /**
   * timeoutInSecond, currently it is 90s.
   **/
  private long timeoutInSecond;
  /**
   * After the time of expiredTime, the segUnit will be timeout.
   */
  private long expiredTime;
  /**
   * In delayQueue time.
   */
  private long inQueueTime;

  /**
   * xx.
   *
   * @param segUnit         segment unit which need to check the timeout status;
   * @param timeoutInSecond after the this time, the segment unit will be timeout;
   */
  public SegmentUnitTimeoutContext(SegmentUnitMetadata segUnit, long timeoutInSecond) {
    this.segUnit = segUnit;
    this.expiredTime = this.segUnit.getLastReported() + timeoutInSecond * 1000;
    this.timeoutInSecond = timeoutInSecond;
    this.inQueueTime = System
        .currentTimeMillis(); // Record the in queue time for debug. When it is created, it is
    // put the delay queue immediately;
  }

  public SegmentUnitMetadata getSegUnit() {
    return this.segUnit;
  }

  @Override
  public int compareTo(Delayed o) {
    if (null == o) {
      return 1;
    }

    if (o == this) {
      return 0;
    }

    SegmentUnitTimeoutContext other = (SegmentUnitTimeoutContext) o;
    // Compare the lastReported time, if the last report time is more large, the possible of
    // segment unit report
    // timeout is more smaller
    long diff = (this.expiredTime - other.expiredTime);
    return (diff == 0L) ? 0 : (diff > 0L ? 1 : -1);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    long now = System.currentTimeMillis();
    return unit.convert(expiredTime - now, TimeUnit.MILLISECONDS);
  }

  /**
   * According to lastReport time, reset the expire time in future;.
   */
  public void resetExpiredTime() {
    this.expiredTime = this.segUnit.getLastReported() + timeoutInSecond * 1000L;
    this.inQueueTime = System.currentTimeMillis();
  }

  public long getInQueueTime() {
    return inQueueTime;
  }

}