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

package py.infocenter.rebalance.struct;

import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;


/**
 * rebalance task struct that used in internal.
 */
public class InternalRebalanceTask extends BaseRebalanceTask {

  private final long targetArchiveId;
  private final long bornTime;


  public InternalRebalanceTask(SegId segmentId, InstanceId srcInstanceId, InstanceId destInstanceId,
      long targetArchiveId, RebalanceTaskType taskType) {
    super(RequestIdBuilder.get(), srcInstanceId, destInstanceId, segmentId, taskType,
        TaskStatus.OK);

    this.targetArchiveId = targetArchiveId;
    this.bornTime = 0;
  }


  public InternalRebalanceTask(SendRebalanceTask rebalanceTask) {
    super(rebalanceTask.getTaskId(), rebalanceTask.getSourceSegmentUnit().getInstanceId(),
        rebalanceTask.getDestInstanceId(),
        rebalanceTask.getSourceSegmentUnit().getSegId(), rebalanceTask.getTaskType(),
        rebalanceTask.getTaskStatus());

    this.targetArchiveId = rebalanceTask.getTargetArchiveId();
    this.bornTime = rebalanceTask.getBornTime();
  }

  public long getTargetArchiveId() {
    return targetArchiveId;
  }

  public long getBornTime() {
    return bornTime;
  }


  public boolean isBornTimeSet() {
    if (bornTime == 0) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "InternalRebalanceTask{"

        + super.toString()

        + "targetArchiveId=" + targetArchiveId

        + ", bornTime=" + bornTime

        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    InternalRebalanceTask that = (InternalRebalanceTask) o;

    if (targetArchiveId != that.targetArchiveId) {
      return false;
    }
    return bornTime == that.bornTime;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (targetArchiveId ^ (targetArchiveId >>> 32));
    result = 31 * result + (int) (bornTime ^ (bornTime >>> 32));
    return result;
  }
}
