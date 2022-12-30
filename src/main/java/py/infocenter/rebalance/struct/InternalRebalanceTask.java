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
