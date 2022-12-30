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
 * A RebalanceTask which communication with datanode.
 */
public class SendRebalanceTask extends BaseRebalanceTask {

  private final int becomeCandidateExpireTimeSeconds = 600;

  private final int taskExpireTimeSeconds;
  private final SimulateSegmentUnit sourceSegmentUnit;
  private long targetArchiveId;
  private long bornTime;


  public SendRebalanceTask(long taskId, SimulateSegmentUnit sourceSegmentUnit,
      InstanceId destInstanceId,
      RebalanceTaskType taskType) {
    super(taskId, sourceSegmentUnit.getInstanceId(), destInstanceId, sourceSegmentUnit.getSegId(),
        taskType, TaskStatus.OK);

    this.bornTime = System.currentTimeMillis();
    this.taskExpireTimeSeconds = Integer.MAX_VALUE;
    this.sourceSegmentUnit = sourceSegmentUnit;
  }



  public SendRebalanceTask(SimulateSegmentUnit sourceSegmentUnit, InstanceId destInstanceId,
      int taskExpireTimeSeconds,
      RebalanceTaskType taskType) {
    super(RequestIdBuilder.get(), sourceSegmentUnit.getInstanceId(), destInstanceId,
        sourceSegmentUnit.getSegId(), taskType, TaskStatus.OK);

    this.bornTime = System.currentTimeMillis();
    this.taskExpireTimeSeconds = taskExpireTimeSeconds;
    this.sourceSegmentUnit = sourceSegmentUnit;
  }



  public SendRebalanceTask(long taskId, SimulateSegmentUnit sourceSegmentUnit,
      InstanceId srcInstanceId, InstanceId destInstanceId,
      SegId segId, int taskExpireTimeSeconds, RebalanceTaskType taskType) {
    super(taskId, srcInstanceId, destInstanceId, segId, taskType, TaskStatus.OK);

    this.taskExpireTimeSeconds = taskExpireTimeSeconds;
    this.bornTime = System.currentTimeMillis();
    this.sourceSegmentUnit = sourceSegmentUnit;
  }



  public SendRebalanceTask(long taskId, SimulateSegmentUnit sourceSegmentUnit,
      InstanceId destInstanceId, int taskExpireTimeSeconds,
      RebalanceTaskType taskType) {
    super(taskId, sourceSegmentUnit.getInstanceId(), destInstanceId, sourceSegmentUnit.getSegId(),
        taskType, TaskStatus.OK);

    this.taskExpireTimeSeconds = taskExpireTimeSeconds;
    this.bornTime = System.currentTimeMillis();
    this.sourceSegmentUnit = sourceSegmentUnit;
  }

  public long getBornTime() {
    return bornTime;
  }

  public void setBornTime(long bornTime) {
    this.bornTime = bornTime;
  }

  public SimulateSegmentUnit getSourceSegmentUnit() {
    return sourceSegmentUnit;
  }

  public InstanceId getInstanceToMigrateFrom() {
    return sourceSegmentUnit.getInstanceId();
  }

  public boolean expired() {
    return System.currentTimeMillis() - bornTime > taskExpireTimeSeconds * 1000;
  }

  public boolean becomeCandidateExpired() {
    return System.currentTimeMillis() - bornTime > becomeCandidateExpireTimeSeconds * 1000;
  }

  public long getTargetArchiveId() {
    return targetArchiveId;
  }

  public void setTargetArchiveId(long targetArchiveId) {
    this.targetArchiveId = targetArchiveId;
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

    SendRebalanceTask that = (SendRebalanceTask) o;

    if (becomeCandidateExpireTimeSeconds != that.becomeCandidateExpireTimeSeconds) {
      return false;
    }
    if (taskExpireTimeSeconds != that.taskExpireTimeSeconds) {
      return false;
    }
    if (targetArchiveId != that.targetArchiveId) {
      return false;
    }
    if (bornTime != that.bornTime) {
      return false;
    }



    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + becomeCandidateExpireTimeSeconds;
    result = 31 * result + taskExpireTimeSeconds;


    result = 31 * result + (int) (targetArchiveId ^ (targetArchiveId >>> 32));
    result = 31 * result + (int) (bornTime ^ (bornTime >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "SendRebalanceTask{"

        + super.toString()

        + ", bornTime=" + bornTime

        + ", taskExpireTimeSeconds=" + taskExpireTimeSeconds

        + ", becomeCandidateExpireTimeSeconds=" + becomeCandidateExpireTimeSeconds

        + ", sourceSegmentUnit=" + sourceSegmentUnit

        + ", targetArchiveId=" + targetArchiveId

        + '}';
  }
}
