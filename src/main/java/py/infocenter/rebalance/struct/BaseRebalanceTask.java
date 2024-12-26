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
import py.instance.InstanceId;

/**
 * A RebalanceTask must contain two things: <br> * One is the segment unit to remove. <br> The other
 * is the data node for that segment unit to migrate to. <br>
 */
public class BaseRebalanceTask {

  private final long taskId;
  private final InstanceId srcInstanceId;
  private final InstanceId destInstanceId;
  private final SegId segId;
  private final RebalanceTaskType taskType;
  private TaskStatus taskStatus;


  public BaseRebalanceTask(long taskId, InstanceId srcInstanceId, InstanceId destInstanceId,
      SegId segId, RebalanceTaskType taskType, TaskStatus taskStatus) {
    this.taskId = taskId;
    this.srcInstanceId = srcInstanceId;
    this.destInstanceId = destInstanceId;
    this.segId = segId;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
  }

  public long getTaskId() {
    return taskId;
  }

  public InstanceId getSrcInstanceId() {
    return srcInstanceId;
  }

  public InstanceId getDestInstanceId() {
    return destInstanceId;
  }

  public SegId getSegId() {
    return segId;
  }

  public RebalanceTaskType getTaskType() {
    return taskType;
  }

  public TaskStatus getTaskStatus() {
    return taskStatus;
  }

  public void setTaskStatus(TaskStatus taskStatus) {
    this.taskStatus = taskStatus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseRebalanceTask that = (BaseRebalanceTask) o;

    if (taskId != that.taskId) {
      return false;
    }
    if (srcInstanceId != null ? !srcInstanceId.equals(that.srcInstanceId)
        : that.srcInstanceId != null) {
      return false;
    }
    if (destInstanceId != null ? !destInstanceId.equals(that.destInstanceId)
        : that.destInstanceId != null) {
      return false;
    }
    if (segId != null ? !segId.equals(that.segId) : that.segId != null) {
      return false;
    }
   
   
   
    return taskType == that.taskType;
  }

  @Override
  public int hashCode() {
    int result = (int) (taskId ^ (taskId >>> 32));
    result = 31 * result + (srcInstanceId != null ? srcInstanceId.hashCode() : 0);
    result = 31 * result + (destInstanceId != null ? destInstanceId.hashCode() : 0);
    result = 31 * result + (segId != null ? segId.hashCode() : 0);
    result = 31 * result + (taskType != null ? taskType.hashCode() : 0);
   
   
    return result;
  }

  @Override
  public String toString() {
    return "BaseRebalanceTask{"

        + ", segId=" + segId

        + ", " + srcInstanceId

        + " ---> " + destInstanceId

        + ", taskType=" + taskType

        + ", taskStatus=" + taskStatus

        + ", taskId=" + taskId

        + '}';
  }

  public enum RebalanceTaskType {
    PrimaryRebalance,       //primary rebalance
    PSRebalance,            //secondary of primary rebalance(PS combination rebalance)
    ArbiterRebalance        //arbiter rebalance
  }

  public enum TaskStatus {
    OK,                     //task is ok
    ILLEGAL,                //task is illegal
  }
}
