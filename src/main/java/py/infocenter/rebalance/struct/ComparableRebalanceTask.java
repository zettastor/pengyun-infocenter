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

import javax.annotation.Nonnull;
import py.instance.InstanceId;


@Deprecated
public class ComparableRebalanceTask extends SimpleRebalanceTask implements
    Comparable<ComparableRebalanceTask> {

  private double urgency = 0;

  public ComparableRebalanceTask(SimpleSegUnitInfo segmentUnitToRemove,
      InstanceId instanceToMigrateTo,
      int taskExpireTimeSeconds, double urgency, RebalanceTaskType taskType) {
    super(segmentUnitToRemove, instanceToMigrateTo, taskExpireTimeSeconds, taskType);
    this.urgency = urgency;
  }

  @Override
  public int compareTo(@Nonnull ComparableRebalanceTask o) {
    int urgencyCompare = Double.compare(urgency, o.urgency);
    if (urgencyCompare == 0) {
      return Long.compare(getTaskId(), o.getTaskId());
    } else {
      return urgencyCompare;
    }
  }

  @Override
  public String toString() {
    return "RebalanceTask [super=" + super.toString() + ", urgency=" + urgency + "]";
  }

}
