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

package py.infocenter.rebalance;

import java.util.Collection;
import javax.annotation.Nonnull;
import py.infocenter.rebalance.exception.NoSuitableTask;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;


public abstract class InstanceInfo implements Comparable<InstanceInfo> {

  protected InstanceId instanceId;

  public InstanceInfo(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public abstract double calculatePressure();

  public abstract RebalanceTask selectArebalanceTask(Collection<InstanceInfo> destinations,
      RebalanceTask.RebalanceTaskType taskType) throws NoSuitableTask;

  public abstract int getGroupId();

  public abstract int getDiskCount();

  public abstract int getFreeFlexibleSegmentUnitCount();

  public abstract long getFreeSpace();

  @Override
  public final int compareTo(@Nonnull InstanceInfo other) {
    int pressureCompare = Double.compare(calculatePressure(), other.calculatePressure());
    if (pressureCompare == 0) {
      return Long.compare(getInstanceId().getId(), other.getInstanceId().getId());
    } else {
      return pressureCompare;
    }
  }

  public InstanceId getInstanceId() {
    return this.instanceId;
  }

}
