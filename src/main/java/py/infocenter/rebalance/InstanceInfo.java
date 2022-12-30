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
