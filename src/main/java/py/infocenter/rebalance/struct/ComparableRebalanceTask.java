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
