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

import py.instance.InstanceId;
import py.rebalance.RebalanceTask;


@Deprecated
public class SimpleRebalanceTask extends RebalanceTask {

  private SimpleSegUnitInfo mySourceSegmentUnit;



  public SimpleRebalanceTask(SimpleSegUnitInfo segmentUnitToRemove, InstanceId instanceToMigrateTo,
      int taskExpireTimeSeconds, RebalanceTaskType taskType) {
    super(segmentUnitToRemove.getSegmentUnit(), instanceToMigrateTo, taskExpireTimeSeconds,
        taskType);
    this.mySourceSegmentUnit = segmentUnitToRemove;
  }

  public SimpleSegUnitInfo getMySourceSegmentUnit() {
    return mySourceSegmentUnit;
  }

  @Override
  public InstanceId getInstanceToMigrateFrom() {
    return mySourceSegmentUnit.getInstanceId();
  }

}
