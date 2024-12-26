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
