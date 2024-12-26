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

package py.infocenter.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.instance.InstanceStatus;
import py.periodic.Worker;


/**
 * check the ha instance is still live or not by check ha instance last report time.
 */
public class HaInstanceEquilibriumSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(HaInstanceEquilibriumSweeper.class);
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;
  private AppContext appContext;
  private boolean enableInstanceEquilibriumVolume;


  @Override
  public void doWork() throws Exception {
    if (!(appContext.getStatus() == InstanceStatus.HEALTHY && enableInstanceEquilibriumVolume)) {
      logger.info(
          "HA instance move volume, only the master do it, the enableInstanceEquilibriumVolume :{}",
          enableInstanceEquilibriumVolume);
      return;
    }

    /* just for test ****/
    if (!instanceVolumeInEquilibriumManger.isStartTest()) {
      logger.warn("for test, current thread not need do Equilibrium task");
      return;
    }

    logger.warn("begin Equilibrium volume work");
    //check the last time Equilibrium is still run or not
    boolean equilibriumStatus = instanceVolumeInEquilibriumManger.equilibriumOk();
    if (equilibriumStatus) {
      instanceVolumeInEquilibriumManger.beginBalanceVolume(appContext.getInstanceId().getId());
    } else {
      logger.warn(
          "there still some volume in equilibrium, clear it and wait next time, the old info is, "

              + "the updateReportToInstancesVersion :{}, the updateTheDatanodeReportTable :{}, the"

              + "volumeReportToInstanceEquilibriumBuildWithVolumeId :{}, the "
              + "volumeReportToInstanceEquilibrium :{}"

              + "the count is :{}, the EquilibriumOkVolume :{}",
          instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion(),
          instanceVolumeInEquilibriumManger.getUpdateTheDatanodeReportTable(),
          instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId(),
          instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibrium(),
          instanceVolumeInEquilibriumManger.getCountEquilibriumNumber(),
          instanceVolumeInEquilibriumManger.getEquilibriumOkVolume());

      instanceVolumeInEquilibriumManger.clearAllEquilibriumInfo();
    }
  }


  public InstanceVolumeInEquilibriumManger getInstanceVolumeInEquilibriumManger() {
    return instanceVolumeInEquilibriumManger;
  }

  public void setInstanceVolumeInEquilibriumManger(
      InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger) {
    this.instanceVolumeInEquilibriumManger = instanceVolumeInEquilibriumManger;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setEnableInstanceEquilibriumVolume(boolean enableInstanceEquilibriumVolume) {
    this.enableInstanceEquilibriumVolume = enableInstanceEquilibriumVolume;
  }

}
