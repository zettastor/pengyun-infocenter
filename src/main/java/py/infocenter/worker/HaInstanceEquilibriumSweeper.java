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
