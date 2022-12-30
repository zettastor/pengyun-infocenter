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

import py.app.context.AppContext;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class HaInstanceEquilibriumSweeperFactory implements WorkerFactory {

  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;
  private AppContext appContext;
  private boolean enableInstanceEquilibriumVolume;

  @Override
  public Worker createWorker() {
    HaInstanceEquilibriumSweeper worker = new HaInstanceEquilibriumSweeper();
    worker.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    worker.setAppContext(appContext);
    worker.setEnableInstanceEquilibriumVolume(enableInstanceEquilibriumVolume);
    return worker;
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
