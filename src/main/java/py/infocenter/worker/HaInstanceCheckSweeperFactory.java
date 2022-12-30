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
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class HaInstanceCheckSweeperFactory implements WorkerFactory {

  private VolumeInformationManger volumeInformationManger;
  private AppContext appContext;
  private int instanceTimeOutCheck;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  @Override
  public Worker createWorker() {
    HaInstanceCheckSweeper worker = new HaInstanceCheckSweeper();
    worker.setVolumeInformationManger(volumeInformationManger);
    worker.setInstanceTimeOutCheck(instanceTimeOutCheck);
    worker.setAppContext(appContext);
    worker.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    worker.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    return worker;
  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public int getInstanceTimeOutCheck() {
    return instanceTimeOutCheck;
  }

  public void setInstanceTimeOutCheck(int instanceTimeOutCheck) {
    this.instanceTimeOutCheck = instanceTimeOutCheck;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public InstanceIncludeVolumeInfoManger getInstanceIncludeVolumeInfoManger() {
    return instanceIncludeVolumeInfoManger;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public void setInstanceVolumeInEquilibriumManger(
      InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger) {
    this.instanceVolumeInEquilibriumManger = instanceVolumeInEquilibriumManger;
  }
}
