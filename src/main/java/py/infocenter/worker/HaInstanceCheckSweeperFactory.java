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
