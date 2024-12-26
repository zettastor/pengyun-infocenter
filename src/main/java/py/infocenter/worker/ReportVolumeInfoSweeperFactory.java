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
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.reportvolume.ReportVolumeManager;
import py.infocenter.store.VolumeStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

/**
 * report volume inf to master HA.
 */
public class ReportVolumeInfoSweeperFactory implements WorkerFactory {

  private VolumeStore volumeStore;
  private InformationCenterClientFactory infoCenterClientFactory;
  private AppContext appContext;
  private VolumeInformationManger volumeInformationManger;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private ReportVolumeManager reportVolumeManager;

  @Override
  public Worker createWorker() {
    ReportVolumeInfoSweeper reportVolumeInfoSweeper = new ReportVolumeInfoSweeper();
    reportVolumeInfoSweeper.setVolumeStore(volumeStore);
    reportVolumeInfoSweeper.setAppContext(appContext);
    reportVolumeInfoSweeper.setInfoCenterClientFactory(infoCenterClientFactory);
    reportVolumeInfoSweeper.setVolumeInformationManger(volumeInformationManger);
    reportVolumeInfoSweeper.setReportVolumeManager(reportVolumeManager);
    return reportVolumeInfoSweeper;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public InformationCenterClientFactory getInfoCenterClientFactory() {
    return infoCenterClientFactory;
  }

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public InstanceIncludeVolumeInfoManger getInstanceIncludeVolumeInfoManger() {
    return instanceIncludeVolumeInfoManger;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public void setReportVolumeManager(ReportVolumeManager reportVolumeManager) {
    this.reportVolumeManager = reportVolumeManager;
  }
}
