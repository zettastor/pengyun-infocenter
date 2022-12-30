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
