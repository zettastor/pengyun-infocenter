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
import py.icshare.DomainStore;
import py.infocenter.store.DriverStore;
import py.infocenter.store.ScsiDriverStore;
import py.infocenter.store.VolumeStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class DriverStoreSweeperFactory implements WorkerFactory {

  private static DriverStoreSweeper worker;

  private DriverStore driverStore;
  private DomainStore domainStore;
  private ScsiDriverStore scsiDriverStore;
  private VolumeStore volumeStore;
  private AppContext appContext;

  private long driverReportTimeout;

  public long getDriverReportTimeout() {
    return driverReportTimeout;
  }

  public void setDriverReportTimeout(long driverReportTimeout) {
    this.driverReportTimeout = driverReportTimeout;
  }

  public AppContext getAppContext() {
    return this.appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public DriverStore getDriverStore() {
    return driverStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new DriverStoreSweeper();
      worker.setDriverStore(driverStore);
      worker.setTimeout(driverReportTimeout);
      worker.setAppContext(appContext);
      worker.setDomainStore(domainStore);
      worker.setScsiDriverStore(scsiDriverStore);
      worker.setVolumeStore(volumeStore);
    }
    return worker;
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public void setScsiDriverStore(ScsiDriverStore scsiDriverStore) {
    this.scsiDriverStore = scsiDriverStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }
}
