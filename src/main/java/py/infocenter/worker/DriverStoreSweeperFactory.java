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
