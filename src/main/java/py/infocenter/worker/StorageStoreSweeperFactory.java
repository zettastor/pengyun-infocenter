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
import py.icshare.InstanceMaintenanceDbStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePoolStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class StorageStoreSweeperFactory implements WorkerFactory {

  private static StorageStoreSweeper worker;

  private StorageStore storageStore;

  private StoragePoolStore storagePoolStore;

  private DomainStore domainStore;

  private VolumeStore volumeStore;

  private long segmentSize;

  private int timeToRemove;

  private AppContext appContext;

  private InstanceMaintenanceDbStore instanceMaintenanceDbStore;

  private int waitCollectVolumeInfoSecond = 30;

  public void setWaitCollectVolumeInfoSecond(int waitCollectVolumeInfoSecond) {
    this.waitCollectVolumeInfoSecond = waitCollectVolumeInfoSecond;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new StorageStoreSweeper();
      worker.setInstanceMetadataStore(storageStore);
      worker.setTimeToRemove(timeToRemove);
      worker.setAppContext(appContext);
      worker.setStoragePoolStore(storagePoolStore);
      worker.setDomainStore(domainStore);
      worker.setVolumeStore(volumeStore);
      worker.setSegmentSize(segmentSize);
      worker.setInstanceMaintenanceDbStore(instanceMaintenanceDbStore);
      worker.setWaitCollectVolumeInfoSecond(waitCollectVolumeInfoSecond);
    }
    return worker;
  }

  public AppContext getAppContext() {
    return this.appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public int getTimeToRemove() {
    return timeToRemove;
  }

  public void setTimeToRemove(int timeToRemove) {
    this.timeToRemove = timeToRemove;
  }

  public StorageStore getStorageStore() {
    return storageStore;
  }

  public void setStorageStore(StorageStore storageStore) {
    this.storageStore = storageStore;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public InstanceMaintenanceDbStore getInstanceMaintenanceDbStore() {
    return instanceMaintenanceDbStore;
  }

  public void setInstanceMaintenanceDbStore(InstanceMaintenanceDbStore instanceMaintenanceDbStore) {
    this.instanceMaintenanceDbStore = instanceMaintenanceDbStore;
  }
}
