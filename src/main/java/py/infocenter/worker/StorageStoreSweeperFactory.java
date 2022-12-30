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
