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
import py.icshare.CapacityRecordStore;
import py.icshare.DomainStore;
import py.infocenter.service.ServerStatusCheck;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePoolStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class TimeoutSweeperFactory implements WorkerFactory {

  private VolumeStore volumeStore;
  private VolumeStatusTransitionStore volumeStatusStore;
  private SegmentUnitTimeoutStore segUnitTimeoutStore;
  private AppContext appContext;
  private Long nextActionTimeIntervalMs;
  private DomainStore domainStore;
  private StoragePoolStore storagePoolStore;
  private CapacityRecordStore capacityRecordStore;
  private StorageStore storageStore;
  private int takeSampleInterValSecond;
  private int storeCapacityRecordCount;
  private long roundTimeInterval;
  private ServerStatusCheck serverStatusCheck;

  @Override
  public Worker createWorker() {
    TimeoutSweeper worker = new TimeoutSweeper();
    worker.setVolumeStatusStore(volumeStatusStore);
    worker.setVolumeStore(volumeStore);
    worker.setSegUnitTimeoutStore(segUnitTimeoutStore);
    worker.setAppContext(appContext);
    worker.setNextActionTimeIntervalMs(nextActionTimeIntervalMs);
    worker.setDomainStore(domainStore);
    worker.setStoragePoolStore(storagePoolStore);
    worker.setCapacityRecordStore(capacityRecordStore);
    worker.setStorageStore(storageStore);
    worker.setTakeSampleInterValSecond(takeSampleInterValSecond);
    worker.setStoreCapacityRecordCount(storeCapacityRecordCount);
    worker.setRoundTimeInterval(roundTimeInterval);
    worker.setServerStatusCheck(serverStatusCheck);
    return worker;
  }

  public void setServerStatusCheck(ServerStatusCheck serverStatusCheck) {
    this.serverStatusCheck = serverStatusCheck;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public void setVolumeStatusStore(VolumeStatusTransitionStore volumeStatusStore) {
    this.volumeStatusStore = volumeStatusStore;
  }

  public void setSegUnitTimeoutStore(SegmentUnitTimeoutStore segUnitTimeoutStore) {
    this.segUnitTimeoutStore = segUnitTimeoutStore;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public Long getNextActionTimeIntervalMs() {
    return nextActionTimeIntervalMs;
  }

  public void setNextActionTimeIntervalMs(Long nextActionTimeIntervalMs) {
    this.nextActionTimeIntervalMs = nextActionTimeIntervalMs;
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public CapacityRecordStore getCapacityRecordStore() {
    return capacityRecordStore;
  }

  public void setCapacityRecordStore(CapacityRecordStore capacityRecordStore) {
    this.capacityRecordStore = capacityRecordStore;
  }

  public StorageStore getStorageStore() {
    return storageStore;
  }

  public void setStorageStore(StorageStore storageStore) {
    this.storageStore = storageStore;
  }

  public int getTakeSampleInterValSecond() {
    return takeSampleInterValSecond;
  }

  public void setTakeSampleInterValSecond(int takeSampleInterValSecond) {
    this.takeSampleInterValSecond = takeSampleInterValSecond;
  }

  public int getStoreCapacityRecordCount() {
    return storeCapacityRecordCount;
  }

  public void setStoreCapacityRecordCount(int storeCapacityRecordCount) {
    this.storeCapacityRecordCount = storeCapacityRecordCount;
  }

  public long getRoundTimeInterval() {
    return roundTimeInterval;
  }

  public void setRoundTimeInterval(long roundTimeInterval) {
    this.roundTimeInterval = roundTimeInterval;
  }
}
