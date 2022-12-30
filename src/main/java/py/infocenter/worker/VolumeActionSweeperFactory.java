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
import py.client.thrift.GenericThriftClientFactory;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.thrift.datanode.service.DataNodeService;


public class VolumeActionSweeperFactory implements WorkerFactory {

  private static VolumeActionSweeper worker;

  private VolumeStore volumeStore;

  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;

  private int timeout;

  private InstanceStore instanceStore;

  private AppContext appContext;

  private StoragePoolStore storagePoolStore;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;
  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private VolumeInformationManger volumeInformationManger;
  private OperationStore operationStore;
  private PySecurityManager securityManager;

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeClientFactory() {
    return dataNodeClientFactory;
  }

  public void setDataNodeClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
    this.dataNodeClientFactory = dataNodeClientFactory;
  }

  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public AppContext getAppContext() {
    return this.appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public SegmentUnitsDistributionManager getSegmentUnitsDistributionManager() {
    return segmentUnitsDistributionManager;
  }

  public void setSegmentUnitsDistributionManager(
      SegmentUnitsDistributionManager segmentUnitsDistributionManager) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new VolumeActionSweeper();
      worker.setVolumeStore(volumeStore);
      worker.setAppContext(appContext);
      worker.setStoragePoolStore(storagePoolStore);
      worker.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);
      worker.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
      worker.setVolumeInformationManger(volumeInformationManger);
      worker.setOperationStore(operationStore);
      worker.setSecurityManager(securityManager);
    }

    return worker;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public LockForSaveVolumeInfo getLockForSaveVolumeInfo() {
    return lockForSaveVolumeInfo;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public void setSecurityManager(PySecurityManager securityManager) {
    this.securityManager = securityManager;
  }
}
