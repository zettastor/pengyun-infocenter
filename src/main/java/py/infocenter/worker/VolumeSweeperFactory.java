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
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.thrift.datanode.service.DataNodeService;


public class VolumeSweeperFactory implements WorkerFactory {

  private static VolumeSweeper worker;

  private VolumeStore volumeStore;

  private VolumeStatusTransitionStore volumeStatusStore;

  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;

  private int timeout;

  private InstanceStore instanceStore;

  private long deadVolumeToRemoveTime;

  private AppContext appContext;

  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;

  private VolumeJobStoreDb volumeJobStoreDb;

  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private VolumeRecycleStore volumeRecycleStore;
  private int volumeSweeperRate;
  private OperationStore operationStore;
  private PySecurityManager securityManager;


  public long getDeadVolumeToRemoveTime() {
    return deadVolumeToRemoveTime;
  }

  public void setDeadVolumeToRemoveTime(long deadVolumeToRemoveTime) {
    this.deadVolumeToRemoveTime = deadVolumeToRemoveTime;
  }

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

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public void setVolumeStatusTransitionStore(VolumeStatusTransitionStore store) {
    this.volumeStatusStore = store;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }


  public SegmentUnitsDistributionManager getSegmentUnitsDistributionManager() {
    return segmentUnitsDistributionManager;
  }

  public void setSegmentUnitsDistributionManager(
      SegmentUnitsDistributionManager segmentUnitsDistributionManager) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
  }

  public void setVolumeJobStoreDb(VolumeJobStoreDb volumeJobStoreDb) {
    this.volumeJobStoreDb = volumeJobStoreDb;
  }


  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public void setVolumeRecycleStore(VolumeRecycleStore volumeRecycleStore) {
    this.volumeRecycleStore = volumeRecycleStore;
  }

  public void setVolumeSweeperRate(int volumeSweeperRate) {
    this.volumeSweeperRate = volumeSweeperRate;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public void setSecurityManager(PySecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new VolumeSweeper(volumeSweeperRate);
      worker.setVolumeStore(volumeStore);
      worker.setDataNodeClientFactory(dataNodeClientFactory);
      worker.setInstanceStore(instanceStore);
      worker.setTimeout(timeout);
      worker.setDeadTimeToRemove(deadVolumeToRemoveTime);
      worker.setVolumeStatusTransitionStore(volumeStatusStore);
      worker.setAppContext(appContext);
      worker.setVolumeJobStoreDb(volumeJobStoreDb);
      worker.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
      worker.setVolumeRecycleStore(volumeRecycleStore);
      worker.setOperationStore(operationStore);
      worker.setSecurityManager(securityManager);
    }

    return worker;
  }
}
