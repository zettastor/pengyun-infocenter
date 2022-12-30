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

package py.infocenter;

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContextImpl;
import py.icshare.DomainStore;
import py.icshare.ServerNode;
import py.icshare.authorization.InMemoryAccountStoreImpl;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.store.DriverClientStore;
import py.infocenter.store.DriverStore;
import py.infocenter.store.InfoCenterStoreSet;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;

public class InfoCenterAppContext extends AppContextImpl {

  private final Logger logger = LoggerFactory.getLogger(InfoCenterAppContext.class);
  private InfoCenterStoreSet storeSet;
  private Map<String, Long> serverNodeReportTimeMap;
  private ServerNodeStore serverNodeStore;
  private DriverClientStore driverClientStore;

  private OperationStore operationStore;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private VolumeStore volumeStore;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  private InMemoryAccountStoreImpl inMemoryAccountStore;

  public InfoCenterAppContext(String name) {
    super(name, InstanceStatus.SUSPEND);
  }

  @Override
  public void setStatus(InstanceStatus status) {
    try {
      if (super.getStatus() == InstanceStatus.SUSPEND && status == InstanceStatus.HEALTHY) {
        logger.warn("Reload all database data into memory.");
        DomainStore domainStore = storeSet.getDomainStore();
        domainStore.clearMemoryMap();
        try {
          domainStore.listAllDomains();
        } catch (Exception e) {
          logger.error("can not list domain from database", e);
          throw new RuntimeException();
        }

        DriverStore driverStore = storeSet.getDriverStore();
        driverStore.clearMemoryData();
        driverStore.list();

        StorageStore storageStore = storeSet.getStorageStore();
        storageStore.clearMemoryData();
        storageStore.list();

        StoragePoolStore storagePoolStore = storeSet.getStoragePoolStore();
        storagePoolStore.clearMemoryMap();
        try {
          storagePoolStore.listAllStoragePools();
        } catch (Exception e) {
          logger.error("can not list storage pools", e);
          throw new RuntimeException();
        }

        VolumeStore volumeStore = storeSet.getVolumeStore();
        volumeStore.loadVolumeFromDbToMemory();

        reloadServerNodeReportTimeMap();

        operationStore.clearMemory();
        operationStore.getAllOperation();

        //load the instance volume info
        instanceIncludeVolumeInfoManger.initVolumeInfo();

        //clear the old value
        instanceVolumeInEquilibriumManger.clearAllEquilibriumInfo();

        //clear and load
        driverClientStore.clearMemoryData();
        driverClientStore.loadToMemory();

        //clear
        inMemoryAccountStore.clearMemoryData();
      }

      /* init the follower **/
      if (super.getStatus() == InstanceStatus.SUSPEND && status == InstanceStatus.SUSPEND) {
        logger.warn("after choose, current instance not change");

        /*clean the volume in memory which for report ,when the HA disconnect and
         * connect again
         ****/

      }

      /* if the old master(network down) change to follower, the memory about db must clean
       * the follower get value which must from db
       *
       * **/

      if (super.getStatus() == InstanceStatus.HEALTHY && status == InstanceStatus.SUSPEND) {

        logger.warn("current status from OK to SUSPEND, clean the memory date");
        DomainStore domainStore = storeSet.getDomainStore();
        domainStore.clearMemoryMap();

        DriverStore driverStore = storeSet.getDriverStore();
        driverStore.clearMemoryData();

        StorageStore storageStore = storeSet.getStorageStore();
        storageStore.clearMemoryData();

        StoragePoolStore storagePoolStore = storeSet.getStoragePoolStore();
        storagePoolStore.clearMemoryMap();

        VolumeStore volumeStore = storeSet.getVolumeStore();
        volumeStore.clearData();

       

        operationStore.clearMemory();

        //clear instance volume info
        instanceIncludeVolumeInfoManger.clearVolumeInfoMap();

        //clear the old value
        instanceVolumeInEquilibriumManger.clearAllEquilibriumInfo();

        //clear the old value
        driverClientStore.clearMemoryData();
      }
    } catch (Exception e) {
      logger.error("caught an exception when reload all database into memory.", e);
      throw e;
    } finally {
      super.setStatus(status);
    }
  }

  //just for test

  public void setStatus(InstanceStatus status, boolean test) {
    try {
      if (super.getStatus() == InstanceStatus.SUSPEND && status == InstanceStatus.HEALTHY) {
        logger.warn("Reload all database data into memory.");
      }

      /* init the follower **/
      if (super.getStatus() == InstanceStatus.SUSPEND && status == InstanceStatus.SUSPEND) {
        logger.warn("init the follower");
      }
    } catch (Exception e) {
      logger.error("caught an exception when reload all database into memory.", e);
      throw e;
    } finally {
      super.setStatus(status);
    }
  }

  private void reloadServerNodeReportTimeMap() {
    List<ServerNode> serverNodes = serverNodeStore.listAllServerNodes();
    if (serverNodes == null || serverNodes.isEmpty()) {
      logger.warn("when reloadServerNodeReportTimeMap, but it is no any value");
      return;
    }

    for (ServerNode serverNode : serverNodes) {
      serverNodeReportTimeMap.putIfAbsent(serverNode.getId(), System.currentTimeMillis());
    }
  }

  public InfoCenterStoreSet getStoreSet() {
    return storeSet;
  }

  public void setStoreSet(InfoCenterStoreSet storeSet) {
    this.storeSet = storeSet;
  }

  public void setServerNodeReportTimeMap(Map<String, Long> serverNodeReportTimeMap) {
    this.serverNodeReportTimeMap = serverNodeReportTimeMap;
  }

  public void setServerNodeStore(ServerNodeStore serverNodeStore) {
    this.serverNodeStore = serverNodeStore;
  }

  public OperationStore getOperationStore() {
    return operationStore;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
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

  public void setDriverClientStore(DriverClientStore driverClientStore) {
    this.driverClientStore = driverClientStore;
  }

  public InMemoryAccountStoreImpl getInMemoryAccountStore() {
    return inMemoryAccountStore;
  }

  public void setInMemoryAccountStore(
      InMemoryAccountStoreImpl inMemoryAccountStore) {
    this.inMemoryAccountStore = inMemoryAccountStore;
  }
}
