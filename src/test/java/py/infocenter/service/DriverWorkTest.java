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

package py.infocenter.service;

import static org.junit.Assert.assertTrue;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.volume.VolumeInAction.NULL;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.icshare.DomainStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.DriverStore;
import py.infocenter.store.ScsiDriverStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.test.base.InformationCenterAppConfigTest;
import py.infocenter.worker.DriverStoreSweeper;
import py.informationcenter.AccessPermissionType;
import py.instance.InstanceStatus;
import py.storage.EdRootpathSingleton;
import py.test.TestBase;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class DriverWorkTest extends TestBase {

  public static final int DEFAULT_NOT_CLIENT_CONNECT = -1;
  private InfoCenterAppContext appContext;
  private DriverStore driverStore;
  private ScsiDriverStore scsiDriverStore;
  private DomainStore domainStore;
  private VolumeStore volumeStore;
  private DriverStoreSweeper driverStoreSweeper;

  
  public void init() throws Exception {
    super.init();
    @SuppressWarnings("resource")
    ApplicationContext ctx = new AnnotationConfigApplicationContext(
        InformationCenterAppConfigTest.class);
    driverStore = (DriverStore) ctx.getBean("driverStore");
    volumeStore = (VolumeStore) ctx.getBean("dbVolumeStore");
    scsiDriverStore = (ScsiDriverStore) ctx.getBean("scsiDriverStore");
    domainStore = (DomainStore) ctx.getBean("domainStore");

    appContext = new InfoCenterAppContext("test");
    appContext.setStatus(InstanceStatus.HEALTHY, true);

    driverStoreSweeper = new DriverStoreSweeper();
    driverStoreSweeper.setDriverStore(driverStore);
    driverStoreSweeper.setTimeout(1);
    driverStoreSweeper.setAppContext(appContext);
    driverStoreSweeper.setDomainStore(domainStore);
    driverStoreSweeper.setScsiDriverStore(scsiDriverStore);
    driverStoreSweeper.setVolumeStore(volumeStore);
  }

  @Ignore
  @Test
  public void testCheckVolumeDriverClientInfo() throws Exception {
    logger.warn("the appContext is :{},{}", appContext, domainStore);
    EdRootpathSingleton edRootpathSingleton = EdRootpathSingleton.getInstance();
    edRootpathSingleton.setRootPath("/tmp/testing");

    long volumeId = 1111;
    final long accountId = 1862755152385798555L;
    VolumeMetadata volumeMetadata = makeVolume(volumeId);
    volumeStore.saveVolume(volumeMetadata);

    //1. check the time, not any client
    driverStoreSweeper.doWork();
    VolumeMetadata volumeMetadataGet = volumeStore.getVolume(volumeId);
    long time = volumeMetadataGet.getClientLastConnectTime();
    Assert.assertTrue(time == DEFAULT_NOT_CLIENT_CONNECT);

    //2. report one driver
    long driverContainerId = 122;
    final long beginTime = System.currentTimeMillis();
    DriverType driverType = DriverType.NBD;
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setVolumeId(volumeId);
    driverMetadata.setDriverContainerId(driverContainerId);
    driverMetadata.setSnapshotId(0);
    driverMetadata.setDriverType(driverType);
    driverMetadata.setAccountId(accountId);
    driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);

    Map<String, AccessPermissionType> clientHostAccessRule = new HashMap<>();
    clientHostAccessRule.put("10.0.0.80", AccessPermissionType.READ);
    driverMetadata.setClientHostAccessRule(clientHostAccessRule);
    driverStore.save(driverMetadata);

    //check the time, have client
    driverStoreSweeper.doWork();
    volumeMetadataGet = volumeStore.getVolume(volumeId);
    time = volumeMetadataGet.getClientLastConnectTime();

    //3. report two driver
    final long driverContainerId2 = 123;
    driverType = DriverType.NBD;
    driverMetadata = new DriverMetadata();
    driverMetadata.setVolumeId(volumeId);
    driverMetadata.setDriverContainerId(driverContainerId2);
    driverMetadata.setSnapshotId(0);
    driverMetadata.setDriverType(driverType);
    driverMetadata.setAccountId(accountId);
    driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);

    clientHostAccessRule = new HashMap<>();
    clientHostAccessRule.put("10.0.0.81", AccessPermissionType.READ);
    driverMetadata.setClientHostAccessRule(clientHostAccessRule);
    driverStore.save(driverMetadata);

    //check the time, have client, the connect time not change
    Assert.assertTrue(driverStore.list().size() == 2);
    driverStoreSweeper.doWork();
    volumeMetadataGet = volumeStore.getVolume(volumeId);
    long timeGet = volumeMetadataGet.getClientLastConnectTime();
    Assert.assertTrue(time == timeGet);

    //4. one driver shut down
    driverStore.delete(driverContainerId2, volumeId, driverType, 0);
    Assert.assertTrue(driverStore.list().size() == 1);
    //check the time, still have client
    driverStoreSweeper.doWork();
    volumeMetadataGet = volumeStore.getVolume(volumeId);
    time = volumeMetadataGet.getClientLastConnectTime();
    Assert.assertTrue(time == timeGet);

    //5. one driver shut down, no driver
    driverStore.delete(driverContainerId, volumeId, driverType, 0);
    Assert.assertTrue(driverStore.list().isEmpty());
    //check the time, make the last disconnect time, set -1 * time
    driverStoreSweeper.doWork();
    volumeMetadataGet = volumeStore.getVolume(volumeId);
    time = volumeMetadataGet.getClientLastConnectTime();

    File file = new File("/tmp/testing");
    if (file.exists()) {
      file.delete();
    }
  }

  
  public VolumeMetadata makeVolume(long volumeId) {
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, 20002,
        20003, null, 0L, 0L);

    volumeMetadata.setVolumeId(volumeId);
    volumeMetadata.setRootVolumeId(1003);
    volumeMetadata.setChildVolumeId(null);
    volumeMetadata.setVolumeSize(1005);
    volumeMetadata.setExtendingSize(1006);
    volumeMetadata.setName("stdname");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(1008);
    volumeMetadata.setDeadTime(0L);
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setInAction(NULL);
    volumeMetadata.setVolumeDescription("test1");
    volumeMetadata.setClientLastConnectTime(-1);
    return volumeMetadata;
  }

  @Before
  public void beforeTest() throws Exception {
    driverStore.clearMemoryData();
    assertTrue(driverStore.list().isEmpty());
  }

  @After
  public void clear() throws Exception {
    driverStore.clearMemoryData();
    assertTrue(driverStore.list().isEmpty());
  }


}
