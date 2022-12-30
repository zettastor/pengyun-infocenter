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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.informationcenter.StoragePoolStrategy.Capacity;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import junit.framework.Assert;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.dbmanager.BackupDbManager;
import py.infocenter.qos.ApplyMigrateRuleTest;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Instance;
import py.instance.InstanceDomain;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.io.qos.MigrationRuleStatus;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.ArchiveNotFoundExceptionThrift;
import py.thrift.share.ArchiveNotFreeToUseExceptionThrift;
import py.thrift.share.CreateStoragePoolRequestThrift;
import py.thrift.share.DatanodeNotFoundExceptionThrift;
import py.thrift.share.DeleteStoragePoolRequestThrift;
import py.thrift.share.DomainNotExistedExceptionThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.ListStoragePoolRequestThrift;
import py.thrift.share.ListStoragePoolResponseThrift;
import py.thrift.share.OneStoragePoolDisplayThrift;
import py.thrift.share.RemoveDatanodeFromDomainRequest;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.StatusThrift;
import py.thrift.share.StillHaveVolumeExceptionThrift;
import py.thrift.share.StoragePoolExistedExceptionThrift;
import py.thrift.share.StoragePoolNameExistedExceptionThrift;
import py.thrift.share.StoragePoolNotExistedExceptionThrift;
import py.thrift.share.StoragePoolStrategyThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.UpdateStoragePoolRequestThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;


public class ProcessStoragePoolTester extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ProcessStoragePoolTester.class);

  public final String testingStoragePoolName = "TestingStoragePool";
  public final long testingStoragePoolId = 1;
  public final long testingDomainId = 1;
  public final long testingArchiveId = 1;
  public final long testingVolumeId = 1;
  @Mock
  private StorageStore storageStore;
  @Mock
  private VolumeStore volumeStore;
  @Mock
  private DomainStore domainStore;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private BackupDbManager dbManager;

  @Mock
  private MigrationRuleStore migrationRuleStore;

  private InformationCenterImpl icImpl;

  @Mock
  private PySecurityManager securityManager;

  @Mock
  private InstanceStore instanceStore;
  @Mock
  private Instance instance;

  @Mock
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;

  @Mock
  private OperationStore operationStore;


  @Before
  public void init() throws Exception {
    super.init();
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    MigrationRuleInformation migrationRuleInformation = ApplyMigrateRuleTest
        .buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE);
    when(migrationRuleStore.get(anyLong())).thenReturn(migrationRuleInformation);

    icImpl = new InformationCenterImpl();
    icImpl.setStorageStore(storageStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setDomainStore(domainStore);
    icImpl.setAppContext(appContext);
    icImpl.setStoragePoolStore(storagePoolStore);
    icImpl.setBackupDbManager(dbManager);
    icImpl.setSegmentSize(1);
    icImpl.setMigrationRuleStore(migrationRuleStore);
    icImpl.setSecurityManager(securityManager);
    icImpl.setDataNodeClientFactory(dataNodeClientFactory);
    icImpl.setInstanceStore(instanceStore);
    icImpl.setOperationStore(operationStore);
  }

  @Test(expected = ServiceIsNotAvailableThrift.class)
  public void test_createStoragePool_serviceSuspend() throws Exception {
    CreateStoragePoolRequestThrift request = Mockito.mock(CreateStoragePoolRequestThrift.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_createStoragePool_invalidInput_storagePollNotSetted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    CreateStoragePoolRequestThrift request1 = new CreateStoragePoolRequestThrift();
    request1.setRequestId(RequestIdBuilder.get());
    request1.setStoragePool(null);

    icImpl.createStoragePool(request1);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_createStoragePool_invalidInput_domainNotSetted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    CreateStoragePoolRequestThrift request = new CreateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setPoolName(TestBase.getRandomString(6));
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    tstoragePool.setStatus(StatusThrift.Available);
    tstoragePool.setLastUpdateTime(System.currentTimeMillis());
    request.setStoragePool(tstoragePool);

    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_createStoragePool_invalidInput_poolIdNotSetted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    CreateStoragePoolRequestThrift request = new CreateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setDomainId(RequestIdBuilder.get());
    tstoragePool.setPoolName(TestBase.getRandomString(6));
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    tstoragePool.setStatus(StatusThrift.Available);
    tstoragePool.setLastUpdateTime(System.currentTimeMillis());
    request.setStoragePool(tstoragePool);

    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_createStoragePool_invalidInput_poolNameNotSetted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    CreateStoragePoolRequestThrift request = new CreateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setDomainId(RequestIdBuilder.get());
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    tstoragePool.setStatus(StatusThrift.Available);
    tstoragePool.setLastUpdateTime(System.currentTimeMillis());
    request.setStoragePool(tstoragePool);

    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test(expected = DomainNotExistedExceptionThrift.class)
  public void test_createStoragePool_domainNotExisted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test(expected = StoragePoolExistedExceptionThrift.class)
  public void test_createStoragePool_poolAlreadyExisted_byId() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    Domain domain = new Domain();
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(new StoragePool());

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test(expected = StoragePoolNameExistedExceptionThrift.class)
  public void test_createStoragePool_poolAlreadyExisted_byName_sameDomain() throws Exception {
    final long domainId = 1L;
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setDomainId(domainId);
    storagePoolForTesting.setName(testingStoragePoolName);
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    //now create a storagePool
    StoragePoolThrift tstoragePool = getTestingStoragePool();
    tstoragePool.setDomainId(domainId);
    CreateStoragePoolRequestThrift request = new CreateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    icImpl.createStoragePool(request);
    Assert.fail();
  }

  @Test
  public void test_createStoragePool_poolAlreadyExisted_byName_differentDomain() throws Exception {
    final long existPoolDomainId = 1L;
    final long toCreatePoolDomainId = 2L;

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setDomainId(existPoolDomainId);
    storagePoolForTesting.setName(testingStoragePoolName);
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    //now create a storagePool
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setDomainId(toCreatePoolDomainId);
    tstoragePool.setPoolName(testingStoragePoolName);
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    CreateStoragePoolRequestThrift request = new CreateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    try {
      icImpl.createStoragePool(request);
    } catch (Exception e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }

    Assert.assertEquals(1, storagePoolStore.listAllStoragePools().size());
  }

  @Test
  public void test_createStoragePool_archiveInDatanodeIsEmpty() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setName("NotExistedName");
    storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    request.getStoragePool().setVolumeIds(new HashSet<Long>());
    request.getStoragePool().setArchivesInDatanode(new HashMap<Long, Set<Long>>());
    try {
      icImpl.createStoragePool(request);
    } catch (TException e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }

    Assert.assertEquals(1, storagePoolStore.listAllStoragePools().size());
  }

  @Test
  public void test_createStoragePool_archiveNotFound_notInStorageStore() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setName("NotExistedName");
    storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    request.getStoragePool().setVolumeIds(new HashSet<Long>());
    when(storageStore.get(Mockito.anyLong())).thenReturn(null);
    try {
      icImpl.createStoragePool(request);
      Assert.fail();
    } catch (ArchiveNotFoundExceptionThrift e) {
      logger.error("", e);
    } catch (TException e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }
    Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
  }

  @Test
  public void test_createStoragePool_archiveNotFound_notInSpecifiedDomain() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    Domain domain = new Domain();
    domain.setDomainId(testingDomainId);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setName("NotExistedName");
    storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    request.getStoragePool().setVolumeIds(new HashSet<Long>());
    InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
    datanode.setDomainId(testingDomainId + 1);
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);
    try {
      icImpl.createStoragePool(request);
      Assert.fail();
    } catch (ArchiveNotFoundExceptionThrift e) {
      logger.error("", e);
    } catch (TException e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }

    Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
  }

  @Test
  public void test_createStoragePool_archiveNotFound_noSpecifiedArchive() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    Domain domain = new Domain();
    domain.setDomainId(testingDomainId);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setName("NotExistedName");
    storagePoolForTesting.setArchivesInDataNode(TestUtils.buildMultiMap(3, 4));
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    request.getStoragePool().setVolumeIds(new HashSet<Long>());
    InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
    datanode.setDomainId(testingDomainId);
    List<RawArchiveMetadata> archives = new ArrayList<>();
    RawArchiveMetadata archiveMetadata = new RawArchiveMetadata();
    archiveMetadata.setFree();
    archiveMetadata.setArchiveId(testingArchiveId);
    archives.add(archiveMetadata);
    datanode.setArchives(archives);
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);
    try {
      icImpl.createStoragePool(request);
      Assert.fail();
    } catch (ArchiveNotFoundExceptionThrift e) {
      logger.error("", e);
    } catch (TException e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }

    Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
  }

  @Test
  public void test_createStoragePool_archiveBeenFound_archiveNotFree() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    Domain domain = new Domain();
    domain.setDomainId(testingDomainId);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    List<StoragePool> allStoragePoolForTesting = new ArrayList<>();
    StoragePool storagePoolForTesting = RequestResponseHelper
        .buildStoragePoolFromThrift(getTestingStoragePool());
    storagePoolForTesting.setName("NotExistedName");

    Multimap<Long, Long> datanodeToArchiveMap = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    datanodeToArchiveMap.put(new Random().nextLong(), testingArchiveId);
    allStoragePoolForTesting.add(storagePoolForTesting);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePoolForTesting);

    CreateStoragePoolRequestThrift request = getCreatingRequest();
    request.getStoragePool().setVolumeIds(new HashSet<Long>());
    InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
    datanode.setDomainId(testingDomainId);
    List<RawArchiveMetadata> archives = new ArrayList<>();
    RawArchiveMetadata archiveMetadata = new RawArchiveMetadata();
    archiveMetadata.setArchiveId(testingArchiveId);
    archives.add(archiveMetadata);
    datanode.setArchives(archives);
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);
    try {
      icImpl.createStoragePool(request);
      Assert.fail();
    } catch (ArchiveNotFoundExceptionThrift e) {
      logger.error("", e);
    } catch (TException e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }

    Mockito.verify(storageStore, Mockito.atLeastOnce()).get(Mockito.anyLong());
  }

  @Test(expected = ServiceHavingBeenShutdownThrift.class)
  public void test_updateStoragePool_serviceHasBeenShutdown() throws Exception {
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    icImpl.shutdownForTest();
    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = ServiceIsNotAvailableThrift.class)
  public void test_updateStoragePool_serviceSuspended() throws Exception {
    UpdateStoragePoolRequestThrift request = Mockito.mock(UpdateStoragePoolRequestThrift.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_updateStoragePool_invalidInput_storagePoolNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(null);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_updateStoragePool_invalidInput_domainIdNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setPoolName(TestBase.getRandomString(6));
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    request.setStoragePool(tstoragePool);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_updateStoragePool_invalidInput_poolIdNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setDomainId(testingDomainId);
    // tstoragePool.setPoolName(TestBase.getRandomString(6));
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    request.setStoragePool(tstoragePool);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_updateStoragePool_invalidInput_poolNameNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setDomainId(testingDomainId);
    // tstoragePool.setPoolName(TestBase.getRandomString(6));
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    request.setStoragePool(tstoragePool);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_updateStoragePool_invalidInput_strategyNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setDomainId(testingDomainId);
    tstoragePool.setPoolName(TestBase.getRandomString(6));
    tstoragePool.setDescription(TestBase.getRandomString(15));
    // tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    request.setStoragePool(tstoragePool);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = DomainNotExistedExceptionThrift.class)
  public void test_updateStoragePool_domainNotExisted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

    UpdateStoragePoolRequestThrift request = getUpdatingRequest();
    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = StoragePoolNotExistedExceptionThrift.class)
  public void test_updateStoragePool_storagePoolNotExisted_byId() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);

    UpdateStoragePoolRequestThrift request = getUpdatingRequest();
    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = StoragePoolNameExistedExceptionThrift.class)
  public void test_updateStoragePool_storagePoolNameExisted_sameDomain() throws Exception {
    long domainId = 1L;

    StoragePoolThrift tstoragePool = getTestingStoragePool();
    tstoragePool.setDomainId(domainId);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    StoragePool storagePoolExist = new StoragePool();
    storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool = new StoragePool();
    storagePool.setDomainId(domainId);
    storagePool.setName(request.getStoragePool().getPoolName());
    allStoragePool.add(storagePool);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test
  public void test_updateStoragePool_storagePoolNameExisted_differentDomain() throws Exception {
    final long existPoolDomainId = 1L;
    long toUpdatePoolDomainId = 2L;

    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setDomainId(toUpdatePoolDomainId);
    tstoragePool.setPoolName(testingStoragePoolName);
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    StoragePool storagePoolExist = new StoragePool();
    storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool = new StoragePool();
    storagePool.setDomainId(existPoolDomainId);
    storagePool.setName(request.getStoragePool().getPoolName());
    allStoragePool.add(storagePool);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);

    try {
      icImpl.updateStoragePool(request);
    } catch (Exception e) {
      logger.error("Caught an unexpected exception", e);
      Assert.fail();
    }
  }

  @Test(expected = ArchiveNotFoundExceptionThrift.class)
  public void test_updateStoragePool_instanceNotExisted() throws Exception {
    long domainId = 1L;

    StoragePoolThrift tstoragePool = getTestingStoragePool();
    tstoragePool.setDomainId(domainId);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    StoragePool storagePoolExist = new StoragePool();
    storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePoolExist.setDomainId(domainId);
    storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool = new StoragePool();
    storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePool.setDomainId(domainId);
    allStoragePool.add(storagePool);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
    when(storageStore.get(Mockito.anyLong())).thenReturn(null);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = ArchiveNotFoundExceptionThrift.class)
  public void test_updateStoragePool_instanceNotInSpecifiedDomain() throws Exception {
    long domainId = 1L;

    StoragePoolThrift tstoragePool = getTestingStoragePool();
    tstoragePool.setDomainId(domainId);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    StoragePool storagePoolExist = new StoragePool();
    storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePoolExist.setDomainId(domainId);
    storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool = new StoragePool();
    storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePool.setDomainId(domainId);
    allStoragePool.add(storagePool);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
    InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
    datanode.setDomainId(testingDomainId);
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

    Domain domain = new Domain();
    domain.setDomainId(testingDomainId + 1);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = ArchiveNotFoundExceptionThrift.class)
  public void test_updateStoragePool_archiveNotFoundById() throws Exception {
    long domainId = 1L;

    StoragePoolThrift tstoragePool = getTestingStoragePool();
    tstoragePool.setDomainId(domainId);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    StoragePool storagePoolExist = new StoragePool();
    storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);
    storagePoolExist.setDomainId(domainId);

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool = new StoragePool();
    storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePool.setDomainId(domainId);
    allStoragePool.add(storagePool);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
    InstanceMetadata datanode = Mockito.mock(InstanceMetadata.class);

    datanode.setDomainId(testingDomainId);
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

    Domain domain = Mockito.mock(Domain.class);
    domain.setDomainId(testingDomainId + 1);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    when(datanode.getArchiveById(Mockito.anyLong())).thenReturn(null);
    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = ArchiveNotFreeToUseExceptionThrift.class)
  public void test_updateStoragePool_archiveNotFreeToUse() throws Exception {
    long domainId = 1L;

    StoragePoolThrift tstoragePool = getTestingStoragePool();
    tstoragePool.setDomainId(domainId);
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePool(tstoragePool);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    StoragePool storagePoolExist = new StoragePool();
    storagePoolExist.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePoolExist.setPoolId(request.getStoragePool().getPoolId() + 1);
    storagePoolExist.setDomainId(domainId);

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool = new StoragePool();
    storagePool.setName("differenc-prifix-" + request.getStoragePool().getPoolName());
    storagePool.setDomainId(domainId);
    allStoragePool.add(storagePool);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);
    when(storagePoolStore.listAllStoragePools()).thenReturn(allStoragePool);
    InstanceMetadata datanode = Mockito.mock(InstanceMetadata.class);

    datanode.setDomainId(testingDomainId);
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

    Domain domain = Mockito.mock(Domain.class);
    domain.setDomainId(testingDomainId + 1);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    RawArchiveMetadata archive = Mockito.mock(RawArchiveMetadata.class);
    archive.setFree();
    when(datanode.getArchiveById(Mockito.anyLong())).thenReturn(archive);
    icImpl.updateStoragePool(request);
    Assert.fail();
  }

  @Test(expected = ServiceHavingBeenShutdownThrift.class)
  public void test_deleteStoragePool_serviceBeenShutdown() throws Exception {
    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    icImpl.shutdownForTest();
    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test(expected = ServiceIsNotAvailableThrift.class)
  public void test_deleteStoragePool_serviceSuspended() throws Exception {
    DeleteStoragePoolRequestThrift request = Mockito.mock(DeleteStoragePoolRequestThrift.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_deleteStoragePool_invalidInput_storagePoolNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setDomainId(this.testingDomainId);

    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_deleteStoragePool_invalidInput_domainNotSet() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setStoragePoolId(this.testingStoragePoolId);

    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test(expected = DomainNotExistedExceptionThrift.class)
  public void test_deleteStoragePool_domainNotExisted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    request.setDomainId(this.testingDomainId);
    request.setStoragePoolId(this.testingStoragePoolId);
    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test(expected = StoragePoolNotExistedExceptionThrift.class)
  public void test_deleteStoragePool_storagePoolNotExisted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());

    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    request.setDomainId(this.testingDomainId);
    request.setStoragePoolId(this.testingStoragePoolId);

    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(null);
    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test(expected = StillHaveVolumeExceptionThrift.class)
  public void test_deleteStoragePool_storagePoolStillContainsVolume() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());

    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    request.setDomainId(this.testingDomainId);
    request.setStoragePoolId(this.testingStoragePoolId);

    StoragePool storagePoolExist = new StoragePool();
    Set<Long> volumeIds = new HashSet<>();
    volumeIds.add(this.testingVolumeId);
    storagePoolExist.setVolumeIds(volumeIds);
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);

    VolumeMetadata volume = new VolumeMetadata();
    volume.setVolumeStatus(VolumeStatus.Available);
    volume.setName("test");
    when(volumeStore.getVolume(Mockito.anyLong())).thenReturn(volume);
    icImpl.deleteStoragePool(request);
    Assert.fail();
  }

  @Test
  public void test_deleteStoragePool_noException() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());

    DeleteStoragePoolRequestThrift request = new DeleteStoragePoolRequestThrift();
    request.setDomainId(this.testingDomainId);
    request.setStoragePoolId(this.testingStoragePoolId);

    StoragePool storagePoolExist = new StoragePool();
    Set<Long> volumeIds = new HashSet<>();
    volumeIds.add(this.testingVolumeId);
    storagePoolExist.setVolumeIds(volumeIds);
    when(storagePoolStore.getStoragePool(Mockito.anyLong())).thenReturn(storagePoolExist);

    when(volumeStore.getVolume(Mockito.anyLong())).thenReturn(null);

    storagePoolExist.setDomainId(this.testingDomainId);
    storagePoolExist.setPoolId(this.testingStoragePoolId);
    storagePoolStore.saveStoragePool(storagePoolExist);
    icImpl.deleteStoragePool(request);
  }

  @Test(expected = ServiceHavingBeenShutdownThrift.class)
  public void test_listStoragePools_serviceBeeningShutdown() throws Exception {
    ListStoragePoolRequestThrift request = new ListStoragePoolRequestThrift();
    icImpl.shutdownForTest();
    icImpl.listStoragePools(request);
    Assert.fail();
  }

  @Test(expected = ServiceIsNotAvailableThrift.class)
  public void test_listStoragePools_serviceSuspend() throws Exception {
    ListStoragePoolRequestThrift request = Mockito.mock(ListStoragePoolRequestThrift.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    icImpl.listStoragePools(request);
    Assert.fail();
  }

  @Test()
  public void test_listStoragePools_invalidInput() throws Exception {
    ListStoragePoolRequestThrift request = Mockito.mock(ListStoragePoolRequestThrift.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    icImpl.listStoragePools(request);
  }

  @Test
  public void test_listStoragePools_withoutException() throws Exception {
    ListStoragePoolRequestThrift request = new ListStoragePoolRequestThrift();
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    request.setDomainId(this.testingDomainId);
    icImpl.listStoragePools(request);
  }

  @Test
  public void testListStoragePoolsWithMigrationInfo() throws Exception {
    ListStoragePoolRequestThrift request = new ListStoragePoolRequestThrift();
    List<Long> listStoragePoolIds = new ArrayList<>();
    listStoragePoolIds.add(1L);
    request.setStoragePoolIds(listStoragePoolIds);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    request.setDomainId(this.testingDomainId);
    List<StoragePool> storagePools = new ArrayList<>();
    StoragePool storagePool = new StoragePool(1L, 1L, "Pool", Capacity, "Pool");
    storagePools.add(storagePool);
    when(storagePoolStore.listStoragePools(any(ArrayList.class))).thenReturn(storagePools);

    // prepare archive
    RawArchiveMetadata archive1InDatanode1 = new RawArchiveMetadata();
    archive1InDatanode1.setArchiveId(1L);
    archive1InDatanode1.setArchiveType(ArchiveType.RAW_DISK);
    archive1InDatanode1.setStorageType(StorageType.SATA);
    archive1InDatanode1.setMigrationSpeed(10);
    archive1InDatanode1.addTotalPageToMigrate(100);
    archive1InDatanode1.addAlreadyMigratedPage(50);
    RawArchiveMetadata archive2InDatanode2 = new RawArchiveMetadata();
    archive2InDatanode2.setArchiveId(2L);
    archive2InDatanode2.setArchiveType(ArchiveType.RAW_DISK);
    archive2InDatanode2.setStorageType(StorageType.SATA);
    archive2InDatanode2.setMigrationSpeed(10);
    archive2InDatanode2.addTotalPageToMigrate(100);
    archive2InDatanode2.addAlreadyMigratedPage(50);

    storagePool.addArchiveInDatanode(1L, 1L);
    storagePool.addArchiveInDatanode(2L, 2L);
    InstanceMetadata datanode1 = new InstanceMetadata(new InstanceId(1L));
    datanode1.setDatanodeStatus(OK);
    datanode1.setEndpoint(new EndPoint("10.0.0.81", 8080).toString());
    datanode1.getArchives().add(archive1InDatanode1);
    InstanceMetadata datanode2 = new InstanceMetadata(new InstanceId(2L));
    datanode2.setDatanodeStatus(OK);
    datanode2.setEndpoint(new EndPoint("10.0.0.82", 8080).toString());
    datanode2.getArchives().add(archive2InDatanode2);
    when(storageStore.get(1L)).thenReturn(datanode1);
    when(storageStore.get(2L)).thenReturn(datanode2);
    ListStoragePoolResponseThrift listStoragePoolResponseThrift = icImpl.listStoragePools(request);
    assertEquals(1L, listStoragePoolResponseThrift.getStoragePoolDisplaysSize());
    OneStoragePoolDisplayThrift oneStoragePoolDisplayThrift = listStoragePoolResponseThrift
        .getStoragePoolDisplays().get(0);
    StoragePoolThrift storagePoolThrift = oneStoragePoolDisplayThrift.getStoragePoolThrift();
    assertEquals(20, storagePoolThrift.getMigrationSpeed());
    assertTrue(50 == storagePoolThrift.getMigrationRatio());

  }

  @Test(expected = ServiceHavingBeenShutdownThrift.class)
  public void test_removeArchiveFromStoragePool_serviceBeeningShutdown() throws Exception {
    RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
    icImpl.shutdownForTest();
    icImpl.removeDatanodeFromDomain(request);
    Assert.fail();
  }

  @Test(expected = ServiceIsNotAvailableThrift.class)
  public void test_removeArchiveFromStoragePool_serviceSuspend() throws Exception {
    RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    icImpl.removeDatanodeFromDomain(request);
    Assert.fail();
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void test_removeArchiveFromStoragePool_invalidInput() throws Exception {
    RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    icImpl.removeDatanodeFromDomain(request);
    Assert.fail();
  }

  @Test(expected = DomainNotExistedExceptionThrift.class)
  public void test_removeArchiveFromStoragePool_domainNotExisted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(null);

    RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
    request.setDomainId(this.testingDomainId);
    request.setDatanodeInstanceId(new Random().nextLong());
    icImpl.removeDatanodeFromDomain(request);
    Assert.fail();
  }

  @Test(expected = DatanodeNotFoundExceptionThrift.class)
  public void test_removeArchiveFromStoragePool_datanodeNotExisted() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(new Domain());
    when(storageStore.get(Mockito.anyLong())).thenReturn(null);

    RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
    request.setDomainId(this.testingDomainId);
    request.setDatanodeInstanceId(new Random().nextLong());
    icImpl.removeDatanodeFromDomain(request);
    Assert.fail();
  }

  @Test
  public void test_removeArchiveFromStoragePool_withoutException() throws Exception {
    Domain domain = new Domain();
    domain.setDomainId(this.testingDomainId);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(domainStore.getDomain(Mockito.anyLong())).thenReturn(domain);

    InstanceMetadata datanode = new InstanceMetadata(new InstanceId(new Random().nextLong()));
    datanode.setInstanceDomain(new InstanceDomain(testingDomainId));
    when(storageStore.get(Mockito.anyLong())).thenReturn(datanode);

    RemoveDatanodeFromDomainRequest request = new RemoveDatanodeFromDomainRequest();
    request.setDomainId(this.testingDomainId);
    request.setDatanodeInstanceId(new Random().nextLong());

    EndPoint endPoint = new EndPoint();
    when(instanceStore.get(any(InstanceId.class))).thenReturn(instance);
    when(instance.getEndPoint()).thenReturn(endPoint);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(dataNodeClient);

    icImpl.removeDatanodeFromDomain(request);
  }

  private CreateStoragePoolRequestThrift getCreatingRequest() {
    CreateStoragePoolRequestThrift request = new CreateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = getTestingStoragePool();
    request.setStoragePool(tstoragePool);

    return request;
  }

  private UpdateStoragePoolRequestThrift getUpdatingRequest() {
    UpdateStoragePoolRequestThrift request = new UpdateStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    StoragePoolThrift tstoragePool = getTestingStoragePool();
    request.setStoragePool(tstoragePool);

    return request;
  }

  private StoragePoolThrift getTestingStoragePool() {
    StoragePoolThrift tstoragePool = new StoragePoolThrift();
    tstoragePool.setDomainId(RequestIdBuilder.get());
    tstoragePool.setPoolId(RequestIdBuilder.get());
    tstoragePool.setPoolName(testingStoragePoolName);
    tstoragePool.setDescription(TestBase.getRandomString(15));
    tstoragePool.setStrategy(StoragePoolStrategyThrift.Capacity);
    tstoragePool.setArchivesInDatanode(TestUtils.buildDatanodeToArchiveMap(3, 4));
    tstoragePool.setVolumeIds(TestUtils.buildIdSet(3));
    tstoragePool.setStatus(StatusThrift.Available);
    tstoragePool.setLastUpdateTime(System.currentTimeMillis());
    logger.debug("TStoragePool is {}", tstoragePool);
    return tstoragePool;
  }
}