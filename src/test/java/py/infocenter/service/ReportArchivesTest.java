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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.RequestResponseHelper;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.icshare.DiskInfo;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.ServerNode;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.dbmanager.BackupDbManager;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.qos.ApplyMigrateRuleTest;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.test.utils.StorageMemStore;
import py.infocenter.worker.GetRebalanceTaskSweeper;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.io.qos.MigrationRuleStatus;
import py.storage.EdRootpathSingleton;
import py.test.TestBase;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.infocenter.service.ReportArchivesRequest;
import py.thrift.infocenter.service.ReportArchivesResponse;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.ArchiveStatusThrift;
import py.thrift.share.ArchiveTypeThrift;
import py.thrift.share.DatanodeStatusThrift;
import py.thrift.share.DatanodeTypeThrift;
import py.thrift.share.GroupThrift;
import py.thrift.share.InstanceDomainThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.NextActionInfoThrift;
import py.thrift.share.NextActionThrift;
import py.thrift.share.PageMigrationSpeedInfoThrift;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;
import py.thrift.share.StorageTypeThrift;

/**
 * A class includes some test for reporting archives.
 *
 */
public class ReportArchivesTest extends TestBase {

  private StorageMemStore storageMemStore = new StorageMemStore();

  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private ServerNodeStore serverNodeStore;
  @Mock
  private DomainStore domainStore;

  private InformationCenterImpl icImpl;
  private long segmentSize = 1L;
  private int groupCount = 5;

  @Mock
  private BackupDbManager dbManager;

  @Mock
  private MigrationRuleStore migrationRuleStore;

  @Mock
  private SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;

  @Mock
  private PySecurityManager securityManager;

  @Mock
  private InstanceStore instanceStore;
  @Mock
  private Instance instance;
  @Mock
  private GetRebalanceTaskSweeper getRebalanceTaskSweeper;

  @Mock
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;

  @Mock
  private OperationStore operationStore;

  @Mock
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;



  @Before
  public void init() throws Exception {
    super.init();
    InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    MigrationRuleInformation migrationRuleInformation = ApplyMigrateRuleTest
        .buildMigrateRuleInformation(1L,
            MigrationRuleStatus.AVAILABLE);

    when(migrationRuleStore.get(anyLong())).thenReturn(migrationRuleInformation);
    icImpl = new InformationCenterImpl();
    icImpl.setStorageStore(storageMemStore);
    icImpl.setGroupCount(groupCount);
    icImpl.setDomainStore(domainStore);
    icImpl.setStoragePoolStore(storagePoolStore);
    icImpl.setBackupDbManager(dbManager);
    icImpl.setAppContext(appContext);
    icImpl.setMigrationRuleStore(migrationRuleStore);
    icImpl.setServerNodeStore(serverNodeStore);
    icImpl.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
    icImpl.setSecurityManager(securityManager);
    icImpl.setDataNodeClientFactory(dataNodeClientFactory);
    icImpl.setInstanceStore(instanceStore);
    icImpl.setOperationStore(operationStore);
    icImpl.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    icImpl.setGetRebalanceTaskSweeper(getRebalanceTaskSweeper);

    AtomicLong version = new AtomicLong(1);
    when(instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion()).thenReturn(version);

    //alter event
    EdRootpathSingleton edRootpathSingleton = EdRootpathSingleton.getInstance();
    edRootpathSingleton.setRootPath("/tmp/testing");
  }

  @Test
  public void testSucceedToReportArchives() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    final ReportDbResponseThrift reportDbResponseThrift = mock(ReportDbResponseThrift.class);
    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.isDeleting()).thenReturn(false);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);
    when(dbManager.passedRecoveryTime()).thenReturn(true);
    when(dbManager.process(any(ReportDbRequestThrift.class))).thenReturn(reportDbResponseThrift);
    when(migrationRuleStore.get(anyLong())).thenReturn(null);

    for (long i = 0; i < 5; i++) {
      ReportArchivesRequest request = new ReportArchivesRequest();
      request.setRequestId(RequestIdBuilder.get());
      InstanceMetadataThrift instanceToRemote = new InstanceMetadataThrift();

      List<ArchiveMetadataThrift> archives = new ArrayList<>();
      for (long k = 0; k < 2; k++) {
        ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
        archive.setDevName("test");
        archive.setArchiveId(k);
        archive.setStatus(ArchiveStatusThrift.GOOD);
        archive.setStoragetype(StorageTypeThrift.SATA);
        archive.setType(ArchiveTypeThrift.RAW_DISK);
        archive.setCreatedBy("test");
        archive.setUpdatedBy("test");
        archive.setCreatedTime(123L);
        archive.setUpdatedTime(123L);
        archive.setSerialNumber("456");
        archive.setSlotNo("789");

        archive.setLogicalSpace(3 * segmentSize);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archives.add(archive);
      }
      instanceToRemote.setInstanceDomain(new InstanceDomainThrift(domainId));
      instanceToRemote.setArchiveMetadata(archives);
      instanceToRemote.setDatanodeStatus(DatanodeStatusThrift.OK);
      instanceToRemote.setDatanodeType(DatanodeTypeThrift.NORMAL);
      instanceToRemote.setEndpoint("10.0.2.79:1024");

      instanceToRemote.setCapacity(3 * segmentSize * 2);
      instanceToRemote.setFreeSpace(3 * segmentSize * 2);
      instanceToRemote.setLogicalCapacity(3 * segmentSize * 2);

      instanceToRemote.setInstanceId(i);
      request.setInstance(instanceToRemote);
      ReportDbRequestThrift reportDbRequestThrift = new ReportDbRequestThrift();
      request.setReportDbRequest(reportDbRequestThrift);

      when(segmentUnitsDistributionManager
          .selectRebalanceTasks(new InstanceId(instanceToRemote.getInstanceId())))
          .thenReturn(HashMultimap.create());

      ReportArchivesResponse response = icImpl.reportArchives(request);
      Assert.assertEquals(i, response.getGroup().getGroupId());
      NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
      assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
      for (Entry<Long, NextActionInfoThrift> entry : response.getArchiveIdMapNextAction()
          .entrySet()) {
        NextActionInfoThrift archiveNextAction = entry.getValue();
        assertTrue(archiveNextAction.getNextAction() == NextActionThrift.KEEP);
      }
      Map<Long, PageMigrationSpeedInfoThrift> archiveIdMapMigrationSpeed = response
          .getArchiveIdMapMigrationSpeed();
      assertEquals(2, archiveIdMapMigrationSpeed.size());
      PageMigrationSpeedInfoThrift archive1MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
      assertEquals(0, archive1MigrationSpeed.getMaxMigrationSpeed());
      PageMigrationSpeedInfoThrift archive2MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
      assertEquals(0, archive2MigrationSpeed.getMaxMigrationSpeed());

      InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
      logger.warn("-------------instance :{}", instance);
      assertEquals(instance.getCapacity(), 3 * segmentSize * 2);
      assertEquals(instance.getFreeSpace(), 3 * segmentSize * 2);
      assertEquals(instance.getLogicalCapacity(), 3 * segmentSize * 2);

      List<RawArchiveMetadata> rawArchiveMetadataList = instance.getArchives();
      for (RawArchiveMetadata rawArchiveMetadata : rawArchiveMetadataList) {
        assertEquals(rawArchiveMetadata.getSerialNumber(), "456");
        assertEquals(rawArchiveMetadata.getDeviceName(), "test");
        assertEquals(rawArchiveMetadata.getStatus(), ArchiveStatus.GOOD);
        assertEquals(rawArchiveMetadata.getStorageType(), StorageType.SATA);
        assertEquals(rawArchiveMetadata.getArchiveType(), ArchiveType.RAW_DISK);
        assertEquals(rawArchiveMetadata.getCreatedTime(), 123);
        assertEquals(rawArchiveMetadata.getUpdatedBy(), "test");
        assertEquals(rawArchiveMetadata.getLogicalSpace(), 3 * segmentSize);
        assertEquals(rawArchiveMetadata.getSlotNo(), "789");
        assertEquals(rawArchiveMetadata.getLogicalFreeSpace(), 3 * segmentSize);
      }
      instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
    }

    long[] capacityArray = {3L, 2L, 5L, 1L, 4L, 10L, 3L, 6L, 7L, 9L};
    int[] groupArray = {0, 1, 2, 3, 4, 3, 1, 0, 4, 1};
    for (int i = 0; i < 10; i++) {
      ReportArchivesRequest request = new ReportArchivesRequest();
      request.setRequestId(RequestIdBuilder.get());
      InstanceMetadataThrift instanceToRemote = new InstanceMetadataThrift();
      instanceToRemote.setArchiveMetadata(new ArrayList<>());
      instanceToRemote.setCapacity(capacityArray[i]);
      instanceToRemote.setInstanceId((long) i);
      instanceToRemote.setInstanceDomain(new InstanceDomainThrift());
      instanceToRemote.setDatanodeType(DatanodeTypeThrift.NORMAL);
      request.setInstance(instanceToRemote);
      ReportDbRequestThrift reportDbRequestThrift = new ReportDbRequestThrift();
      request.setReportDbRequest(reportDbRequestThrift);

      when(segmentUnitsDistributionManager
          .selectRebalanceTasks(new InstanceId(instanceToRemote.getInstanceId())))
          .thenReturn(HashMultimap.create());

      ReportArchivesResponse response = icImpl.reportArchives(request);
      Assert.assertEquals(groupArray[i], response.getGroup().getGroupId());
      assertEquals(0, response.getArchiveIdMapMigrationSpeedSize());

      InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
      instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
    }
  }

  @Test
  public void nullNewGroupAndNullOldGroup() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(null);
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (long k = 0; k < 2; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertNotNull(response.getGroup());
  }

  @Test
  public void nullNewGroupAndNotNullOldGroup() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(null);
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (long k = 0; k < 2; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    InstanceMetadata oldInstance = new InstanceMetadata(new InstanceId(0L));
    oldInstance.setGroup(new Group(0));

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    storageMemStore.save(oldInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
  }

  @Test
  public void notNullNewGroupAndNullOldGroup() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (long k = 0; k < 2; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());

    InstanceMetadata instanceFromStore = storageMemStore.get(newInstance.getInstanceId());
    Assert.assertEquals(0, instanceFromStore.getGroup().getGroupId());
  }

  @Test
  public void differentNotNullNewGroupAndNotNullOldGroup() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (long k = 0; k < 2; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    InstanceMetadata oldInstance = new InstanceMetadata(new InstanceId(0L));
    oldInstance.setGroup(new Group(1));

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    storageMemStore.save(oldInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
  }

  @Test
  public void sameNotNullNewGroupAndNotNullOldGroup() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (long k = 0; k < 2; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    InstanceMetadata oldInstance = new InstanceMetadata(new InstanceId(0L));
    oldInstance.setGroup(new Group(0));

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    storageMemStore.save(oldInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
  }

  /**
   * The case mocks datanodes reporting archives which has capacity 0. And we expect infocenter
   * would assign these archives to different groups but not a same one.
   */
  @Test
  public void reportArchivesWith0Capacity() throws Exception {

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    // create first instance with capacity 0
    InstanceMetadataThrift newInstance1 = new InstanceMetadataThrift();
    newInstance1.setInstanceId(0L);
    newInstance1.setCapacity(0L);
    newInstance1.setArchiveMetadata(new ArrayList<ArchiveMetadataThrift>());
    newInstance1.setInstanceDomain(new InstanceDomainThrift());
    newInstance1.setDatanodeType(DatanodeTypeThrift.NORMAL);

    // create second instance
    InstanceMetadataThrift newInstance2 = new InstanceMetadataThrift();
    newInstance2.setInstanceId(1L);
    newInstance2.setCapacity(0L);
    newInstance2.setArchiveMetadata(new ArrayList<ArchiveMetadataThrift>());
    newInstance2.setInstanceDomain(new InstanceDomainThrift());
    newInstance2.setDatanodeType(DatanodeTypeThrift.NORMAL);

    // report the first instance
    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance1);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance1.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);
    Assert.assertEquals(0, response.getGroup().getGroupId());

    // after the first report, the first instance already exist in storage store
    newInstance1.setGroup(new GroupThrift(0));

    // report the second instance
    request.setInstance(newInstance2);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance2.getInstanceId())))
        .thenReturn(HashMultimap.create());

    response = icImpl.reportArchives(request);
    // expect the second instance would not be assign to the next group from the group of first 
    // instance
    Assert.assertEquals(1, response.getGroup().getGroupId());
  }

  /**
   * As we know, datanodes run on different machines, they can report archives at the same time.
   * This case mock that and check if infocenter could handle this case.
   */
  @Test
  public void fiveDatanodeReportArchiveAtTheSameTime() throws Exception {
    // each instance belongs to different group, if the group is assigned, then set the group to 
    // true
    final boolean[] groupOccupied = new boolean[groupCount];
    final CountDownLatch mainLatch = new CountDownLatch(groupCount);
    final CountDownLatch threadLatch = new CountDownLatch(1);

    try {
      when(segmentUnitsDistributionManager.selectRebalanceTasks(any(InstanceId.class)))
          .thenThrow(new NoNeedToRebalance());
    } catch (NoNeedToRebalance noNeedToRebalance) {
      noNeedToRebalance.printStackTrace();
    }

    // datanode report archives at the same time
    for (int i = 0; i < groupCount; i++) {
      final long instanceId = i;
      groupOccupied[i] = false;

      Thread reportThread = new Thread() {
        @Override
        public void run() {

          final Long domainId = RequestIdBuilder.get();
          Long storagePoolId = RequestIdBuilder.get();
          Domain domain = mock(Domain.class);
          StoragePool storagePool = mock(StoragePool.class);

          // for domain
          Set<Long> storagePoolIdList = new HashSet<>();
          storagePoolIdList.add(storagePoolId);
          List<Domain> domains = new ArrayList<>();
          domains.add(domain);
          Set<Long> datanodeIds = new HashSet<>();

          // for storagePool
          List<StoragePool> storagePools = new ArrayList<>();
          storagePools.add(storagePool);
          Multimap<Long, Long> archivesInDataNode = Multimaps.synchronizedSetMultimap(HashMultimap
              .<Long, Long>create());
          for (long i = 0; i < 5; i++) {
            datanodeIds.add(i);
            for (long k = 0; k < 2; k++) {
              archivesInDataNode.put(i, k);
            }
          }
          try {
            when(domainStore.listAllDomains()).thenReturn(domains);
            when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
          } catch (Exception e) {
            logger.error("caught not list domain or storage pool exception", e);
          }
          when(domain.getStoragePools()).thenReturn(storagePoolIdList);
          when(domain.getDataNodes()).thenReturn(datanodeIds);
          when(domain.getDomainId()).thenReturn(domainId);
          when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
          when(storagePool.getPoolId()).thenReturn(storagePoolId);

          InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
          newInstance.setInstanceId(instanceId);
          newInstance.setCapacity(0L);
          newInstance.setArchiveMetadata(new ArrayList<ArchiveMetadataThrift>());
          newInstance.setInstanceDomain(new InstanceDomainThrift());
          newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);
          ReportArchivesRequest request = new ReportArchivesRequest();
          request.setRequestId(RequestIdBuilder.get());
          request.setInstance(newInstance);

          try {
            threadLatch.await();
            ReportArchivesResponse response = icImpl.reportArchives(request);
            groupOccupied[response.getGroup().getGroupId()] = true;
          } catch (Exception e) {
            logger.error("caught exception", e);
          } finally {
            mainLatch.countDown();
          }
        }
      };

      reportThread.start();
    }

    threadLatch.countDown();
    mainLatch.await();

    // check if all group is assigned
    for (int i = 0; i < groupCount; i++) {
      Assert.assertTrue(groupOccupied[i]);
    }
  }

  @Test
  public void testDataNodeKeepAction() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);

    // for domain
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    for (long i = 0; i < 2; i++) {
      datanodeIds.add(i);
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.isDeleting()).thenReturn(false);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
  }

  @Test
  public void testDataNodeNewAllcAction() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);

    // for domain
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    for (long i = 0; i < 2; i++) {
      datanodeIds.add(i);
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.isDeleting()).thenReturn(false);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(true);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    newInstance.setArchiveMetadata(archives);
    newInstance.setInstanceDomain(null);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.NEWALLOC);
  }

  @Test
  public void testDataNodeFreeSelfAction() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);

    // for domain
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    when(domainStore.listAllDomains()).thenReturn(domains);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.isDeleting()).thenReturn(false);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(true);
    when(dbManager.passedRecoveryTime()).thenReturn(true);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.FREEMYSELF);
  }

  @Test
  public void testDataNodeChangeAction() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);

    // for domain
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();
    for (long i = 0; i < 2; i++) {
      datanodeIds.add(i);
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId + 1);
    when(domain.isDeleting()).thenReturn(false);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(true);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    // diff with info center record
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.CHANGE);
  }

  @Test
  public void testArchiveKeepAction() throws Exception {
    int archiveNumber = 5;
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 1; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < archiveNumber; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);
    when(storagePool.isDeleting()).thenReturn(true);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (int k = 0; k < archiveNumber; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);

      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
    for (Entry<Long, NextActionInfoThrift> entry : response.getArchiveIdMapNextAction()
        .entrySet()) {
      NextActionInfoThrift archiveNextAction = entry.getValue();
      assertTrue(archiveNextAction.getNextAction() == NextActionThrift.KEEP);
    }
  }

  @Test
  public void testArchiveNewAllcAction() throws Exception {
    int archiveNumber = 5;
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 1; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < archiveNumber; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(true);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);
    when(storagePool.isDeleting()).thenReturn(false);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (int k = 0; k < archiveNumber; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      // archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
    for (Entry<Long, NextActionInfoThrift> entry : response.getArchiveIdMapNextAction()
        .entrySet()) {
      NextActionInfoThrift archiveNextAction = entry.getValue();
      assertTrue(archiveNextAction.getNextAction() == NextActionThrift.NEWALLOC);
    }
  }

  @Test
  public void testArchiveFreeSelfAction() throws Exception {
    int archiveNumber = 5;
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 1; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < archiveNumber; k++) {
        archivesInDataNode.put(i, k + 10);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(true);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);
    when(storagePool.isDeleting()).thenReturn(false);
    when(dbManager.passedRecoveryTime()).thenReturn(true);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (int k = 0; k < archiveNumber; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
    for (Entry<Long, NextActionInfoThrift> entry : response.getArchiveIdMapNextAction()
        .entrySet()) {
      NextActionInfoThrift archiveNextAction = entry.getValue();
      assertTrue(archiveNextAction.getNextAction() == NextActionThrift.FREEMYSELF);
    }
  }

  @Test
  public void testArchiveChangeAction() throws Exception {
    int archiveNumber = 5;
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 1; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < archiveNumber; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(true);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId + 10);
    when(storagePool.isDeleting()).thenReturn(false);

    InstanceMetadataThrift newInstance = new InstanceMetadataThrift();
    newInstance.setInstanceId(0L);
    newInstance.setGroup(new GroupThrift(0));
    newInstance.setCapacity(1L);

    List<ArchiveMetadataThrift> archives = new ArrayList<>();
    for (int k = 0; k < archiveNumber; k++) {
      ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
      archive.setArchiveId(k);
      archive.setStatus(ArchiveStatusThrift.GOOD);
      archive.setStoragetype(StorageTypeThrift.SATA);
      archive.setStoragePoolId(storagePoolId);
      archive.setLogicalFreeSpace(3 * segmentSize);
      archive.setType(ArchiveTypeThrift.RAW_DISK);
      archives.add(archive);
    }
    newInstance.setInstanceDomain(new InstanceDomainThrift(domainId));
    newInstance.setArchiveMetadata(archives);
    newInstance.setDatanodeType(DatanodeTypeThrift.NORMAL);

    ReportArchivesRequest request = new ReportArchivesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setInstance(newInstance);

    when(segmentUnitsDistributionManager
        .selectRebalanceTasks(new InstanceId(newInstance.getInstanceId())))
        .thenReturn(HashMultimap.create());

    ReportArchivesResponse response = icImpl.reportArchives(request);

    Assert.assertEquals(0, response.getGroup().getGroupId());
    NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
    assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
    for (Entry<Long, NextActionInfoThrift> entry : response.getArchiveIdMapNextAction()
        .entrySet()) {
      NextActionInfoThrift archiveNextAction = entry.getValue();
      assertTrue(archiveNextAction.getNextAction() == NextActionThrift.CHANGE);
    }
  }

  @Test
  public void testLongToDouble() {
    Long totalLogicSpace = 1024 * 1024 * 1024L;
    Long totalFreeSpace = 1000 * 1024 * 1024L;
    Double usedRatio = (((double) totalLogicSpace - totalFreeSpace) / totalLogicSpace);
    assertTrue(usedRatio != 0);
  }

  @Test
  public void testSetSlot2Archives() throws Exception {
    storageMemStore.clearMemoryData();

    final Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);

    final ReportDbResponseThrift reportDbResponseThrift = mock(ReportDbResponseThrift.class);
    // for domain
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    List<Domain> domains = new ArrayList<>();
    domains.add(domain);
    Set<Long> datanodeIds = new HashSet<>();

    // for storagePool
    List<StoragePool> storagePools = new ArrayList<>();
    storagePools.add(storagePool);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (long i = 0; i < 5; i++) {
      datanodeIds.add(i);
      for (long k = 0; k < 2; k++) {
        archivesInDataNode.put(i, k);
      }
    }
    when(domainStore.listAllDomains()).thenReturn(domains);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePools);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    when(domain.getDataNodes()).thenReturn(datanodeIds);
    when(domain.getDomainId()).thenReturn(domainId);
    when(domain.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.timePassedLongEnough(any(Long.class))).thenReturn(false);
    when(storagePool.isDeleting()).thenReturn(false);
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);
    when(dbManager.passedRecoveryTime()).thenReturn(true);
    when(dbManager.process(any(ReportDbRequestThrift.class))).thenReturn(reportDbResponseThrift);
    when(migrationRuleStore.get(anyLong())).thenReturn(null);

    ServerNode serverNode = new ServerNode();
    serverNode.setId("111");
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      DiskInfo diskInfo = new DiskInfo();
      char disk = (char) ((long) 'a' + i);
      diskInfo.setSn("/dev/sd" + disk);
      diskInfo.setSlotNumber(String.valueOf(i));
      diskInfo.setServerNode(serverNode);

      diskInfoSet.add(diskInfo);
    }
    serverNode.setDiskInfoSet(diskInfoSet);

    when(serverNodeStore.getServerNodeByIp(any())).thenReturn(serverNode);

    for (long i = 0; i < 5; i++) {
      ReportArchivesRequest request = new ReportArchivesRequest();
      request.setRequestId(RequestIdBuilder.get());
      InstanceMetadataThrift instanceToRemote = new InstanceMetadataThrift();

      List<ArchiveMetadataThrift> archives = new ArrayList<>();
      for (long k = 0; k < 2; k++) {
        ArchiveMetadataThrift archive = new ArchiveMetadataThrift();
        char disk = (char) ((long) 'a' + k);
        archive.setSerialNumber("/dev/sd" + disk);
        archive.setArchiveId(k);
        archive.setStatus(ArchiveStatusThrift.GOOD);
        archive.setStoragetype(StorageTypeThrift.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setLogicalSpace(10 * segmentSize);
        archive.setType(ArchiveTypeThrift.RAW_DISK);
        archives.add(archive);
      }
      instanceToRemote.setInstanceDomain(new InstanceDomainThrift(domainId));
      instanceToRemote.setArchiveMetadata(archives);
      instanceToRemote.setDatanodeStatus(DatanodeStatusThrift.OK);
      instanceToRemote.setEndpoint("10.0.2.79:1024");
      instanceToRemote.setDatanodeType(DatanodeTypeThrift.NORMAL);

      instanceToRemote.setCapacity(1L);
      instanceToRemote.setInstanceId(i);
      request.setInstance(instanceToRemote);
      ReportDbRequestThrift reportDbRequestThrift = new ReportDbRequestThrift();
      request.setReportDbRequest(reportDbRequestThrift);

      when(segmentUnitsDistributionManager
          .selectRebalanceTasks(new InstanceId(instanceToRemote.getInstanceId())))
          .thenReturn(HashMultimap.create());

      ReportArchivesResponse response = icImpl.reportArchives(request);
      Assert.assertEquals(i, response.getGroup().getGroupId());

      //verify slotNo
      InstanceMetadata insMetaDataRet = storageMemStore.get(instanceToRemote.getInstanceId());
      Assert.assertTrue(insMetaDataRet.getArchives().get(0).getSlotNo().equals("0"));
      Assert.assertTrue(insMetaDataRet.getArchives().get(1).getSlotNo().equals("1"));

      NextActionInfoThrift datandeNextActionInfo = response.getDatanodeNextAction();
      assertTrue(datandeNextActionInfo.getNextAction() == NextActionThrift.KEEP);
      for (Entry<Long, NextActionInfoThrift> entry : response.getArchiveIdMapNextAction()
          .entrySet()) {
        NextActionInfoThrift archiveNextAction = entry.getValue();
        assertTrue(archiveNextAction.getNextAction() == NextActionThrift.KEEP);
      }
      Map<Long, PageMigrationSpeedInfoThrift> archiveIdMapMigrationSpeed = response
          .getArchiveIdMapMigrationSpeed();
      assertEquals(2, archiveIdMapMigrationSpeed.size());
      PageMigrationSpeedInfoThrift archive1MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
      assertEquals(0, archive1MigrationSpeed.getMaxMigrationSpeed());
      PageMigrationSpeedInfoThrift archive2MigrationSpeed = archiveIdMapMigrationSpeed.get(0L);
      assertEquals(0, archive2MigrationSpeed.getMaxMigrationSpeed());

      InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
      instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
    }

    long[] capacityArray = {3L, 2L, 5L, 1L, 4L, 10L, 3L, 6L, 7L, 9L};
    int[] groupArray = {0, 1, 2, 3, 4, 3, 1, 0, 4, 1};
    for (int i = 0; i < 10; i++) {
      ReportArchivesRequest request = new ReportArchivesRequest();
      request.setRequestId(RequestIdBuilder.get());
      InstanceMetadataThrift instanceToRemote = new InstanceMetadataThrift();
      instanceToRemote.setArchiveMetadata(new ArrayList<>());
      instanceToRemote.setCapacity(capacityArray[i]);
      instanceToRemote.setInstanceId((long) i);
      instanceToRemote.setInstanceDomain(new InstanceDomainThrift());
      instanceToRemote.setDatanodeType(DatanodeTypeThrift.NORMAL);
      request.setInstance(instanceToRemote);
      ReportDbRequestThrift reportDbRequestThrift = new ReportDbRequestThrift();
      request.setReportDbRequest(reportDbRequestThrift);

      when(segmentUnitsDistributionManager
          .selectRebalanceTasks(new InstanceId(instanceToRemote.getInstanceId())))
          .thenReturn(HashMultimap.create());

      ReportArchivesResponse response = icImpl.reportArchives(request);
      Assert.assertEquals(groupArray[i], response.getGroup().getGroupId());
      assertEquals(0, response.getArchiveIdMapMigrationSpeedSize());

      InstanceMetadata instance = RequestResponseHelper.buildInstanceFrom(instanceToRemote);
      instance.setGroup(RequestResponseHelper.buildGroupFrom(response.getGroup()));
    }
  }

}
