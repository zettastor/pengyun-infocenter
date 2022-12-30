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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeInAction.NULL;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.common.RequestIdBuilder;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.EndPoint;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.ReserveSegUnitResult;
import py.infocenter.rebalance.ReserveSegUnitsInfo;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.test.utils.TestBeans;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.storage.EdRootpathSingleton;
import py.test.TestBase;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.NotEnoughGroupExceptionThrift;
import py.thrift.share.NotEnoughNormalGroupExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * A class includes some test for reserving segment units from information center.
 *
 */
public class ReserveSegUnitsTest extends TestBase {

  @Mock
  private StorageStore storageStore;

  private VolumeStore volumeStore;
  @Mock
  private DomainStore domainStore;
  @Mock
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;

  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private InstanceStore instanceStore;

  private SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;

  private long segmentSize = 1L;

  private InformationCenterImpl icImpl;

  private VolumeInformationManger volumeInformationManger;

  @Mock
  private InfoCenterAppContext appContext;

  private Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = new HashMap<>();



  @Before
  public void init() throws Exception {
    super.init();
    ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);
    volumeStore = (VolumeStore) ctx.getBean("twoLevelVolumeStore");

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(appContext.getInstanceId()).thenReturn(new InstanceId(111));
    when(instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap())
        .thenReturn(instanceToVolumeInfoMap);

    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setAppContext(appContext);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    segmentUnitsDistributionManager = new SegmentUnitsDistributionManagerImpl(segmentSize,
        volumeStore,
        storageStore, storagePoolStore, null, domainStore);
    segmentUnitsDistributionManager.setSegmentWrappCount(10);
    segmentUnitsDistributionManager.setVolumeInformationManger(volumeInformationManger);
    segmentUnitsDistributionManager.setInstanceStore(instanceStore);

    //mock Instance
    Instance datanodeInstance = new Instance(new InstanceId("1"), "test",
        InstanceStatus.HEALTHY, new EndPoint("10.0.0.80", 8021));
    datanodeInstance.setNetSubHealth(false);

    when(instanceStore.get(any(InstanceId.class))).thenReturn(datanodeInstance);
    when(instanceStore.getByHostNameAndServiceName(any(String.class), any(String.class)))
        .thenReturn(datanodeInstance);

    icImpl = new InformationCenterImpl();
    icImpl.setStorageStore(storageStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setDomainStore(domainStore);
    icImpl.setStoragePoolStore(storagePoolStore);
    icImpl.setAppContext(appContext);
    icImpl.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
    icImpl.setSegmentWrappCount(10);
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);

    //alter event
    EdRootpathSingleton edRootpathSingleton = EdRootpathSingleton.getInstance();
    edRootpathSingleton.setRootPath("/tmp/testing");
  }

  @Test
  public void testSucceedToReserveSegUnitsWithThreeInstance() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    Long volumeSize = 3L;
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    List<InstanceMetadata> instanceList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 1; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(volumeSize * segmentSize + 1);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put((long) i, (long) k);
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);

      instanceList.add(instanceMetadata);
    }
    // TestUtils.generateVolumeMetadata()
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storageStore.list()).thenReturn(instanceList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    VolumeMetadata volumeMetadata = createavolume(3, VolumeType.SMALL,
        reserveSegUnitsInfo.getVolumeId(), storagePoolId, domainId);

    ReserveSegUnitResult reserveSegUnitResult = null;
    try {
      reserveSegUnitResult = segmentUnitsDistributionManager.reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
      Assert.assertEquals(1, instancesFromRemote.size());
      Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
    } catch (NotEnoughSpaceExceptionThrift e) {
      logger.warn("caught a exception,", e);
      ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();
      for (SegmentMetadata segmentMetadata : volumeMetadata.getSegmentTable().values()) {
        SegmentMembership membership = segmentMetadata.getLatestMembership();
        primaryCounter.increment(membership.getPrimary().getId());
      }
      SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
          .getSegmentUnitMetadata(new InstanceId(2));
      boolean isRegular = segmentUnitMetadata.getMembership().isPrimary(new InstanceId(2))
          || primaryCounter.get(2L) == volumeSize;
      if (!isRegular) {
        logger.warn("fact primary:{}, expired primary:{}",
            segmentUnitMetadata.getMembership().getPrimary(), 2);
      }

      assert (isRegular);
    }
  }

  @Ignore
  @Test
  public void testFailedToReserveSegUnitsWithThreeInstance() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    List<InstanceMetadata> instanceList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);
      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 1; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        // zero freeSize of last datanode that can be chosen for reserve segmentUnit
        archive.setWeight(1);
        if (i != 2) {
          archive.setLogicalFreeSpace(3 * segmentSize);
          archive.setWeight(2);
        } else {
          archive.setLogicalFreeSpace(3 * segmentSize);
        }
        archives.add(archive);
        archivesInDataNode.put((long) i, (long) k);
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);

      instanceList.add(instanceMetadata);
    }
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storageStore.list()).thenReturn(instanceList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    createavolume(3, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId, domainId);

    boolean caughtException = false;
    try {
      segmentUnitsDistributionManager.reserveSegUnits(reserveSegUnitsInfo);
    } catch (NotEnoughSpaceExceptionThrift e) {
      caughtException = true;
    }

    Assert.assertTrue(caughtException);
  }

  @Test
  public void testSucceedToReserveSegUnitsWithFourInstanceWithDomain() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 4; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        if (i == 2) {
          continue;
        }
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    VolumeMetadata volumeMetadata = createavolume(3, VolumeType.SMALL,
        reserveSegUnitsInfo.getVolumeId(), storagePoolId, domainId);

    ReserveSegUnitResult reserveSegUnitResult = null;
    try {
      reserveSegUnitResult = segmentUnitsDistributionManager.reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
      Assert.assertEquals(1, instancesFromRemote.size());
      Assert.assertEquals(3L, instancesFromRemote.get(0).getInstanceId());
    } catch (NotEnoughSpaceExceptionThrift e) {
      logger.warn("caught a exception,", e);
      SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
          .getSegmentUnitMetadata(new InstanceId(3));
      assert (segmentUnitMetadata.getMembership().isPrimary(new InstanceId(3)));
    }
  }

  @Test
  public void testCreateVolumeRequest() {
    CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
    Assert.assertTrue(createVolumeRequest.getDomainId() == 0);
  }

  @Test
  public void testReserveSegUnitsSecondaryPsa4Datanode3Segment() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 4; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(2, instancesFromRemote.size());
    Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsSecondaryPsa5Datanode3Segment() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    //excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(3, instancesFromRemote.size());
    assert ((4L == instancesFromRemote.get(0).getInstanceId()) || (1L == instancesFromRemote.get(0)
        .getInstanceId()));
  }

  @Test
  public void testReserveSegUnitsSecondaryPsa5Datanode3SegmentDiffWeight() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(2L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 1; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(5 * segmentSize);
        if (i == 1) {
          archive.setWeight(3);
        } else if (i == 4) {
          archive.setWeight(4);
        } else {
          archive.setWeight(2);
        }
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(3, instancesFromRemote.size());
    Assert.assertEquals(4L, instancesFromRemote.get(0).getInstanceId());
    Assert.assertEquals(1L, instancesFromRemote.get(1).getInstanceId());
    Assert.assertEquals(3L, instancesFromRemote.get(2).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsSecondaryPsa5Datanode3Segment1DatanodeNoArchive()
      throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        if (i == 4) {
          archive.setLogicalFreeSpace(0);
        }
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(2, instancesFromRemote.size());
    Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsArbiterPsa4Datanode3Segment() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Arbiter);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 4; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archives.add(archive);
        archive.setWeight(1);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(2, instancesFromRemote.size());
    Assert.assertEquals(3L, instancesFromRemote.get(0).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsArbiterPsa5Datanode3Segment() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Arbiter);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(2, instancesFromRemote.size());
    Assert.assertEquals(4L, instancesFromRemote.get(0).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsArbiterPsa5Datanode3Segment1Simple() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Arbiter);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);
      if (i == 2) {
        instanceMetadata.setDatanodeType(SIMPLE);
      }

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.SMALL, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(2, instancesFromRemote.size());
    Assert.assertEquals(2L, instancesFromRemote.get(0).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsSecondaryPss4Datanode3Segment() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(2L);
    excludedInstanceIds.add(3L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 1; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 2;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      segment.put(SegmentUnitTypeThrift.Arbiter, new ArrayList<>());

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      normalInstanceList.add(instanceList.get(2).getInstanceId());
      normalInstanceList.add(instanceList.get(3).getInstanceId());
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.REGULAR, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(1, instancesFromRemote.size());
    Assert.assertEquals(4L, instancesFromRemote.get(0).getInstanceId());
  }

  @Test
  public void testReserveSegUnitsSecondaryPss4Datanode3SegmentDiffWeight() throws Exception {
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(0L);
    excludedInstanceIds.add(1L);

    final ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, RequestIdBuilder.get(), 0, SegmentUnitType.Normal);

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId(i));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        if (i == 2) {
          archive.setWeight(2);
        }
        archives.add(archive);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 10000;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      segment.put(SegmentUnitTypeThrift.Arbiter, new ArrayList<>());

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      normalInstanceList.add(instanceList.get(2).getInstanceId());
      normalInstanceList.add(instanceList.get(3).getInstanceId());
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    createavolume(volumeSize, VolumeType.REGULAR, reserveSegUnitsInfo.getVolumeId(), storagePoolId,
        domainId, segmentIndex2SegmentMap);

    ReserveSegUnitResult reserveSegUnitResult = segmentUnitsDistributionManager
        .reserveSegUnits(reserveSegUnitsInfo);

    List<InstanceMetadataThrift> instancesFromRemote = reserveSegUnitResult.getInstances();
    Assert.assertEquals(3, instancesFromRemote.size());

    Assert.assertEquals(4L, instancesFromRemote.get(0).getInstanceId());
    Assert.assertEquals(2L, instancesFromRemote.get(1).getInstanceId());
    Assert.assertEquals(3L, instancesFromRemote.get(2).getInstanceId());
  }

  private void createavolume(long size, VolumeType volumeType, long volumeId, long poolId,
      long domainId,
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap) {
    ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId,
        size, segmentSize, volumeType, domainId, poolId);
    volumeMetadata.setName("testVolume");
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> indexMapEntry :
        segmentIndex2SegmentMap
            .entrySet()) {
      int segIndex = indexMapEntry.getKey();
      final SegId segId = new SegId(volumeMetadata.getVolumeId(), segIndex);
      Map<SegmentUnitTypeThrift, List<InstanceId>> map = indexMapEntry.getValue();
      List<InstanceId> arbiterUnits = map.get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceId> segUnits = map.get(SegmentUnitTypeThrift.Normal);

      // build membership
      SegmentMembership membership;
      InstanceId primary = new InstanceId(segUnits.get(0));
      List<InstanceId> secondaries = new ArrayList<>();
      for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
        secondaries.add(new InstanceId(segUnits.get(i)));
      }
      List<InstanceId> arbiters = new ArrayList<>();
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiters.add(new InstanceId(arbiterUnits.get(i)));
      }
      if (arbiters.isEmpty()) {
        membership = new SegmentMembership(primary, secondaries);
      } else {
        membership = new SegmentMembership(primary, secondaries, arbiters);
      }

      // build segment meta data
      SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

      // build segment units
      for (InstanceId arbiter : membership.getArbiters()) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter);
        InstanceId instanceId = new InstanceId(arbiter);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
      normalUnits.add(primary);
      for (InstanceId normalUnit : normalUnits) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
            volumeType, SegmentUnitType.Normal);
        InstanceId instanceId = new InstanceId(normalUnit);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
        segmentUnitMetadata.setArchiveId(archiveId);
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      volumeMetadata.addSegmentMetadata(segment, membership);
    }
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setInAction(NULL);
    volumeInformationManger.saveVolume(volumeMetadata);
  }

  private VolumeMetadata createavolume(long size, VolumeType volumeType, long volumeId, long poolId,
      long domainId)
      throws NotEnoughGroupExceptionThrift,
      NotEnoughSpaceExceptionThrift, NotEnoughNormalGroupExceptionThrift, TException {
    ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> result =
        segmentUnitsDistributionManager
            .reserveVolume(size, volumeType, false, Integer.MAX_VALUE, poolId);
    logger.warn("reserve result {}", result);
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId,
        size, segmentSize, volumeType, domainId, poolId);
    volumeMetadata.setName("testVolume");
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);

    for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        indexMapEntry : result.entrySet()) {
      int segIndex = indexMapEntry.getKey();
      final SegId segId = new SegId(volumeMetadata.getVolumeId(), segIndex);
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> map = indexMapEntry
          .getValue();
      List<InstanceIdAndEndPointThrift> arbiterUnits = map.get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceIdAndEndPointThrift> segUnits = map.get(SegmentUnitTypeThrift.Normal);

      // build membership
      SegmentMembership membership;
      InstanceId primary = new InstanceId(segUnits.get(0).getInstanceId());
      List<InstanceId> secondaries = new ArrayList<>();
      for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
        secondaries.add(new InstanceId(segUnits.get(i).getInstanceId()));
      }
      List<InstanceId> arbiters = new ArrayList<>();
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiters.add(new InstanceId(arbiterUnits.get(i).getInstanceId()));
      }
      if (arbiters.isEmpty()) {
        membership = new SegmentMembership(primary, secondaries);
      } else {
        membership = new SegmentMembership(primary, secondaries, arbiters);
      }

      // build segment meta data
      SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

      // build segment units
      for (InstanceId arbiter : membership.getArbiters()) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter);
        InstanceId instanceId = new InstanceId(arbiter);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
      normalUnits.add(primary);
      for (InstanceId normalUnit : normalUnits) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
            volumeType, SegmentUnitType.Normal);
        InstanceId instanceId = new InstanceId(normalUnit);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        instance.getArchives().get(diskIndex).setLogicalFreeSpace(
            instance.getArchives().get(diskIndex).getLogicalFreeSpace() - segmentSize);
        long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
        segmentUnitMetadata.setArchiveId(archiveId);
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      volumeMetadata.addSegmentMetadata(segment, membership);
    }
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setInAction(NULL);
    volumeInformationManger.saveVolume(volumeMetadata);
    return volumeMetadata;
  }

}
