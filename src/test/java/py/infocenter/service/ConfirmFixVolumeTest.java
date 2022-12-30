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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.test.TestUtils.generateArchiveMetadata;
import static py.test.TestUtils.generateVolumeMetadataWithSingleSegment;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.share.ConfirmFixVolumeRequestThrift;
import py.thrift.share.ConfirmFixVolumeResponse;
import py.thrift.share.CreateSegmentUnitInfo;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.LackDatanodeExceptionThrift;
import py.thrift.share.SegIdThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitRoleThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

public class ConfirmFixVolumeTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ConfirmFixVolumeTest.class);

  private static final Long TEST_PRIMARY_ID = 1L;
  private static final Long TEST_SECONDARY1_ID = 2L;
  private static final Long TEST_SECONDARY2_ID = 3L;
  private static final Long TEST_JOINING_SECONDARY_ID = 3L;
  private static final Long TEST_ARBITER_ID = 3L;

  @Mock
  private StorageStore storageStore;

  @Mock
  private InstanceStore instanceStore;
  @Mock
  private VolumeStore volumeStore;
  @Mock
  private DomainStore domainStore;
  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;

  private InformationCenterImpl icImpl;

  @Mock
  private PySecurityManager securityManager;

  @Before
  public void init() throws Exception {
    super.init();
    InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    icImpl = new InformationCenterImpl();
    icImpl.setStorageStore(storageStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setDomainStore(domainStore);
    icImpl.setInstanceStore(instanceStore);
    icImpl.setStoragePoolStore(storagePoolStore);
    icImpl.setAppContext(appContext);
    icImpl.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);
    icImpl.setSegmentWrappCount(10);
    icImpl.setSecurityManager(securityManager);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  // datanode in request < datanode is died in volume
  @Ignore
  @Test
  public void testConfirmFixVolumeWithNotAllDataNode() throws TException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();
    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadataList.add(archiveMetadata);
      dataNodeMetadata.setArchives(archiveMetadataList);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
      when(storageStore.get(datanodeId)).thenReturn(null);
    }

    Set<Long> lostDataNode = new HashSet<>();
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);

    boolean caughtLackNodeException = false;
    try {
      icImpl.confirmFixVolume(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertTrue(caughtLackNodeException);
  }

  //all ArchiveDied , do not need datanode in request
  @Ignore
  @Test
  public void testConfirmFixVolumeWithAllArchiveDied()
      throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.OFFLINED);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(Long.valueOf(i), 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    for (Long datanodeId = 1L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    boolean caughtLackNodeException = false;
    try {
      icImpl.confirmFixVolume(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

  }

  // datanode in request = datanode is died in volume
  @Ignore
  @Test
  public void testConfirmFixVolumeWithAllDataNode() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<Long>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    for (Long datanodeId = 1L; datanodeId <= 2L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(null);
    }
    for (Long datanodeId = 3L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(1L);
    lostDataNode.add(2L);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    try {
      icImpl.confirmFixVolume(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithPaliveatPss() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
    InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_SECONDARY2_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> joinsendaryInstanceId = entry1.getValue().getJoiningSecondaries();
          if (!joinsendaryInstanceId.contains(entry1.getKey().getInstanceId())) {
            fail();
          }
          assertEquals(entry1.getValue().getPrimary(), 1L);
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(),
            SegmentUnitRoleThrift.JoiningSecondary);
      }

    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithSaliveatPss() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_SECONDARY2_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> joiningSecondaries = entry1.getValue().getJoiningSecondaries();
          Long primaryId = entry1.getValue().getPrimary();
          if (!joiningSecondaries.contains(entry1.getKey().getInstanceId())) {
            fail();
          }
          assertEquals(primaryId, TEST_SECONDARY2_ID);
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(),
            SegmentUnitRoleThrift.JoiningSecondary);
      }

    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithJaliveatPss() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    SegmentMembership memship = volumeMetadata.getMembership(0);
    InstanceId id = new InstanceId(TEST_SECONDARY2_ID);
    SegmentMembership newMemship = memship.removeSecondary(id);

    id = new InstanceId(TEST_JOINING_SECONDARY_ID);
    memship = newMemship.addJoiningSecondary(id);
    volumeMetadata.updateMembership(0, memship);

    SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
    for (InstanceId instanceId : memship.getMembers()) {
      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
      segmentUnitMetadata.setMembership(memship);
    }

    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<Long>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_JOINING_SECONDARY_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_JOINING_SECONDARY_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> joinsendaryInstanceId = entry1.getValue().getJoiningSecondaries();
          if (!joinsendaryInstanceId.contains(TEST_JOINING_SECONDARY_ID)) {
            fail();
          }
          assertEquals(entry1.getValue().getPrimary(), entry1.getKey().getInstanceId());
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRoleThrift.Primary);
      }

    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithNoaliveatPss() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<Long>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(Long.valueOf(i), Long.valueOf(0));
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(null);

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_SECONDARY2_ID);
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 2);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 1);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> secondaryId = entry1.getValue().getSecondaries();
          if (entry1.getValue().getPrimary() == entry1.getKey().getInstanceId()) {
            assertEquals(entry1.getValue().getEpoch(), 2);
            assertEquals(entry1.getValue().getGeneration(), 0);
            assertEquals(createSegmentUnitInfo.getSegmentUnitRole(),
                SegmentUnitRoleThrift.Primary);
          } else if (secondaryId.contains(entry1.getKey().getInstanceId())) {
            assertEquals(entry1.getValue().getEpoch(), 2);
            assertEquals(entry1.getValue().getGeneration(), 0);
            assertEquals(createSegmentUnitInfo.getSegmentUnitRole(),
                SegmentUnitRoleThrift.Secondary);
          } else {
            fail();
          }
        }
      }
    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithPaliveatPsa() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(Long.valueOf(i), Long.valueOf(0));
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
    InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_ARBITER_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> arbiterId = entry1.getValue().getArbiters();
          if (!arbiterId.contains(entry1.getKey().getInstanceId())) {
            fail();
          }
          assertEquals(entry1.getValue().getPrimary(), 1L);
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRoleThrift.Arbiter);
      }

    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithSaliveatPsa() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(instanceMetadataList.get(1));
    InstanceId instanceId = new InstanceId(TEST_SECONDARY1_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(1));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_ARBITER_ID);
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> arbiterId = entry1.getValue().getArbiters();
          Long primaryId = entry1.getValue().getPrimary();
          if (!arbiterId.contains(entry1.getKey().getInstanceId())) {
            fail();
          }
          assertEquals(primaryId, TEST_SECONDARY1_ID);
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRoleThrift.Arbiter);
      }
    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithAaliveatPsa() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<Long>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);

    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_ARBITER_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> arbiterId = entry1.getValue().getArbiters();
          if (!arbiterId.contains(TEST_ARBITER_ID)) {
            fail();
          }
          assertEquals(entry1.getValue().getPrimary(), entry1.getKey().getInstanceId());
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRoleThrift.Primary);
      }

    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithNoaliveatPsa() throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<Long>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_ARBITER_ID);
    lostDataNode.add(TEST_SECONDARY1_ID);
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 2);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 1);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Set<Long> arbiterId = entry1.getValue().getArbiters();
          if (entry1.getValue().getPrimary() == entry1.getKey().getInstanceId()) {
            assertEquals(entry1.getValue().getEpoch(), 2);
            assertEquals(entry1.getValue().getGeneration(), 0);
            assertEquals(createSegmentUnitInfo.getSegmentUnitRole(),
                SegmentUnitRoleThrift.Primary);
          } else if (arbiterId.contains(entry1.getKey().getInstanceId())) {
            assertEquals(entry1.getValue().getEpoch(), 2);
            assertEquals(entry1.getValue().getGeneration(), 0);
            assertEquals(createSegmentUnitInfo.getSegmentUnitRole(),
                SegmentUnitRoleThrift.Arbiter);
          } else {
            fail();
          }
        }
      }
    }
  }

  @Ignore
  @Test
  public void testConfirmFixVolumeWithNotPaliveatPja()
      throws TException, IOException, SQLException {
    ConfirmFixVolumeRequestThrift confirmFixVolumeRequest = new ConfirmFixVolumeRequestThrift();
    confirmFixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertNotNull(volumeMetadata);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    final List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    Set<Long> storagePoolIdList = new HashSet<Long>();
    storagePoolIdList.add(volumeMetadata.getStoragePoolId());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(volumeMetadata.getStoragePoolId());
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId)
          .setDiskName("Good Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadataList.add(archiveMetadata);
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    for (Long i = 4L; i <= 5L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int groupId = new Long(i).intValue();
      Group group = new Group(groupId);
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.HEALTHY);
      assertNotNull(datanodeInstance);

      //volumeMetadata.getSegmentByIndex(0).getSegmentUnitMetadata(instanceId).setDiskName("Good 
      // Name");
      final List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      RawArchiveMetadata archiveMetadata = generateArchiveMetadata(1, 1);
      assertNotNull(archiveMetadata);

      archiveMetadata.setDeviceName("Good Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadata.setStoragePoolId(volumeMetadata.getStoragePoolId());
      archiveMetadata.setLogicalFreeSpace(1L);
      archiveMetadata.setStorageType(StorageType.SATA);
      archiveMetadataList.add(archiveMetadata);

      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDomainId(volumeMetadata.getDomainId());
      dataNodeMetadata.setGroup(group);
      dataNodeMetadata.setDatanodeStatus(OK);
      dataNodeMetadata.setDatanodeType(NORMAL);
      archivesInDataNode.put(i, 0L);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    SegmentMembership memship = volumeMetadata.getMembership(0);
    InstanceId id = new InstanceId(2L);
    SegmentMembership newMemship = memship.removeSecondary(id);

    memship = newMemship.addJoiningSecondary(id);
    volumeMetadata.updateMembership(0, memship);
    volumeMetadatasList.add(volumeMetadata);

    SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
    for (InstanceId instanceId : memship.getMembers()) {
      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
      segmentUnitMetadata.setMembership(memship);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);

    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(instanceMetadataList.get(1));
    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_SECONDARY1_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(1));

    instanceId = new InstanceId(TEST_ARBITER_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    for (Long datanodeId = 4L; datanodeId <= 5L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    when(storageStore.list()).thenReturn(instanceMetadataList);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.getStoragePools()).thenReturn(storagePoolIdList);
    confirmFixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    confirmFixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    Set<Long> lostDataNode = new HashSet<>();
    lostDataNode.add(TEST_PRIMARY_ID);
    confirmFixVolumeRequest.setLostDatanodes(lostDataNode);
    boolean caughtLackNodeException = false;
    ConfirmFixVolumeResponse fixVolumeRsp = null;
    try {
      fixVolumeRsp = icImpl.confirmFixVolume_tmp(confirmFixVolumeRequest);
    } catch (LackDatanodeExceptionThrift e) {
      caughtLackNodeException = true;
    }
    assertFalse(caughtLackNodeException);

    assertNotNull(fixVolumeRsp);
    assertNotNull(fixVolumeRsp.getCreateSegmentUnits());
    Map<SegIdThrift, List<CreateSegmentUnitInfo>> createSegmentUnitList = fixVolumeRsp
        .getCreateSegmentUnits();
    assertEquals(createSegmentUnitList.size(), 1);
    for (Map.Entry<SegIdThrift, List<CreateSegmentUnitInfo>> entry : createSegmentUnitList
        .entrySet()) {
      List<CreateSegmentUnitInfo> createSegmentUnitInfoList = entry.getValue();
      assertEquals(createSegmentUnitInfoList.size(), 1);
      for (CreateSegmentUnitInfo createSegmentUnitInfo : createSegmentUnitInfoList) {
        assertEquals(createSegmentUnitInfo.getSegmentMembershipMap().size(), 2);
        for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry1 :
            createSegmentUnitInfo
                .getSegmentMembershipMap().entrySet()) {
          Long primaryId = entry1.getValue().getPrimary();
          if (primaryId.equals(TEST_PRIMARY_ID)) {
            fail();
          }
          assertEquals(entry1.getValue().getEpoch(), 2);
          assertEquals(entry1.getValue().getGeneration(), 0);
        }
        assertEquals(createSegmentUnitInfo.getSegmentUnitRole(), SegmentUnitRoleThrift.Primary);
      }
    }
  }
}
