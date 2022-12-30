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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.test.TestUtils.generateArchiveMetadata;
import static py.test.TestUtils.generateVolumeMetadataWithSingleSegment;

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.share.FixVolumeRequestThrift;
import py.thrift.share.FixVolumeResponseThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * use for test FixVolumeTest method.
 */
public class FixVolumeTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(FixVolumeTest.class);
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
  private InformationCenterImpl icImpl;
  @Mock
  private PySecurityManager securityManager;

  @Mock
  private VolumeInformationManger volumeInformationManger;

  
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
    icImpl.setSecurityManager(securityManager);
    icImpl.setVolumeInformationManger(volumeInformationManger);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);

  }

  // all node and disk are good
  @Test
  public void testFixVolumeWithoutFix() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);

    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();

    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int value = new Long(i).intValue();
      Group group = new Group(value);
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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertEquals(0, lostDatanodesSize);

    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(volumeCompletely);

    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertFalse(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithAllDataNodeDied() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();
    volumeMetadatasList.add(volumeMetadata);

    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    List<Instance> instanceList = new ArrayList<>();
    for (Long i = 1L; i <= 3L; i++) {
      InstanceId instanceId = new InstanceId(i);
      final InstanceMetadata dataNodeMetadata = new InstanceMetadata(instanceId);
      int value = new Long(i).intValue();
      Group group = new Group(value);
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
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(null);
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 3);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithInstanceStatusError() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

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
      Instance datanodeInstance = new Instance(instanceId, group, "111", InstanceStatus.FAILED);
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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 3);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithArchiveName() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

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

      archiveMetadata.setDeviceName("Error Name");
      archiveMetadata.setStatus(ArchiveStatus.GOOD);
      archiveMetadataList.add(archiveMetadata);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 0);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithArchiveStatusError() throws TException, VolumeNotFoundException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

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

      if (i == 1L) {
        archiveMetadata.setStatus(ArchiveStatus.OFFLINED);
      } else if (i == 2L) {
        archiveMetadata.setStatus(ArchiveStatus.BROKEN);
      } else {
        archiveMetadata.setStatus(ArchiveStatus.OFFLINING);
      }

      archiveMetadataList.add(archiveMetadata);
      dataNodeMetadata.setArchives(archiveMetadataList);
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);

    for (Long datanodeId = 1L; datanodeId <= 3L; datanodeId++) {
      int listIndex = new Long(datanodeId).intValue() - 1;
      when(storageStore.get(datanodeId)).thenReturn(instanceMetadataList.get(listIndex));
      InstanceId instanceId = new InstanceId(datanodeId);
      when(instanceStore.get(instanceId)).thenReturn(instanceList.get(listIndex));
    }

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 0);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithSaliveatPss() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_SECONDARY2_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 2);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithPaliveatPss() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    when(storageStore.get(TEST_SECONDARY2_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
    InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 2);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithJaliveatPsj() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.REGULAR);
    assertTrue(volumeMetadata != null);

    volumeMetadata.setVolumeSize(1);
    volumeMetadata.setSegmentSize(1);
    final List<VolumeMetadata> volumeMetadatasList = new ArrayList<>();

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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    SegmentMembership memship = volumeMetadata.getMembership(0);
    InstanceId id = new InstanceId(3L);
    SegmentMembership newMemship = memship.removeSecondary(id);

    id = new InstanceId(3L);
    memship = newMemship.addJoiningSecondary(id);
    volumeMetadata.updateMembership(0, memship);
    volumeMetadatasList.add(volumeMetadata);

    SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
    for (InstanceId instanceId : memship.getMembers()) {
      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
      segmentUnitMetadata.setMembership(memship);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_JOINING_SECONDARY_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_JOINING_SECONDARY_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 2);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithAaliveatPsa() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertTrue(volumeMetadata != null);

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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
    InstanceId instanceId = new InstanceId(TEST_ARBITER_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(2));

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 2);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(!volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithPaliveatPsa() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertTrue(volumeMetadata != null);

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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(null);
    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(null);
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(instanceMetadataList.get(0));
    InstanceId instanceId = new InstanceId(TEST_PRIMARY_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(0));

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();
    assertTrue(lostDatanodesSize == 2);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertTrue(volumeCompletely);
    boolean bneedFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(bneedFixVolume);
  }

  @Test
  public void testFixVolumeWithOnlyPnotAliveAtPja() throws TException {
    FixVolumeRequestThrift fixVolumeRequest = new FixVolumeRequestThrift();
    fixVolumeRequest.setRequestId(RequestIdBuilder.get());

    //Set<Long> datanodes = buildIdSet(5);
    InstanceMetadata datanode = mock(InstanceMetadata.class);

    VolumeMetadata volumeMetadata = generateVolumeMetadataWithSingleSegment(VolumeType.SMALL);
    assertTrue(volumeMetadata != null);

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
      dataNodeMetadata.setDatanodeStatus(OK);
      instanceMetadataList.add(dataNodeMetadata);
      instanceList.add(datanodeInstance);
    }

    SegmentMembership membership = volumeMetadata.getMembership(0);
    InstanceId id = new InstanceId(2L);
    SegmentMembership newMembership = membership.removeSecondary(id);

    membership = newMembership.addJoiningSecondary(id);
    volumeMetadata.updateMembership(0, membership);
    volumeMetadatasList.add(volumeMetadata);

    SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(0);
    for (InstanceId instanceId : membership.getMembers()) {
      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
      segmentUnitMetadata.setMembership(membership);
    }

    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);
    try {
      when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);
    } catch (VolumeNotFoundException e) {
      logger.error("find a error:", e);
    }

    when(storageStore.get(TEST_ARBITER_ID)).thenReturn(instanceMetadataList.get(2));
    when(storageStore.get(TEST_SECONDARY1_ID)).thenReturn(instanceMetadataList.get(1));
    when(storageStore.get(TEST_PRIMARY_ID)).thenReturn(null);
    InstanceId instanceId = new InstanceId(TEST_SECONDARY1_ID);
    when(instanceStore.get(instanceId)).thenReturn(instanceList.get(1));
    InstanceId instanceId2 = new InstanceId(TEST_ARBITER_ID);
    when(instanceStore.get(instanceId2)).thenReturn(instanceList.get(2));

    fixVolumeRequest.setVolumeId(volumeMetadata.getVolumeId());
    fixVolumeRequest.setAccountId(volumeMetadata.getAccountId());

    FixVolumeResponseThrift fixVolumeRsp = icImpl.fixVolume(fixVolumeRequest);
    int lostDatanodesSize = fixVolumeRsp.getLostDatanodesSize();

    assertTrue(lostDatanodesSize == 1);
    assertEquals(fixVolumeRsp.getLostDatanodes().iterator().next(), TEST_PRIMARY_ID);
    boolean volumeCompletely = fixVolumeRsp.isFixVolumeCompletely();
    assertFalse(volumeCompletely);
    boolean needFixVolume = fixVolumeRsp.isNeedFixVolume();
    assertTrue(needFixVolume);
  }
}
