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

package py.infocenter.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.RequestResponseHelper.buildReadWriteTypeFrom;
import static py.RequestResponseHelper.buildSegmentMembershipFrom;
import static py.RequestResponseHelper.buildVolumeInActionFrom;
import static py.RequestResponseHelper.buildVolumeSourceTypeFrom;
import static py.RequestResponseHelper.convertFromSegmentUnitTypeThrift;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.junit.Before;
import org.junit.Test;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.common.RequestIdBuilder;
import py.driver.DriverStatus;
import py.icshare.OperationStatus;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.job.UpdateOperationProcessorImpl;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentMetadataThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitStatusThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class UpdateOperationProcessorImplTest extends TestBase {

  @Before
  public void init() throws Exception {
    super.init(); // ensure the programm complie have no error
    InfoCenterConstants.segmentUnitReportTimeout = 90;
  }

  /**
   * Test is that calculate segment.
   */
  @Test
  public void testCalculateOkSegments() {
    int segmentCount = 10;
    int okSegmentCount = 4;
    VolumeMetadata volumeMetadataTest = buildVolume(segmentCount, okSegmentCount);
    volumeMetadataTest.setVolumeType(VolumeType.REGULAR);
    int okSegmentFromCalculate = UpdateOperationProcessorImpl
        .calculateOkSegment(volumeMetadataTest);
    assertEquals(okSegmentFromCalculate, okSegmentCount);
  }

  /**
   * Test is that simulate deletion of the segment being deleted.
   */
  @Test
  public void testInDeleteSegments() {
    int segmentCount = 10;
    int inDeleteSegmentCount = 4;
    List<SegmentMetadataThrift> segmentMetadataList = buildDeleteSegmentMetadataList(segmentCount,
        inDeleteSegmentCount);
    VolumeMetadataThrift volumeMetadata = buildVolumeMetadataThrift(segmentMetadataList);

    VolumeMetadata volumeMetadataTest = buildVolumeFrom(volumeMetadata);
    int inDeleteSegmentCountFromCalculate = UpdateOperationProcessorImpl
        .calculateInDeleteSegment(volumeMetadataTest);
    assertEquals(inDeleteSegmentCountFromCalculate, inDeleteSegmentCount);
  }

  /**
   * Test is that the status of the simulated volume is changed from Created to Operational.
   */
  @Test
  public void testCreateVolumeStatusToOperationStatus() {
    OperationStatus status = null;
    status = UpdateOperationProcessorImpl
        .createVolumeStatusToOperationStatus(VolumeStatus.Creating);
    assertTrue(status == OperationStatus.ACTIVITING);
    status = UpdateOperationProcessorImpl.createVolumeStatusToOperationStatus(VolumeStatus.Dead);
    assertTrue(status == OperationStatus.FAILED);
    status = UpdateOperationProcessorImpl
        .createVolumeStatusToOperationStatus(VolumeStatus.Available);
    assertTrue(status == OperationStatus.SUCCESS);
  }

  /**
   * Test is that launch a driver and operation driver status. And check driver status.
   */
  @Test
  public void testLaunchDriverToOperationStatus() {
    OperationStatus status = null;
    status = UpdateOperationProcessorImpl.launchDriverToOperationStatus(DriverStatus.LAUNCHED);
    assertTrue(status == OperationStatus.SUCCESS);
    status = UpdateOperationProcessorImpl.launchDriverToOperationStatus(DriverStatus.LAUNCHING);
    assertTrue(status == OperationStatus.ACTIVITING);
    status = UpdateOperationProcessorImpl.launchDriverToOperationStatus(DriverStatus.UNAVAILABLE);
    assertTrue(status == OperationStatus.FAILED);
  }

  /**
   * Test is that umount a driver and operation driver status. And check driver status.
   */
  @Test
  public void testUmountDriverToOperationStatus() {
    OperationStatus status = null;
    status = UpdateOperationProcessorImpl.umountDriverToOperationStatus(DriverStatus.REMOVING);
    assertTrue(status == OperationStatus.SUCCESS);
    status = UpdateOperationProcessorImpl.umountDriverToOperationStatus(DriverStatus.LAUNCHING);
    assertTrue(status == OperationStatus.ACTIVITING);
    status = UpdateOperationProcessorImpl.umountDriverToOperationStatus(DriverStatus.UNAVAILABLE);
    assertTrue(status == OperationStatus.ACTIVITING);
    status = UpdateOperationProcessorImpl.umountDriverToOperationStatus(DriverStatus.UNKNOWN);
    assertTrue(status == OperationStatus.FAILED);
  }


  /**
   * Build basic segment metadata thrift for testing.
   *
   * @param segmentCount   the number of segment
   * @param okSegmentCount the number of ok segment
   *
   */
  public List<SegmentMetadataThrift> buildCreateSegmentMetadataList(int segmentCount,
      int okSegmentCount) {
    if (segmentCount < okSegmentCount) {
      return null;
    }
    int segmentId = 0;
    List<SegmentMetadataThrift> segmentMetadataList = new ArrayList<>();
    for (int i = 0; i < okSegmentCount; i++) {
      SegmentMetadataThrift segmentMetadata = buildOkSegmentMetadata(segmentId++);
      segmentMetadataList.add(segmentMetadata);
    }
    for (int i = 0; i < segmentCount - okSegmentCount; i++) {
      SegmentMetadataThrift segmentMetadata = buildClockSegmentMetadata(segmentId++);
      segmentMetadataList.add(segmentMetadata);
    }
    return segmentMetadataList;
  }


  /**
   * Build a volume metadata information for testing.
   *
   * @param segmentCount   the number of segment
   * @param okSegmentCount the number of ok segment
   */
  public VolumeMetadata buildVolume(int segmentCount, int okSegmentCount) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    long volumeId = 123466L;
    if (segmentCount < okSegmentCount) {
      return null;
    }
    int segmentId = 0;
    for (int i = 0; i < okSegmentCount; i++) {
      buildOkSegmentMetadataNew(volumeMetadata, volumeId, segmentId++);
    }
    for (int i = 0; i < segmentCount - okSegmentCount; i++) {
      buildClockSegmentMetadataNew(volumeMetadata, volumeId, segmentId++);
    }
    return volumeMetadata;
  }


  /**
   * Build segment metadata that init volume basic information .
   *
   * @param volumeMetadata volume metadata
   * @param volumeId       volume id
   * @param segIndex       segment index
   */
  public void buildOkSegmentMetadataNew(VolumeMetadata volumeMetadata, long volumeId,
      int segIndex) {
    SegId segId = new SegId(volumeId, segIndex);

    // build membership
    SegmentMembership membership;
    InstanceId primary = new InstanceId(RequestIdBuilder.get());
    List<InstanceId> secondaries = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      secondaries.add(new InstanceId(RequestIdBuilder.get()));
    }

    membership = new SegmentMembership(primary, secondaries);

    // build segment meta data
    SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

    Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
    normalUnits.add(primary);

    for (InstanceId normalUnit : normalUnits) {
      SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
          normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
          VolumeType.DEFAULT_VOLUME_TYPE, SegmentUnitType.Normal);
      InstanceId instanceId = new InstanceId(normalUnit);
      segmentUnitMetadata.setInstanceId(instanceId);
      segmentUnitMetadata.setLastReported(System.currentTimeMillis());
      segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
    }

    volumeMetadata.addSegmentMetadata(segment, membership);
  }


  /**
   * Build clock segment metadata information for testing.
   *
   * @param volumeMetadata volume metadata
   * @param volumeId       volume id
   * @param segIndex       segment index
   */
  public void buildClockSegmentMetadataNew(VolumeMetadata volumeMetadata, long volumeId,
      int segIndex) {

    SegId segId = new SegId(volumeId, segIndex);

    // build membership
    SegmentMembership membership;
    InstanceId primary = new InstanceId(RequestIdBuilder.get());

    List<InstanceId> secondaries = new ArrayList<>();
    List<InstanceId> inactiveSecondaries = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      inactiveSecondaries.add(new InstanceId(RequestIdBuilder.get()));
    }

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, null,
        inactiveSecondaries, null);

    // build segment meta data
    SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

    Set<InstanceId> normalUnits = new HashSet<>();
    normalUnits.add(primary);

    //just have Primary,so not ok

    volumeMetadata.addSegmentMetadata(segment, membership);
  }


  /**
   * Build need delete segment metadata information.
   *
   * @param segmentCount         the number of segment
   * @param inDeleteSegmentCount the number of in delete segment
   *
   */
  public List<SegmentMetadataThrift> buildDeleteSegmentMetadataList(int segmentCount,
      int inDeleteSegmentCount) {
    if (segmentCount < inDeleteSegmentCount) {
      return null;
    }

    int segIndex = 0;
    List<SegmentMetadataThrift> segmentMetadataList = new ArrayList<>();
    for (int i = 0; i < inDeleteSegmentCount; i++) {
      SegmentMetadataThrift segmentMetadata = buildInDeleteSegmentMetadata(segIndex++);
      segmentMetadataList.add(segmentMetadata);
    }
    for (int i = 0; i < segmentCount - inDeleteSegmentCount; i++) {
      SegmentMetadataThrift segmentMetadata = buildClockDeleteSegmentMetadata(segIndex++);
      segmentMetadataList.add(segmentMetadata);
    }
    return segmentMetadataList;
  }

  // build an OK segment
  // one segment-unit-metadata instancedid = primary and status is primay : P OK
  // the other one instancedid != primary and status is secondary : S OK


  /**
   * Build segment metadata information that the status is ok.
   *
   * @param segmentId segment id
   *
   */
  public SegmentMetadataThrift buildOkSegmentMetadata(int segmentId) {
    SegmentMetadataThrift segmentMetadata = new SegmentMetadataThrift();
    segmentMetadata.setSegId(segmentId);
    segmentMetadata.setIndexInVolume(segmentId);
    final List<SegmentUnitMetadataThrift> segmentUnitsList = new ArrayList<>();
    SegmentUnitMetadataThrift segmentUnit1 = new SegmentUnitMetadataThrift();
    final SegmentUnitMetadataThrift segmentUnit2 = new SegmentUnitMetadataThrift();
    Long instanceId1 = RequestIdBuilder.get();
    segmentUnit1.setInstanceId(instanceId1);
    SegmentMembershipThrift membershipThrift1 = new SegmentMembershipThrift();
    membershipThrift1.setPrimary(instanceId1);
    segmentUnit1.setMembership(membershipThrift1);
    segmentUnit1.setStatus(SegmentUnitStatusThrift.Primary);
    Long instanceId2 = RequestIdBuilder.get();
    segmentUnit2.setInstanceId(instanceId2);
    segmentUnit2.setMembership(membershipThrift1);
    segmentUnit2.setStatus(SegmentUnitStatusThrift.Secondary);
    segmentUnitsList.add(segmentUnit1);
    segmentUnitsList.add(segmentUnit2);
    segmentMetadata.setSegmentUnits(segmentUnitsList);
    return segmentMetadata;
  }

  // build an InDelete segment
  // one segment-unit-metadata instancedid = primary and status is deleting or deleted : P in-delete
  // the other one instancedid != primary and status is secondary : S in-delete

  /**
   * Build segment metadata information that the status is in delete.
   *
   * @param segIndex segment index
   */
  public SegmentMetadataThrift buildInDeleteSegmentMetadata(int segIndex) {
    SegmentMetadataThrift segmentMetadata = new SegmentMetadataThrift();
    segmentMetadata.setSegId(segIndex);
    segmentMetadata.setIndexInVolume(segIndex);

    final List<SegmentUnitMetadataThrift> segmentUnitsList = new ArrayList<>();
    SegmentUnitMetadataThrift segmentUnit1 = new SegmentUnitMetadataThrift();
    final SegmentUnitMetadataThrift segmentUnit2 = new SegmentUnitMetadataThrift();
    Long instanceId1 = RequestIdBuilder.get();
    segmentUnit1.setInstanceId(instanceId1);
    SegmentMembershipThrift membershipThrift1 = new SegmentMembershipThrift();
    membershipThrift1.setPrimary(instanceId1);
    segmentUnit1.setMembership(membershipThrift1);
    segmentUnit1.setStatus(SegmentUnitStatusThrift.Deleted);
    Long instanceId2 = RequestIdBuilder.get();
    segmentUnit2.setInstanceId(instanceId2);
    segmentUnit2.setMembership(membershipThrift1);
    segmentUnit2.setStatus(SegmentUnitStatusThrift.Deleting);
    segmentUnitsList.add(segmentUnit1);
    segmentUnitsList.add(segmentUnit2);
    segmentMetadata.setSegmentUnits(segmentUnitsList);
    return segmentMetadata;
  }

  /**
   * Build segment metadata that segment roll back done.
   *
   * @param snapshotVersion snapshot version
   * @param index           index
   *
   */
  public SegmentMetadata buildRollBackDoneSegmentMetadata(int snapshotVersion, int index) {
    final SegmentMetadata segmentMetadata = new SegmentMetadata(null, index);
    final List<SegmentUnitMetadata> segmentUnitsList = new ArrayList<>();
    long volumeId = RequestIdBuilder.get();
    SegId segIdSegmentUnit1 = new SegId(volumeId, index);
    SegId segIdSegmentUnit2 = new SegId(volumeId, index);
    SegmentUnitMetadata segmentUnit1 = new SegmentUnitMetadata(segIdSegmentUnit1, 0L);
    SegmentUnitMetadata segmentUnit2 = new SegmentUnitMetadata(segIdSegmentUnit2, 0L);
    segmentUnitsList.add(segmentUnit1);
    segmentUnitsList.add(segmentUnit2);
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataTable = new ConcurrentHashMap();
    segmentUnitMetadataTable.put(new InstanceId(RequestIdBuilder.get()), segmentUnit1);
    segmentUnitMetadataTable.put(new InstanceId(RequestIdBuilder.get()), segmentUnit2);
    segmentMetadata.setSegmentUnitMetadataTable(segmentUnitMetadataTable);
    return segmentMetadata;
  }

  /**
   * Build clock segment metadata information for testing that according to segment id .
   *
   * @param segmentId segment id
   *
   */
  public SegmentMetadataThrift buildClockSegmentMetadata(int segmentId) {
    SegmentMetadataThrift segmentMetadata = new SegmentMetadataThrift();
    segmentMetadata.setSegId(segmentId);
    final List<SegmentUnitMetadataThrift> segmentUnitsList = new ArrayList<>();
    SegmentUnitMetadataThrift segmentUnit1 = new SegmentUnitMetadataThrift();
    final SegmentUnitMetadataThrift segmentUnit2 = new SegmentUnitMetadataThrift();
    Long instanceId1 = RequestIdBuilder.get();
    segmentUnit1.setInstanceId(instanceId1);
    SegmentMembershipThrift membershipThrift1 = new SegmentMembershipThrift();
    membershipThrift1.setPrimary(instanceId1);
    segmentUnit1.setMembership(membershipThrift1);
    segmentUnit1.setStatus(SegmentUnitStatusThrift.Secondary);
    Long instanceId2 = RequestIdBuilder.get();
    segmentUnit2.setInstanceId(instanceId2);
    segmentUnit2.setMembership(membershipThrift1);
    segmentUnit2.setStatus(SegmentUnitStatusThrift.Primary);
    segmentUnitsList.add(segmentUnit1);
    segmentUnitsList.add(segmentUnit2);
    segmentMetadata.setSegmentUnits(segmentUnitsList);
    return segmentMetadata;
  }

  /**
   * Build clock delete segment metadata.
   *
   * @param segIndex segment index
   *
   */
  public SegmentMetadataThrift buildClockDeleteSegmentMetadata(int segIndex) {
    SegmentMetadataThrift segmentMetadata = new SegmentMetadataThrift();
    segmentMetadata.setSegId(segIndex);
    segmentMetadata.setIndexInVolume(segIndex);

    final List<SegmentUnitMetadataThrift> segmentUnitsList = new ArrayList<>();
    SegmentUnitMetadataThrift segmentUnit1 = new SegmentUnitMetadataThrift();
    final SegmentUnitMetadataThrift segmentUnit2 = new SegmentUnitMetadataThrift();
    Long instanceId1 = RequestIdBuilder.get();
    segmentUnit1.setInstanceId(instanceId1);
    SegmentMembershipThrift membershipThrift1 = new SegmentMembershipThrift();
    membershipThrift1.setPrimary(instanceId1);
    segmentUnit1.setMembership(membershipThrift1);
    segmentUnit1.setStatus(SegmentUnitStatusThrift.Primary);
    Long instanceId2 = RequestIdBuilder.get();
    segmentUnit2.setInstanceId(instanceId2);
    segmentUnit2.setMembership(membershipThrift1);
    segmentUnit2.setStatus(SegmentUnitStatusThrift.Deleting);
    segmentUnitsList.add(segmentUnit1);
    segmentUnitsList.add(segmentUnit2);
    segmentMetadata.setSegmentUnits(segmentUnitsList);
    return segmentMetadata;
  }


  /**
   * Build basic volume  metadata information for testing.
   *
   * @param segmentMetadataList the list of segment metadata info
   *
   */
  public VolumeMetadataThrift buildVolumeMetadataThrift(
      List<SegmentMetadataThrift> segmentMetadataList) {
    VolumeMetadataThrift volumeMetadata = new VolumeMetadataThrift();
    volumeMetadata.setSegmentsMetadata(segmentMetadataList);
    return volumeMetadata;
  }


  /**
   * Build basic volume  metadata information for testing.
   */
  public VolumeMetadata buildVolumeMetadata(List<SegmentMetadata> segmentMetadataList) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    for (SegmentMetadata segmentMetadata : segmentMetadataList) {
      SegmentMembership segmentMemership = buildSegmentMembership();
      volumeMetadata.addSegmentMetadata(segmentMetadata, segmentMemership);
    }
    return volumeMetadata;
  }


  /**
   * Build segment membership information for testing.
   */
  public SegmentMembership buildSegmentMembership() {
    int epoch = 0;
    int generation = 0;
    InstanceId primary = new InstanceId(RequestIdBuilder.get());
    InstanceId secondary = new InstanceId(RequestIdBuilder.get());
    Collection<InstanceId> secondaries = new ArrayList<>();
    secondaries.add(secondary);
    SegmentMembership segmentMembership = new SegmentMembership(
        new SegmentVersion(epoch, generation), primary,
        secondaries);
    return segmentMembership;
  }


  /**
   * Build basic volume metadata information for testing and create response return.
   */
  public GetVolumeResponse buildGetVolumeResponse(VolumeMetadataThrift volumeMetadata) {
    GetVolumeResponse response = new GetVolumeResponse();
    response.setVolumeMetadata(volumeMetadata);
    return response;
  }


  /**
   * Build basic volume  metadata information for testing.
   */
  public VolumeMetadata buildVolumeFrom(VolumeMetadataThrift volumeMetadataThrift) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(volumeMetadataThrift.getVolumeId());
    volumeMetadata.setName(volumeMetadataThrift.getName());
    volumeMetadata.setVolumeSize(volumeMetadataThrift.getVolumeSize());
    volumeMetadata.setSegmentSize(volumeMetadataThrift.getSegmentSize());

    volumeMetadata.setAccountId(volumeMetadataThrift.getAccountId());
    volumeMetadata.setRootVolumeId(volumeMetadataThrift.getRootVolumeId());
    volumeMetadata.setExtendingSize(volumeMetadataThrift.getExtendingSize());
    volumeMetadata.setDeadTime(volumeMetadataThrift.getDeadTime());
    volumeMetadata
        .setSegmentNumToCreateEachTime(volumeMetadataThrift.getLeastSegmentUnitAtBeginning());
    volumeMetadata
        .setSegmentNumToCreateEachTime(volumeMetadataThrift.getSegmentNumToCreateEachTime());
    volumeMetadata.setVolumeLayout(volumeMetadataThrift.getVolumeLayout());
    volumeMetadata.setEnableLaunchMultiDrivers(volumeMetadataThrift.isEnableLaunchMultiDrivers());

    if (volumeMetadataThrift.isSetDomainId()) {
      volumeMetadata.setDomainId(volumeMetadataThrift.getDomainId());
    }

    if (volumeMetadataThrift.isSetStoragePoolId()) {
      volumeMetadata.setStoragePoolId(volumeMetadataThrift.getStoragePoolId());
    }

    volumeMetadata.setVolumeCreatedTime(new Date(volumeMetadataThrift.getVolumeCreatedTime()));
    if (volumeMetadataThrift.getLastExtendedTime() == 0) {
      volumeMetadata.setLastExtendedTime(null);
    } else {
      volumeMetadata.setLastExtendedTime(new Date(volumeMetadataThrift.getLastExtendedTime()));
    }
    volumeMetadata
        .setVolumeSource(buildVolumeSourceTypeFrom(volumeMetadataThrift.getVolumeSource()));
    volumeMetadata.setReadWrite(buildReadWriteTypeFrom(volumeMetadataThrift.getReadWrite()));
    volumeMetadata.setInAction(buildVolumeInActionFrom(volumeMetadataThrift.getInAction()));

    volumeMetadata.setPageWrappCount(volumeMetadataThrift.getPageWrappCount());
    volumeMetadata.setSegmentWrappCount(volumeMetadataThrift.getSegmentWrappCount());

    // get segment meta data from the volume
    List<SegmentMetadataThrift> segmentMetadataThrifts = volumeMetadataThrift
        .getSegmentsMetadata();
    for (SegmentMetadataThrift segmentMetadataThrift : segmentMetadataThrifts) {
      // SegId segId = new SegId(volumeMetadata.getVolumeId(), segmentMetadataThrift.getSegIndex());
      SegmentMetadata segmentMetadata = new SegmentMetadata(
          new SegId(segmentMetadataThrift.getVolumeId(), segmentMetadataThrift.getSegId()),
          segmentMetadataThrift.getIndexInVolume());
      SegmentMembership highestMembershipInSegment = null;
      segmentMetadata.setFreeRatio(segmentMetadataThrift.getFreeSpaceRatio());
      for (SegmentUnitMetadataThrift segUnitMetaThrift : segmentMetadataThrift.getSegmentUnits()) {
        SegmentUnitMetadata segmentUnit = buildSegmentUnitMetadataFrom(segUnitMetaThrift);
        segmentMetadata
            .putSegmentUnitMetadata(new InstanceId(segUnitMetaThrift.getInstanceId()), segmentUnit);
        SegmentMembership currentMembership = segmentUnit.getMembership();
        if (null == highestMembershipInSegment) {
          highestMembershipInSegment = currentMembership;
        } else if (null != currentMembership
            && currentMembership.compareVersion(highestMembershipInSegment) > 0) {
          highestMembershipInSegment = currentMembership;
        }
      }

      volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);
    }
    volumeMetadata.setFreeSpaceRatio(volumeMetadataThrift.getFreeSpaceRatio());

    volumeMetadata.getRebalanceInfo().setRebalanceRatio(volumeMetadataThrift.getRebalanceRatio());
    volumeMetadata.getRebalanceInfo()
        .setRebalanceVersion(volumeMetadataThrift.getRebalanceVersion());
    volumeMetadata.setStableTime(volumeMetadataThrift.getStableTime());
    return volumeMetadata;
  }


  /**
   * Build basic segment unit metadata information for testing.
   */
  public SegmentUnitMetadata buildSegmentUnitMetadataFrom(
      SegmentUnitMetadataThrift segUnitMetaThrift) {
    Validate.notNull(segUnitMetaThrift);

    SegmentMembership membership = buildSegmentMembershipFrom(segUnitMetaThrift.getMembership())
        .getSecond();
    SegmentUnitStatus status = SegmentUnitStatus.valueOf(segUnitMetaThrift.getStatus().name());
    VolumeType volumeType = segUnitMetaThrift.getVolumeType() == null
        ? null
        : VolumeType.valueOf(segUnitMetaThrift.getVolumeType());

    SegId segId = new SegId(segUnitMetaThrift.getVolumeId(), segUnitMetaThrift.getSegIndex());
    SegmentUnitMetadata segUnit = new SegmentUnitMetadata(segId, segUnitMetaThrift.getOffset(),
        membership, status,
        volumeType, convertFromSegmentUnitTypeThrift(segUnitMetaThrift.getSegmentUnitType()));
    if (segUnitMetaThrift.isSetInstanceId()) {
      segUnit.setInstanceId(new InstanceId(segUnitMetaThrift.getInstanceId()));
    }
    segUnit.setAccountMetadataJson(segUnitMetaThrift.getAccountMetadataJson());
    segUnit.setVolumeMetadataJson(segUnitMetaThrift.getVolumeMetadataJson());
    segUnit.setLastUpdated(segUnitMetaThrift.getLastUpdated());
    segUnit.setLastReported(System.currentTimeMillis());
    segUnit.setDiskName(segUnitMetaThrift.getDiskName());
    segUnit.setArchiveId(segUnitMetaThrift.getArchiveId());

    if (segUnitMetaThrift.isSetRatioMigration()) {
      segUnit.setRatioMigration(segUnitMetaThrift.getRatioMigration());
    }

    segUnit.setTotalPageToMigrate((int) segUnitMetaThrift.getTotalPageToMigrate());
    segUnit.setAlreadyMigratedPage((int) segUnitMetaThrift.getAlreadyMigratedPage());
    segUnit.setMigrationSpeed((int) segUnitMetaThrift.getMigrationSpeed());
    segUnit.setInnerMigrating(segUnitMetaThrift.isInnerMigrating());

    segUnit.setEnableLaunchMultiDrivers(segUnitMetaThrift.isEnableLaunchMultiDrivers());
    segUnit.setVolumeSource(buildVolumeSourceTypeFrom(segUnitMetaThrift.getVolumeSource()));
    segUnit.setSrcVolumeId(segUnitMetaThrift.getSourceVolumeId());
    return segUnit;
  }

}