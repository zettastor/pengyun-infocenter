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
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.icshare.AccessRuleInformation;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.VolumeRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.AccessRuleStatus;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.ApplyFailedDueToVolumeIsReadOnlyExceptionThrift;
import py.thrift.share.ApplyVolumeAccessRuleOnVolumesRequest;
import py.thrift.share.ApplyVolumeAccessRuleOnVolumesResponse;
import py.thrift.share.ApplyVolumeAccessRulesRequest;
import py.thrift.share.ApplyVolumeAccessRulesResponse;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * A class includes some tests for applying volume access rule.
 *
 */
public class ApplyVolumeAccessRuleTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ApplyVolumeAccessRuleTest.class);

  private InformationCenterImpl icImpl;

  @Mock
  private AccessRuleStore accessRuleStore;

  @Mock
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;

  @Mock
  private VolumeStore volumeStore;
  @Mock
  private InfoCenterAppContext appContext;

  @Mock
  private PySecurityManager securityManager;

  private VolumeMetadata volume;

  @Mock
  private VolumeInformationManger volumeInformationManger;


  @Before
  public void init() throws Exception {
    super.init();

    icImpl = new InformationCenterImpl();
    icImpl.setVolumeInformationManger(volumeInformationManger);

    volume = new VolumeMetadata();
    volume.setVolumeStatus(VolumeStatus.Available);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume.setVolumeId(1);
    volume.setName("volume_1");
    volume.setVolumeType(VolumeType.SMALL);
    volume.setVolumeStatus(VolumeStatus.Available);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);

    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    //set the volume
    VolumeMetadataAndDrivers volumeMetadataAndDrivers = new VolumeMetadataAndDrivers();
    volumeMetadataAndDrivers.setVolumeMetadata(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeMetadataAndDrivers);

    icImpl.setAccessRuleStore(accessRuleStore);
    icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * Test for applying volume with false value for "commit" field in request. In the case, no access
   * rules could be applied to volume but status transfers to applying.
   */
  @Test
  public void testApplyVolumeAccessRulesWithoutCommit() throws Exception {
    ApplyVolumeAccessRulesRequest request = new ApplyVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
    request.setVolumeId(0L);
    request.setCommit(false);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // the access rule with current status deleting after action "apply" is still deleting
    when(accessRuleStore.get(eq(0L)))
        .thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.DELETING));
    when(accessRuleStore.get(eq(1L)))
        .thenReturn(buildAccessRuleInformation(1L, AccessRuleStatus.AVAILABLE));
    when(accessRuleStore.get(eq(2L)))
        .thenReturn(buildAccessRuleInformation(2L, AccessRuleStatus.AVAILABLE));
    when(accessRuleStore.get(eq(3L)))
        .thenReturn(buildAccessRuleInformation(3L, AccessRuleStatus.AVAILABLE));
    when(accessRuleStore.get(eq(4L)))
        .thenReturn(buildAccessRuleInformation(4L, AccessRuleStatus.AVAILABLE));

    when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
        buildRelationshipInfoList(0L, 0L, AccessRuleStatusBindingVolume.FREE));
    when(volumeRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
        buildRelationshipInfoList(1L, 0L, AccessRuleStatusBindingVolume.APPLIED));
    when(volumeRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildRelationshipInfoList(2L, 0L, AccessRuleStatusBindingVolume.APPLING));
    // the access rule with current status canceling after action "apply" is still canceling
    when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
    when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));

    ApplyVolumeAccessRulesResponse response = icImpl.applyVolumeAccessRules(request);

    class IsAppling extends ArgumentMatcher<VolumeRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
        // the status of free access rules could be applying after action "apply"
        if (relationshipInfo.getRuleId() != 4L
            || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLING.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsAppling()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (VolumeAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 3L);
    }
  }


  @Test
  public void testApplyVolumeAccessRulesWithCommit() throws Exception {
    ApplyVolumeAccessRulesRequest request = new ApplyVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(0L);
    request.setCommit(true);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // no affect after action "apply" on "deleting" volume access rules
    when(accessRuleStore.get(eq(0L)))
        .thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.DELETING));
    when(accessRuleStore.get(eq(1L)))
        .thenReturn(buildAccessRuleInformation(1L, AccessRuleStatus.AVAILABLE));
    when(accessRuleStore.get(eq(2L)))
        .thenReturn(buildAccessRuleInformation(2L, AccessRuleStatus.AVAILABLE));
    when(accessRuleStore.get(eq(3L)))
        .thenReturn(buildAccessRuleInformation(3L, AccessRuleStatus.AVAILABLE));
    when(accessRuleStore.get(eq(4L)))
        .thenReturn(buildAccessRuleInformation(4L, AccessRuleStatus.AVAILABLE));

    when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
        buildRelationshipInfoList(0L, 0L, AccessRuleStatusBindingVolume.FREE));
    when(volumeRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
        buildRelationshipInfoList(1L, 0L, AccessRuleStatusBindingVolume.APPLIED));
    when(volumeRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildRelationshipInfoList(2L, 0L, AccessRuleStatusBindingVolume.APPLING));
    // no affect after action "apply" on "canceling" volume access rules
    when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
    when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));
    ApplyVolumeAccessRulesResponse response = icImpl.beginApplyVolumeAccessRules(request, volume);

    class IsApplied extends ArgumentMatcher<VolumeRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
        // "free" and "appling" volume access rules turn into "applied" after action "apply"
        if (relationshipInfo.getRuleId() != 4L || relationshipInfo.getRuleId() != 2L
            || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(2)).save(argThat(new IsApplied()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (VolumeAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 3L);
    }
  }

  /**
   * Volume is marked as readOnly, apply an access rule with write permission. Expected:
   * ApplyFailedDueToVolumeIsReadOnlyExceptionThrift
   */
  @Test(expected = ApplyFailedDueToVolumeIsReadOnlyExceptionThrift.class)
  public void testApplyWriteAccessRuleToReadOnlyVolume() throws Exception {
    // Prepare volume
    VolumeMetadata volume1 = new VolumeMetadata();
    volume1.setVolumeStatus(VolumeStatus.Available);
    volume1.setReadWrite(VolumeMetadata.ReadWriteType.READONLY);

    //set the volume
    VolumeMetadataAndDrivers volumeMetadataAndDrivers = new VolumeMetadataAndDrivers();
    volumeMetadataAndDrivers.setVolumeMetadata(volume1);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeMetadataAndDrivers);

    // Prepare access rule
    when(accessRuleStore.get(eq(0L)))
        .thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.FREE));

    // Start test
    ApplyVolumeAccessRulesRequest request = new ApplyVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(0L);
    request.setCommit(false);
    request.addToRuleIds(0L);
    icImpl.applyVolumeAccessRules(request);
  }


  /**
   * Test for applying volume with true value, all access rules except those who are canceling or
   * deleting could be applied to volume.
   */
  @Test
  public void testApplyVolumeAccessRuleOnVolumes() throws Exception {
    ApplyVolumeAccessRuleOnVolumesRequest request = new ApplyVolumeAccessRuleOnVolumesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setRuleId(0L);
    request.setCommit(true);

    request.addToVolumeIds(0L);
    request.addToVolumeIds(1L);
    request.addToVolumeIds(2L);
    request.addToVolumeIds(3L);
    request.addToVolumeIds(4L);

    List<VolumeRuleRelationshipInformation> relationshipInfoList = new ArrayList<>();

    relationshipInfoList
        .add(buildRelationshipInfoList(0L, 0L, AccessRuleStatusBindingVolume.FREE).get(0));
    relationshipInfoList
        .add(buildRelationshipInfoList(0L, 1L, AccessRuleStatusBindingVolume.APPLIED).get(0));
    relationshipInfoList
        .add(buildRelationshipInfoList(0L, 2L, AccessRuleStatusBindingVolume.APPLING).get(0));
    relationshipInfoList
        .add(buildRelationshipInfoList(0L, 3L, AccessRuleStatusBindingVolume.CANCELING).get(0));
    relationshipInfoList
        .add(buildRelationshipInfoList(0L, 4L, AccessRuleStatusBindingVolume.FREE).get(0));

    when(accessRuleStore.get(eq(0L)))
        .thenReturn(buildAccessRuleInformation(0L, AccessRuleStatus.AVAILABLE));

    when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
        relationshipInfoList);

    ApplyVolumeAccessRuleOnVolumesResponse response = icImpl
        .applyVolumeAccessRuleOnVolumes(request);

    class IsApplied extends ArgumentMatcher<VolumeRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
        // "free" and "appling" volume access rules turn into "applied" after action "apply"
        if (relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(3)).save(argThat(new IsApplied()));
    //canceling
    assert (response.getAirVolumeList().size() == 1);
  }

  /**
   * Test for applying volume with two rules, which has same ip. 2 Rules with same ip, 5 volume in
   * pool, 3 volume applied rule1 apply rule2, 3 volume will be failed
   */
  @Test//(expected=)
  public void testApplyVolumeAccessRuleOnVolumes_with2SameIpRule() throws Exception {
    long oldRuleId = 0;
    long newRuleId = 1;
    final long volumeCount = 5;
    long relationshipVolumeCount = 3;
    ApplyVolumeAccessRuleOnVolumesRequest request = new ApplyVolumeAccessRuleOnVolumesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setRuleId(newRuleId);
    request.setCommit(true);

    List<VolumeRuleRelationshipInformation> relationshipInfoList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    Set<Long> appliedVolumeIdList = new HashSet<>();
    for (long i = 0; i < volumeCount; i++) {
      long volumeId = i;
      VolumeMetadata volume = new VolumeMetadata();
      volume.setVolumeId(volumeId);
      volume.setName("volume_" + i);
      volume.setVolumeType(VolumeType.SMALL);
      volume.setVolumeStatus(VolumeStatus.Available);
      volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);

      volumeMetadataList.add(volume);

      request.addToVolumeIds(i);

      when(volumeStore.getVolume(i)).thenReturn(volume);

      if (i < relationshipVolumeCount) {
        relationshipInfoList.add(
            buildRelationshipInfoList(oldRuleId, i, AccessRuleStatusBindingVolume.FREE).get(0));
        appliedVolumeIdList.add(i);
      }
    }

    when(accessRuleStore.get(oldRuleId)).thenReturn(
        buildAccessRuleInformation(oldRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.10", 2));
    when(accessRuleStore.get(newRuleId)).thenReturn(
        buildAccessRuleInformation(newRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.10", 1));
    when(volumeRuleRelationshipStore.getByRuleId(oldRuleId)).thenReturn(relationshipInfoList);

    for (int i = 0; i < relationshipVolumeCount; i++) {
      List<VolumeRuleRelationshipInformation> volumeRelationshipInfoList = new ArrayList<>();
      volumeRelationshipInfoList.add(relationshipInfoList.get(i));
      when(volumeRuleRelationshipStore.getByVolumeId(i)).thenReturn(volumeRelationshipInfoList);
    }

    ApplyVolumeAccessRuleOnVolumesResponse response = icImpl
        .applyVolumeAccessRuleOnVolumes(request);

    assert (response.getAirVolumeList().size() == relationshipVolumeCount);

    Set<Long> failedAppliedVolumeIdList = new HashSet<>();
    for (VolumeMetadataThrift volumeMetadataThrift : response.getAirVolumeList()) {
      failedAppliedVolumeIdList.add(volumeMetadataThrift.getVolumeId());
    }

    assert (appliedVolumeIdList.size() == failedAppliedVolumeIdList.size());
    appliedVolumeIdList.retainAll(failedAppliedVolumeIdList);
    assert (appliedVolumeIdList.size() == failedAppliedVolumeIdList.size());

    Mockito.verify(volumeRuleRelationshipStore,
        Mockito.times((int) (volumeCount - relationshipVolumeCount)))
        .save(any(VolumeRuleRelationshipInformation.class));
  }

  /**
   * Test for applying volume with two rules, which has different ip. 2 Rules with different ip, 5
   * volume in pool, 3 volume applied rule1 apply rule2, all volume will be success
   */
  @Test//(expected=)
  public void testApplyVolumeAccessRuleOnVolumes_with2DiffIpRule() throws Exception {
    long oldRuleId = 0;
    long newRuleId = 1;
    final long volumeCount = 5;
    long relationshipVolumeCount = 3;
    ApplyVolumeAccessRuleOnVolumesRequest request = new ApplyVolumeAccessRuleOnVolumesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setRuleId(newRuleId);
    request.setCommit(true);

    List<VolumeRuleRelationshipInformation> relationshipInfoList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    Set<Long> appliedVolumeIdList = new HashSet<>();
    for (long i = 0; i < volumeCount; i++) {
      long volumeId = i;
      VolumeMetadata volume = new VolumeMetadata();
      volume.setVolumeId(volumeId);
      volume.setName("volume_" + i);
      volume.setVolumeType(VolumeType.SMALL);
      volume.setVolumeStatus(VolumeStatus.Available);
      volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);

      volumeMetadataList.add(volume);

      request.addToVolumeIds(i);

      when(volumeStore.getVolume(i)).thenReturn(volume);

      if (i < relationshipVolumeCount) {
        relationshipInfoList.add(
            buildRelationshipInfoList(oldRuleId, i, AccessRuleStatusBindingVolume.FREE).get(0));
        appliedVolumeIdList.add(i);
      }
    }

    when(accessRuleStore.get(oldRuleId)).thenReturn(
        buildAccessRuleInformation(oldRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.10", 2));
    when(accessRuleStore.get(newRuleId)).thenReturn(
        buildAccessRuleInformation(newRuleId, AccessRuleStatus.AVAILABLE, "192.168.2.11", 1));
    when(volumeRuleRelationshipStore.getByRuleId(oldRuleId)).thenReturn(relationshipInfoList);

    for (int i = 0; i < relationshipVolumeCount; i++) {
      List<VolumeRuleRelationshipInformation> volumeRelationshipInfoList = new ArrayList<>();
      volumeRelationshipInfoList.add(relationshipInfoList.get(i));
      when(volumeRuleRelationshipStore.getByVolumeId(i)).thenReturn(volumeRelationshipInfoList);
    }

    ApplyVolumeAccessRuleOnVolumesResponse response = icImpl
        .applyVolumeAccessRuleOnVolumes(request);

    assert (response.getAirVolumeList().size() == 0);

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times((int) (volumeCount)))
        .save(any(VolumeRuleRelationshipInformation.class));
  }

  public AccessRuleInformation buildAccessRuleInformation(long ruleId, AccessRuleStatus status) {
    AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
    accessRuleInformation.setIpAddress("");
    accessRuleInformation.setPermission(2);
    accessRuleInformation.setStatus(status.name());
    accessRuleInformation.setRuleId(ruleId);
    return accessRuleInformation;
  }

  public AccessRuleInformation buildAccessRuleInformation(long ruleId, AccessRuleStatus status,
      String ip, int permission) {
    AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
    accessRuleInformation.setIpAddress(ip);
    accessRuleInformation.setPermission(permission);
    accessRuleInformation.setStatus(status.name());
    accessRuleInformation.setRuleId(ruleId);
    return accessRuleInformation;
  }

  public List<VolumeRuleRelationshipInformation> buildRelationshipInfoList(long ruleId,
      long volumeId,
      AccessRuleStatusBindingVolume status) {
    VolumeRuleRelationshipInformation relationshipInfo = new VolumeRuleRelationshipInformation();

    relationshipInfo.setRelationshipId(RequestIdBuilder.get());
    relationshipInfo.setRuleId(ruleId);
    relationshipInfo.setVolumeId(volumeId);
    relationshipInfo.setStatus(status.name());

    List<VolumeRuleRelationshipInformation> list = new ArrayList<>();
    list.add(relationshipInfo);
    return list;
  }
}
