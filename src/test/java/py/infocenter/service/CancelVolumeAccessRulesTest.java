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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.AccessRuleInformation;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.VolumeRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.AccessRuleStatus;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.CancelVolAccessRuleAllAppliedRequest;
import py.thrift.share.CancelVolAccessRuleAllAppliedResponse;
import py.thrift.share.CancelVolumeAccessRulesRequest;
import py.thrift.share.CancelVolumeAccessRulesResponse;
import py.thrift.share.VolumeAccessRuleThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class includes some tests for canceling volume access rules.
 *
 */
public class CancelVolumeAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CancelVolumeAccessRulesTest.class);

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


  @Before
  public void init() throws Exception {
    super.init();

    icImpl = new InformationCenterImpl();

    VolumeMetadata volume = new VolumeMetadata();
    volume.setVolumeStatus(VolumeStatus.Available);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    icImpl.setAccessRuleStore(accessRuleStore);
    icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for canceling volume access rules with false for commit field in request.
   */
  @Test
  public void testCancelVolumeAccessRulesWithoutCommit() throws Exception {
    CancelVolumeAccessRulesRequest request = new CancelVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(0L);
    request.setCommit(false);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // no affect for action "cancel" on deleting volume access rules
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
    // no affect for action "cancel" on appling volume access rules
    when(volumeRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildRelationshipInfoList(2L, 0L, AccessRuleStatusBindingVolume.APPLING));
    when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
    when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));
    CancelVolumeAccessRulesResponse response = icImpl.cancelVolumeAccessRules(request);

    class IsCanceling extends ArgumentMatcher<VolumeRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        VolumeRuleRelationshipInformation relationshipInfo = (VolumeRuleRelationshipInformation) o;
        // turn "applied" volume access rules into "canceling"
        if (relationshipInfo.getRuleId() != 1L
            || relationshipInfo.getStatus()
            .equals(AccessRuleStatusBindingVolume.CANCELING.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsCanceling()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (VolumeAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 2L);
    }
  }

  /**
   * A test for canceling volume access rules with false for commit field in request.
   */
  @Test
  public void testCancelVolumeAccessRulesWithCommit() throws Exception {
    CancelVolumeAccessRulesRequest request = new CancelVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(0L);
    request.setCommit(true);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);


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
    when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
    when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));
    CancelVolumeAccessRulesResponse response = icImpl.beginCancelVolumeAccessRule(request);

    class IsFree extends ArgumentMatcher<Long> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        long ruleId = (long) o;
        // delete "applied" and "canceling" volume access rules from relationship table
        if (ruleId == 1L || ruleId == 3L) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(2)).deleteByRuleIdandVolumeId(eq(0L),
        longThat(new IsFree()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (VolumeAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 2L);
    }
  }


  @Test
  public void testCancelVolumeAccessRuleAllApplied() throws Exception {
    CancelVolAccessRuleAllAppliedRequest request = new CancelVolAccessRuleAllAppliedRequest();
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

    CancelVolAccessRuleAllAppliedResponse response = icImpl.cancelVolAccessRuleAllApplied(request);

    assert (response.getAirVolumeIdsSize() == 1);
  }


  public AccessRuleInformation buildAccessRuleInformation(long ruleId, AccessRuleStatus status) {
    AccessRuleInformation accessRuleInformation = new AccessRuleInformation();
    accessRuleInformation.setIpAddress("");
    accessRuleInformation.setPermission(2);
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

    List<VolumeRuleRelationshipInformation> list =
        new ArrayList<VolumeRuleRelationshipInformation>();
    list.add(relationshipInfo);
    return list;
  }
}
