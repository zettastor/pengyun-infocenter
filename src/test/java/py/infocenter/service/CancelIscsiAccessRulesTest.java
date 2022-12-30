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
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverType;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.DriverKey;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.AccessRuleStatus;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.CancelIscsiAccessRuleAllAppliedRequest;
import py.thrift.share.CancelIscsiAccessRuleAllAppliedResponse;
import py.thrift.share.CancelIscsiAccessRulesRequest;
import py.thrift.share.CancelIscsiAccessRulesResponse;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class includes some tests for canceling volume access rules.
 */
public class CancelIscsiAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CancelIscsiAccessRulesTest.class);

  private static final String DRIVER_TYPE = "ISCSI";
  private InformationCenterImpl icImpl;

  @Mock
  private IscsiAccessRuleStore iscsiAccessRuleStore;

  @Mock
  private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;

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

    icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
    icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for canceling iscsi access rules with false for commit field in request.
   */
  @Test
  public void testCancelIscsiAccessRulesWithoutCommit() throws Exception {
    CancelIscsiAccessRulesRequest request = new CancelIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.setCommit(false);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // no affect for action "cancel" on deleting iscsi access rules
    when(iscsiAccessRuleStore.get(eq(0L)))
        .thenReturn(buildIscsiAccessRuleInformation(0L, AccessRuleStatus.DELETING));
    when(iscsiAccessRuleStore.get(eq(1L)))
        .thenReturn(buildIscsiAccessRuleInformation(1L, AccessRuleStatus.AVAILABLE));
    when(iscsiAccessRuleStore.get(eq(2L)))
        .thenReturn(buildIscsiAccessRuleInformation(2L, AccessRuleStatus.AVAILABLE));
    when(iscsiAccessRuleStore.get(eq(3L)))
        .thenReturn(buildIscsiAccessRuleInformation(3L, AccessRuleStatus.AVAILABLE));
    when(iscsiAccessRuleStore.get(eq(4L)))
        .thenReturn(buildIscsiAccessRuleInformation(4L, AccessRuleStatus.AVAILABLE));

    when(iscsiRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
        buildIscsiRelationshipInfoList(0L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
        buildIscsiRelationshipInfoList(1L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.APPLIED));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildIscsiRelationshipInfoList(2L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.APPLING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildIscsiRelationshipInfoList(3L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.CANCELING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildIscsiRelationshipInfoList(4L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE));

    logger.warn("iscsiRuleRelationshipStore1 {}", iscsiRuleRelationshipStore.list().size());
    final CancelIscsiAccessRulesResponse response = icImpl.cancelIscsiAccessRules(request);

    class IsCanceling extends ArgumentMatcher<IscsiRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
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

    logger.warn("iscsiRuleRelationshipStore {}", iscsiRuleRelationshipStore.list());
    Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).save(argThat(new IsCanceling()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (IscsiAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 2L);
    }
  }

  /**
   * A test for canceling volume access rules with false for commit field in request.
   */
  @Test
  public void testCancelIscsiAccessRulesWithCommit() throws Exception {
    CancelIscsiAccessRulesRequest request = new CancelIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.setCommit(true);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // no affect for action "cancel" on deleting volume access rules
    when(iscsiAccessRuleStore.get(eq(0L)))
        .thenReturn(buildIscsiAccessRuleInformation(0L, AccessRuleStatus.DELETING));
    when(iscsiAccessRuleStore.get(eq(1L)))
        .thenReturn(buildIscsiAccessRuleInformation(1L, AccessRuleStatus.AVAILABLE));
    when(iscsiAccessRuleStore.get(eq(2L)))
        .thenReturn(buildIscsiAccessRuleInformation(2L, AccessRuleStatus.AVAILABLE));
    when(iscsiAccessRuleStore.get(eq(3L)))
        .thenReturn(buildIscsiAccessRuleInformation(3L, AccessRuleStatus.AVAILABLE));
    when(iscsiAccessRuleStore.get(eq(4L)))
        .thenReturn(buildIscsiAccessRuleInformation(4L, AccessRuleStatus.AVAILABLE));

    when(iscsiRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(
        buildIscsiRelationshipInfoList(0L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
        buildIscsiRelationshipInfoList(1L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.APPLIED));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildIscsiRelationshipInfoList(2L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.APPLING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildIscsiRelationshipInfoList(3L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.CANCELING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildIscsiRelationshipInfoList(4L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE));
    CancelIscsiAccessRulesResponse response = icImpl.cancelIscsiAccessRules(request);

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

    Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(2)).deleteByRuleIdandDriverKey(
        Matchers.any(), longThat(new IsFree()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (IscsiAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 2L);
    }
  }

  @Test
  public void testCancelIscsiAccessRuleAllApplied() throws Exception {
    CancelIscsiAccessRuleAllAppliedRequest request = new CancelIscsiAccessRuleAllAppliedRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setRuleId(0L);
    request.setCommit(true);
    request.addToDriverKeys(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 1L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 2L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 3L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 4L, 0, DriverTypeThrift.ISCSI));

    when(iscsiAccessRuleStore.get(eq(0L)))
        .thenReturn(buildIscsiAccessRuleInformation(0L, AccessRuleStatus.AVAILABLE));

    List<IscsiRuleRelationshipInformation> relationList = new ArrayList<>();
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 0L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 1L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(buildIscsiRelationshipInfoList(0L, 0L, 2L, 0, DRIVER_TYPE,
        AccessRuleStatusBindingVolume.APPLIED).get(0));
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 3L, 0, DRIVER_TYPE,
            AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(buildIscsiRelationshipInfoList(0L, 0L, 4L, 0, DRIVER_TYPE,
        AccessRuleStatusBindingVolume.APPLING).get(0));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(relationList);

    final CancelIscsiAccessRuleAllAppliedResponse response = icImpl
        .cancelIscsiAccessRuleAllApplied(request);

    class IsCanceling extends ArgumentMatcher<IscsiRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
        // turn "applied" volume access rules into "canceling"
        if (relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    logger.warn("iscsiRuleRelationshipStore {}", iscsiRuleRelationshipStore.list());

    //delete applied
    Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).deleteByRuleIdandDriverKey(
        new DriverKey(0L, 2L, 0, DriverType.ISCSI), 0L);
    // applying add to air list
    Assert.assertTrue(response.getAirDriverKeyListSize() == 1);
  }


  public IscsiAccessRuleInformation buildIscsiAccessRuleInformation(long ruleId,
      AccessRuleStatus status) {
    IscsiAccessRuleInformation iscsiAccessRuleInformation = new IscsiAccessRuleInformation();
    iscsiAccessRuleInformation.setRuleNotes("rule1");
    iscsiAccessRuleInformation.setInitiatorName("");
    iscsiAccessRuleInformation.setUser("");
    iscsiAccessRuleInformation.setPassed("");
    iscsiAccessRuleInformation.setPermission(2);
    iscsiAccessRuleInformation.setStatus(status.name());
    iscsiAccessRuleInformation.setRuleId(ruleId);
    return iscsiAccessRuleInformation;
  }


  public List<IscsiRuleRelationshipInformation> buildIscsiRelationshipInfoList(long ruleId,
      long did, long vid, int sid, String type,
      AccessRuleStatusBindingVolume status) {
    IscsiRuleRelationshipInformation relationshipInfo = new IscsiRuleRelationshipInformation();
    relationshipInfo.setRelationshipId(RequestIdBuilder.get());
    relationshipInfo.setRuleId(ruleId);
    relationshipInfo.setDriverContainerId(did);
    relationshipInfo.setVolumeId(vid);
    relationshipInfo.setSnapshotId(sid);
    relationshipInfo.setDriverType(type);
    relationshipInfo.setStatus(status.name());
    List<IscsiRuleRelationshipInformation> list = new ArrayList<IscsiRuleRelationshipInformation>();
    list.add(relationshipInfo);
    return list;
  }
}
