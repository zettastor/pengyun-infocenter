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
import static org.mockito.Matchers.anyString;
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
import py.icshare.AccessRuleStatusBindingVolume;
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
import py.thrift.share.DeleteIscsiAccessRulesRequest;
import py.thrift.share.DeleteIscsiAccessRulesResponse;
import py.thrift.share.IscsiAccessRuleThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;


/**
 * A class includes some test for delete iscsi access rules.
 */
public class DeleteIscsiAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DeleteIscsiAccessRulesTest.class);

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
   * A test for deleting iscsi access rules with false commit field. In the case, no iscsi access
   * rules could be delete.
   */
  @Test
  public void testDeleteIscsiAccessRulesWithoutCommit() throws Exception {
    DeleteIscsiAccessRulesRequest request = new DeleteIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(1111);
    request.setCommit(false);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

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
        buildIscsiRelationshipInfoList(0L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
        buildIscsiRelationshipInfoList(1L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.APPLIED));
    // no affect on "appling" volume access rules after "delete" action
    when(iscsiRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildIscsiRelationshipInfoList(2L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.APPLING));
    // no affect on "canceling" volume access rules after "delete" action
    when(iscsiRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildIscsiRelationshipInfoList(3L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.CANCELING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildIscsiRelationshipInfoList(4L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE));
    DeleteIscsiAccessRulesResponse response = icImpl.deleteIscsiAccessRules(request);

    Mockito.verify(iscsiAccessRuleStore, Mockito.times(0)).delete(anyLong());
    Mockito.verify(iscsiAccessRuleStore, Mockito.times(2))
        .save(any(IscsiAccessRuleInformation.class));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (IscsiAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 3L || ruleId == 2L);
    }
  }

  /**
   * A test for delete iscsi access rules with true commit field in request, In the case, all iscsi
   * access rules except appling and canceling rules could be delete.
   */
  @Test
  public void testDeleteVolumeAccessRulesWithConfirm() throws Exception {
    DeleteIscsiAccessRulesRequest request = new DeleteIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(1111);
    request.setCommit(true);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

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
        buildIscsiRelationshipInfoList(0L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(1L))).thenReturn(
        buildIscsiRelationshipInfoList(1L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.APPLIED));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildIscsiRelationshipInfoList(2L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.APPLING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildIscsiRelationshipInfoList(3L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.CANCELING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildIscsiRelationshipInfoList(4L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE));

    final DeleteIscsiAccessRulesResponse response = icImpl.deleteIscsiAccessRules(request);

    class IsProperToDelete extends ArgumentMatcher<Long> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        long ruleId = (long) o;
        // delete free, applied and deleting volume access rules
        if (ruleId == 0L || ruleId == 1L || ruleId == 4L) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(iscsiAccessRuleStore, Mockito.times(3)).delete(longThat(new IsProperToDelete()));
    Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(1)).deleteByRuleId(1L);
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (IscsiAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 3L || ruleId == 2L);
    }
  }


  public IscsiAccessRuleInformation buildIscsiAccessRuleInformation(long ruleId,
      AccessRuleStatus status) {
    IscsiAccessRuleInformation iscsiAccessRuleInformation = new IscsiAccessRuleInformation();
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
