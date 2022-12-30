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
import py.thrift.share.DeleteVolumeAccessRulesRequest;
import py.thrift.share.DeleteVolumeAccessRulesResponse;
import py.thrift.share.VolumeAccessRuleThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class includes some test for delete volume access rules.
 *
 */
public class DeleteVolumeAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DeleteVolumeAccessRulesTest.class);

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
   * A test for deleting volume access rules with false commit field. In the case, no volume access
   * rules could be delete.
   */
  @Test
  public void testDeleteVolumeAccessRulesWithoutCommit() throws Exception {
    DeleteVolumeAccessRulesRequest request = new DeleteVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setCommit(false);
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
    // no affect on "appling" volume access rules after "delete" action
    when(volumeRuleRelationshipStore.getByRuleId(eq(2L))).thenReturn(
        buildRelationshipInfoList(2L, 0L, AccessRuleStatusBindingVolume.APPLING));
    // no affect on "canceling" volume access rules after "delete" action
    when(volumeRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildRelationshipInfoList(3L, 0L, AccessRuleStatusBindingVolume.CANCELING));
    when(volumeRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildRelationshipInfoList(4L, 0L, AccessRuleStatusBindingVolume.FREE));
    DeleteVolumeAccessRulesResponse response = icImpl
        .beginDeleteVolumeAccessRules(RequestIdBuilder.get(),
            request.getRuleIds(), request.isCommit());

    Mockito.verify(accessRuleStore, Mockito.times(0)).delete(anyLong());
    Mockito.verify(accessRuleStore, Mockito.times(2)).save(any(AccessRuleInformation.class));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (VolumeAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 3L || ruleId == 2L);
    }
  }

  /**
   * A test for delete volume access rules with true commit field in request, In the case, all
   * volume access rules except appling and canceling rules could be delete.
   */
  @Test
  public void testDeleteVolumeAccessRulesWithConfirm() throws Exception {
    DeleteVolumeAccessRulesRequest request = new DeleteVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
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

    final DeleteVolumeAccessRulesResponse response = icImpl
        .beginDeleteVolumeAccessRules(RequestIdBuilder.get(),
            request.getRuleIds(), request.isCommit());

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

    Mockito.verify(accessRuleStore, Mockito.times(3)).delete(longThat(new IsProperToDelete()));
    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(1)).deleteByRuleId(1L);
    Assert.assertTrue(response.getAirAccessRuleListSize() == 2);
    for (VolumeAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 3L || ruleId == 2L);
    }
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
