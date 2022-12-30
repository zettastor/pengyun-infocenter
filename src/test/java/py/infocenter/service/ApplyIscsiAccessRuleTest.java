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
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.Assert;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.store.DriverStore;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.AccessRuleStatus;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.ApplyIscsiAccessRuleOnIscsisRequest;
import py.thrift.share.ApplyIscsiAccessRuleOnIscsisResponse;
import py.thrift.share.ApplyIscsiAccessRulesRequest;
import py.thrift.share.ApplyIscsiAccessRulesResponse;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class includes some tests for applying iscsi access rule.
 */
public class ApplyIscsiAccessRuleTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ApplyIscsiAccessRuleTest.class);

  private InformationCenterImpl icImpl;

  @Mock
  private IscsiAccessRuleStore iscsiAccessRuleStore;

  @Mock
  private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;

  @Mock
  private VolumeStore volumeStore;

  @Mock
  private DriverStore driverStore;

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
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    DriverMetadata driver1 = new DriverMetadata();
    driver1.setDriverContainerId(0L);
    driver1.setSnapshotId(0);
    driver1.setDriverType(DriverType.ISCSI);
    driver1.setVolumeId(0L);

    DriverMetadata driver2 = new DriverMetadata();
    driver2.setDriverContainerId(0L);
    driver2.setSnapshotId(0);
    driver2.setDriverType(DriverType.ISCSI);
    driver2.setVolumeId(1L);

    when(driverStore.get(0L, 0L, DriverType.ISCSI, 0)).thenReturn(driver1);
    when(driverStore.get(0L, 1L, DriverType.ISCSI, 0)).thenReturn(driver2);

    icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
    icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setDriverStore(driverStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  
  @Test
  public void testApplyIscsiAccessRulesWithoutCommit() throws Exception {
    ApplyIscsiAccessRulesRequest request = new ApplyIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.setCommit(false);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // the access rule with current status deleting after action "apply" is still deleting
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
    // the access rule with current status canceling after action "apply" is still canceling
    when(iscsiRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildIscsiRelationshipInfoList(3L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.CANCELING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildIscsiRelationshipInfoList(4L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE));

    ApplyIscsiAccessRulesResponse response = icImpl.applyIscsiAccessRules(request);

    class IsAppling extends ArgumentMatcher<IscsiRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
        // the status of free access rules could be applying after action "apply"
        if (relationshipInfo.getRuleId() != 4L
            || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLING.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(4)).save(argThat(new IsAppling()));
    Assert.assertTrue(response.getAirAccessRuleListSize() == 1);
    for (IscsiAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 3L);
    }
  }

  /**
   * Test for applying volume with true value for "commit" field in request. In the case, all access
   * rules except those who are canceling or deleting could be applied to volume.
   */
  @Test
  public void testApplyIscsiAccessRulesWithCommit() throws Exception {
    ApplyIscsiAccessRulesRequest request = new ApplyIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.setCommit(true);
    request.addToRuleIds(0L);
    request.addToRuleIds(1L);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.addToRuleIds(4L);

    // no affect after action "apply" on "deleting" volume access rules
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
    // the access rule with current status canceling after action "apply" is still canceling
    when(iscsiRuleRelationshipStore.getByRuleId(eq(3L))).thenReturn(
        buildIscsiRelationshipInfoList(3L, 0L, 0L, 0, "IScsi",
            AccessRuleStatusBindingVolume.CANCELING));
    when(iscsiRuleRelationshipStore.getByRuleId(eq(4L))).thenReturn(
        buildIscsiRelationshipInfoList(4L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE));

    ApplyIscsiAccessRulesResponse response = icImpl.applyIscsiAccessRules(request);

    class IsApplied extends ArgumentMatcher<IscsiRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
        // "free" and "appling" volume access rules turn into "applied" after action "apply"
        if (relationshipInfo.getRuleId() != 4L || relationshipInfo.getRuleId() != 2L
            || relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(4)).save(argThat(new IsApplied()));
    logger.info("response air access rule list size :{}", response.getAirAccessRuleListSize());
    Assert.assertTrue(response.getAirAccessRuleListSize() == 1);
    for (IscsiAccessRuleThrift ruleFromRemote : response.getAirAccessRuleList()) {
      long ruleId = ruleFromRemote.getRuleId();
      Assert.assertTrue(ruleId == 0L || ruleId == 3L);
    }
  }

  @Test
  public void testaApplyIscsiAccessRuleOnIscsis() throws Exception {
    ApplyIscsiAccessRuleOnIscsisRequest request = new ApplyIscsiAccessRuleOnIscsisRequest();
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
        buildIscsiRelationshipInfoList(0L, 0L, 0L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 1L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 2L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 3L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE)
            .get(0));
    relationList.add(
        buildIscsiRelationshipInfoList(0L, 0L, 4L, 0, "IScsi", AccessRuleStatusBindingVolume.FREE)
            .get(0));

    when(iscsiRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(relationList);

    ApplyIscsiAccessRuleOnIscsisResponse response = icImpl.applyIscsiAccessRuleOnIscsis(request);

    // driverstore only have two drivers
    Assert.assertTrue(response.getAirDriverKeyListSize() == 3);
  }

  @Test
  public void testApplyIscsiAccessRuleOnIscsiForMultiThread()
      throws TException, InterruptedException {
    int count = 10;
    CountDownLatch countDownLatch = new CountDownLatch(count);
    CyclicBarrier cyclicBarrier = new CyclicBarrier(count);
    final List<IscsiRuleRelationshipInformation> relationList = new ArrayList<>();

    when(iscsiRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(relationList);
    when(iscsiAccessRuleStore.get(eq(0L)))
        .thenReturn(buildIscsiAccessRuleInformation(0L, AccessRuleStatus
            .AVAILABLE));
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) {
        Object[] arguments = invocation.getArguments();
        IscsiRuleRelationshipInformation argument = (IscsiRuleRelationshipInformation) arguments[0];
        relationList.add(argument);
        return null;
      }
    }).when(iscsiRuleRelationshipStore).save(any(IscsiRuleRelationshipInformation.class));

    ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < count; i++) {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          try {
            cyclicBarrier.await();
            applyIscsiAccessRuleOnDriver(relationList);
          } catch (Exception e) {
            logger.error("execute failed: ", e);
          }
          countDownLatch.countDown();
        }
      });
    }

    executorService.shutdown();
    countDownLatch.await();
    Assert.assertEquals(2, relationList.size());
  }

  
  public void applyIscsiAccessRuleOnDriver(List<IscsiRuleRelationshipInformation> relationList)
      throws TException {
    ApplyIscsiAccessRuleOnIscsisRequest request = new ApplyIscsiAccessRuleOnIscsisRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setRuleId(0L);
    request.setCommit(true);
    request.addToDriverKeys(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 1L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 2L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 3L, 0, DriverTypeThrift.ISCSI));
    request.addToDriverKeys(new DriverKeyThrift(0L, 4L, 0, DriverTypeThrift.ISCSI));

    icImpl.applyIscsiAccessRuleOnIscsis(request);
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
