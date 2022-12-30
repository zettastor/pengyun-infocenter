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
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.DriverKey;
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
import py.thrift.share.ApplyIscsiAccessRulesRequest;
import py.thrift.share.ApplyIscsiAccessRulesResponse;
import py.thrift.share.CancelIscsiAccessRulesRequest;
import py.thrift.share.CancelIscsiAccessRulesResponse;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.IscsiBeingDeletedExceptionThrift;
import py.thrift.share.IscsiNotFoundExceptionThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;


public class ApplyCancelIscsiAccessRulesRandomTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(ApplyCancelIscsiAccessRulesRandomTest.class);
  private static final String DRIVER_TYPE = "ISCSI";
  private InformationCenterImpl icImpl;

  private int acount = 0;
  private int ccount = 0;

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


  /**
   * Build environment that running test.
   */
  @Before
  public void init() throws Exception {
    super.init();
    icImpl = new InformationCenterImpl();
    new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI);
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
    icImpl.setAppContext(appContext);
    icImpl.setDriverStore(driverStore);
    icImpl.setSecurityManager(securityManager);
    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  @Test
  public void testApplyIscsiAccessRulesWithTwoIscsis() throws Exception {
    Configuration conf = new Configuration();
    testCase(conf);
    while (conf.next()) {
      testCase(conf);
    }
  }

  private void testCase(Configuration conf) throws Exception {
    System.out.println(conf.toString());
    ApplyIscsiAccessRulesRequest request = new ApplyIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDriverKey(new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI));
    request.setCommit(conf.applyCommit);
    request.addToRuleIds(0L);

    CancelIscsiAccessRulesRequest request1 = new CancelIscsiAccessRulesRequest();
    request1.setRequestId(RequestIdBuilder.get());
    request1.setDriverKey(new DriverKeyThrift(0L, 1L, 0, DriverTypeThrift.ISCSI));
    request1.setCommit(conf.cancelCommit);
    request1.addToRuleIds(0L);

    IscsiAccessRuleInformation ruleInfo = buildIscsiAccessRuleInformation(0L,
        AccessRuleStatus.AVAILABLE);

    when(iscsiAccessRuleStore.get(eq(0L))).thenReturn(ruleInfo);

    IscsiRuleRelationshipInformation relation00 = new IscsiRuleRelationshipInformation(
        RequestIdBuilder.get(), 0L, 0L, 0, DRIVER_TYPE, 0L);
    relation00.setStatus(conf.applyStatus);

    IscsiRuleRelationshipInformation relation10 = new IscsiRuleRelationshipInformation(
        RequestIdBuilder.get(), 0L, 1L, 0, DRIVER_TYPE, 0L);
    relation10.setStatus(conf.cancelStatus);

    List<IscsiRuleRelationshipInformation> list = new ArrayList<IscsiRuleRelationshipInformation>();
    list.add(relation10);
    list.add(relation00);

    when(iscsiRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(list);

    ApplyThread apply = new ApplyThread(request);
    CancelThread cancel = new CancelThread(request1);

    Thread athread = new Thread(apply);
    Thread cthread = new Thread(cancel);

    boolean applySwitch = false;
    applySwitch = true;
    boolean cancelSwitch = false;
    cancelSwitch = true;

    if (applySwitch) {
      athread.start();
    }
    if (cancelSwitch) {
      cthread.start();
    }

    while ((applySwitch && !apply.done) || (cancelSwitch && !cancel.done)) {
      Thread.sleep(100);
    }

    ApplyIscsiAccessRulesResponse response = apply.getResponse();
    CancelIscsiAccessRulesResponse response1 = cancel.getResponse();

    class IsApplied extends ArgumentMatcher<IscsiRuleRelationshipInformation> {

      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        IscsiRuleRelationshipInformation relationshipInfo = (IscsiRuleRelationshipInformation) o;
        // "free" and "appling" volume access rules turn into "applied" after action "apply"
        if (relationshipInfo.getStatus().equals(AccessRuleStatusBindingVolume.APPLIED.name())) {
          return true;
        } else {
          return false;
        }
      }
    }

    if (applySwitch) {
      Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(acount))
          .save(argThat(new IsApplied()));
    }
    if (cancelSwitch) {
      Mockito.verify(iscsiRuleRelationshipStore, Mockito.times(ccount)).deleteByRuleIdandDriverKey(
          new DriverKey(0L, 1L, 0, DriverType.ISCSI), 0L);
    }
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

  class ApplyThread implements Runnable {

    public boolean done = false;
    private ApplyIscsiAccessRulesRequest request;
    private ApplyIscsiAccessRulesResponse response;

    public ApplyThread(ApplyIscsiAccessRulesRequest request) {
      this.request = request;
    }

    public void run() {
      try {
        System.out.println("send request");
        response = icImpl.applyIscsiAccessRules(request);
        done = true;
      } catch (IscsiNotFoundExceptionThrift e) {
        done = true;
        e.printStackTrace();
      } catch (IscsiBeingDeletedExceptionThrift e) {
        done = true;
        e.printStackTrace();
      } catch (TException e) {
        done = true;
        e.printStackTrace();
      }
    }

    public ApplyIscsiAccessRulesResponse getResponse() {
      return response;
    }

    public void setResponse(ApplyIscsiAccessRulesResponse response) {
      this.response = response;
    }
  }

  class CancelThread implements Runnable {

    public boolean done = false;
    private CancelIscsiAccessRulesRequest request;
    private CancelIscsiAccessRulesResponse response;

    public CancelThread(CancelIscsiAccessRulesRequest request) {
      this.request = request;
    }

    public void run() {
      try {
        setResponse(icImpl.cancelIscsiAccessRules(request));
        done = true;
      } catch (IscsiNotFoundExceptionThrift e) {
        done = true;
        e.printStackTrace();
      } catch (IscsiBeingDeletedExceptionThrift e) {
        done = true;
        e.printStackTrace();
      } catch (TException e) {
        done = true;
        e.printStackTrace();
      }
    }

    public CancelIscsiAccessRulesResponse getResponse() {
      return response;
    }

    public void setResponse(CancelIscsiAccessRulesResponse response) {
      this.response = response;
    }
  }

  class Configuration {

    public int acommit;
    public int astatus;
    public int ccommit;
    public int cstatus;

    public boolean applyCommit;
    public boolean cancelCommit;
    public String applyStatus;
    public String cancelStatus;

    public int count;

    public Configuration() {
      acommit = 0;
      astatus = 0;
      ccommit = 0;
      cstatus = 0;
      count = 0;
      trans();
    }

    public boolean next() {
      cstatus++;
      return adjust();
    }

    public boolean adjust() {
      if (cstatus > 3) {
        cstatus = 0;
        ccommit++;
      }
      if (ccommit > 1) {
        ccommit = 0;
        astatus++;
      }
      if (astatus > 3) {
        astatus = 0;
        acommit++;
      }
      if (acommit > 1) {
        return false;
      }
      trans();
      return true;
    }

    public void trans() {
      if (acommit == 0) {
        applyCommit = true;
      } else {
        applyCommit = false;
      }
      if (astatus == 0) {
        applyStatus = AccessRuleStatusBindingVolume.FREE.name();
      } else if (astatus == 1) {
        applyStatus = AccessRuleStatusBindingVolume.APPLING.name();
      } else if (astatus == 2) {
        applyStatus = AccessRuleStatusBindingVolume.APPLIED.name();
      } else if (astatus == 3) {
        applyStatus = AccessRuleStatusBindingVolume.CANCELING.name();
      }
      if (ccommit == 0) {
        cancelCommit = true;
      } else {
        cancelCommit = false;
      }
      if (cstatus == 0) {
        cancelStatus = AccessRuleStatusBindingVolume.FREE.name();
      } else if (cstatus == 1) {
        cancelStatus = AccessRuleStatusBindingVolume.APPLING.name();
      } else if (cstatus == 2) {
        cancelStatus = AccessRuleStatusBindingVolume.APPLIED.name();
      } else if (cstatus == 3) {
        cancelStatus = AccessRuleStatusBindingVolume.CANCELING.name();
      }
      if (applyCommit && (astatus == 0 || astatus == 1)) {
        acount++;
      }
      if (cancelCommit && (cstatus == 2 || cstatus == 3)) {
        ccount++;
      }
    }

    @Override
    public String toString() {
      return "Configuration [aCommit=" + acommit + ", aStatus=" + astatus + ", cCommit=" + ccommit
          + ", cStatus=" + cstatus + ", applyCommit=" + applyCommit + ", cancelCommit="
          + cancelCommit
          + ", applyStatus=" + applyStatus + ", cancelStatus=" + cancelStatus + "]";
    }
  }
}
