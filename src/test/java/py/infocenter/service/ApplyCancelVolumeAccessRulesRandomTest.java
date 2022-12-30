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

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
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
import py.thrift.share.ApplyVolumeAccessRulesRequest;
import py.thrift.share.ApplyVolumeAccessRulesResponse;
import py.thrift.share.CancelVolumeAccessRulesRequest;
import py.thrift.share.CancelVolumeAccessRulesResponse;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;


public class ApplyCancelVolumeAccessRulesRandomTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ApplyVolumeAccessRuleTest.class);

  private InformationCenterImpl icImpl;

  private int acount = 0;
  private int ccount = 0;

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

  @Mock
  private VolumeInformationManger volumeInformationManger;

  private VolumeMetadata volume;

  
  @Before
  public void init() throws Exception {
    super.init();

    icImpl = new InformationCenterImpl();

    volume = new VolumeMetadata();
    volume.setVolumeStatus(VolumeStatus.Available);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    volumeAndDriverInfo.setVolumeMetadata(volume);

    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    icImpl.setAccessRuleStore(accessRuleStore);
    icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);
    icImpl.setVolumeInformationManger(volumeInformationManger);

  }

  @Test
  public void testApplyVolumeAccessRulesWithTwoVolumes() throws Exception {
    Configuration conf = new Configuration();
    testCase(conf);
    while (conf.next()) {
      testCase(conf);
    }
  }

  
  private void testCase(Configuration conf) throws Exception {
    System.out.println(conf.toString());
    ApplyVolumeAccessRulesRequest arequest = new ApplyVolumeAccessRulesRequest();
    arequest.setRequestId(RequestIdBuilder.get());
    arequest.setVolumeId(0L);
    arequest.setCommit(conf.applyCommit);
    arequest.addToRuleIds(0L);

    CancelVolumeAccessRulesRequest crequest = new CancelVolumeAccessRulesRequest();
    crequest.setRequestId(RequestIdBuilder.get());
    crequest.setVolumeId(1L);
    crequest.setCommit(conf.cancelCommit);
    crequest.addToRuleIds(0L);

    AccessRuleInformation ruleInfo = buildAccessRuleInformation(0L, AccessRuleStatus.AVAILABLE);

    when(accessRuleStore.get(eq(0L))).thenReturn(ruleInfo);

    VolumeRuleRelationshipInformation relation00 = new VolumeRuleRelationshipInformation(
        RequestIdBuilder.get(), 0L, 0L);
    relation00.setStatus(conf.applyStatus);

    VolumeRuleRelationshipInformation relation10 = new VolumeRuleRelationshipInformation(
        RequestIdBuilder.get(), 1L, 0L);
    relation10.setStatus(conf.cancelStatus);

    List<VolumeRuleRelationshipInformation> list =
        new ArrayList<VolumeRuleRelationshipInformation>();
    list.add(relation10);
    list.add(relation00);

    when(volumeRuleRelationshipStore.getByRuleId(eq(0L))).thenReturn(list);

    ApplyThread apply = new ApplyThread(arequest);
    CancelThread cancel = new CancelThread(crequest);

    Thread athread = new Thread(apply);
    Thread cthread = new Thread(cancel);

    athread.start();
    cthread.start();

    while (!apply.done || !cancel.done) {
      Thread.sleep(100);
    }

    ApplyVolumeAccessRulesResponse response = apply.getResponse();
    CancelVolumeAccessRulesResponse response1 = cancel.getResponse();

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

    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(acount))
        .save(argThat(new IsApplied()));
    Mockito.verify(volumeRuleRelationshipStore, Mockito.times(ccount))
        .deleteByRuleIdandVolumeId(1L, 0L);
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

  class ApplyThread implements Runnable {

    public boolean done = false;
    private ApplyVolumeAccessRulesRequest request;
    private ApplyVolumeAccessRulesResponse response;

    public ApplyThread(ApplyVolumeAccessRulesRequest request) {
      this.request = request;
    }

    public void run() {
      try {
        System.out.println("send request");
        response = icImpl.beginApplyVolumeAccessRules(request, volume);
        done = true;
      } catch (TException e) {
        done = true;
        e.printStackTrace();
      }
    }

    public ApplyVolumeAccessRulesResponse getResponse() {
      return response;
    }

    public void setResponse(ApplyVolumeAccessRulesResponse response) {
      this.response = response;
    }
  }

  class CancelThread implements Runnable {

    public boolean done = false;
    private CancelVolumeAccessRulesRequest request;
    private CancelVolumeAccessRulesResponse response;

    public CancelThread(CancelVolumeAccessRulesRequest request) {
      this.request = request;
    }

    public void run() {
      try {
        setResponse(icImpl.beginCancelVolumeAccessRule(request));
        done = true;
      } catch (TException e) {
        done = true;
        e.printStackTrace();
      }
    }

    public CancelVolumeAccessRulesResponse getResponse() {
      return response;
    }

    public void setResponse(CancelVolumeAccessRulesResponse response) {
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
