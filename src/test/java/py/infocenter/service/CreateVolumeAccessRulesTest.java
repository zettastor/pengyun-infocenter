/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.infocenter.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.AccessRuleInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.CreateVolumeAccessRulesRequest;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.VolumeAccessRuleDuplicateThrift;
import py.thrift.share.VolumeAccessRuleThrift;

/**
 * A class includes some tests for creating volume access rules.
 *
 */
public class CreateVolumeAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CreateVolumeAccessRulesTest.class);

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

  @Mock
  private OperationStore operationStore;


  @Before
  public void init() throws Exception {
    super.init();

    icImpl = new InformationCenterImpl();

    List<AccessRuleInformation> accessRuleInformationList = new ArrayList<AccessRuleInformation>();
    when(accessRuleStore.list()).thenReturn(accessRuleInformationList);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    icImpl.setAccessRuleStore(accessRuleStore);
    icImpl.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);
    icImpl.setOperationStore(operationStore);
    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for create volume access rules. In the case, new access rules should save to db.
   */
  @Test
  public void testCreateVolumeAccessRules()
      throws VolumeAccessRuleDuplicateThrift, InvalidInputExceptionThrift,
      TException {
    VolumeAccessRuleThrift accessRuleFromRemote = new VolumeAccessRuleThrift();
    accessRuleFromRemote.setRuleId(0L);
    accessRuleFromRemote.setIncomingHostName("10.0.1.16");
    accessRuleFromRemote.setPermission(AccessPermissionTypeThrift.READWRITE);
    CreateVolumeAccessRulesRequest request = new CreateVolumeAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.addToAccessRules(accessRuleFromRemote);

    icImpl.createVolumeAccessRules(request);
    Mockito.verify(accessRuleStore, Mockito.times(1)).save(any(AccessRuleInformation.class));
  }
}
