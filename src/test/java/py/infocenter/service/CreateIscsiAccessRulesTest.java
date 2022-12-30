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

import static org.junit.Assert.assertTrue;
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
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.ChapSameUserPasswdErrorThrift;
import py.thrift.share.CreateIscsiAccessRulesRequest;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.IscsiAccessRuleDuplicateThrift;
import py.thrift.share.IscsiAccessRuleThrift;


/**
 * A class includes some tests for creating iscsi access rules.
 */
public class CreateIscsiAccessRulesTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CreateIscsiAccessRulesTest.class);

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

  @Mock
  private OperationStore operationStore;


  @Before
  public void init() throws Exception {
    super.init();

    icImpl = new InformationCenterImpl();

    List<IscsiAccessRuleInformation> iscsiAccessRuleInformationList =
        new ArrayList<IscsiAccessRuleInformation>();
    when(iscsiAccessRuleStore.list()).thenReturn(iscsiAccessRuleInformationList);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    icImpl.setIscsiAccessRuleStore(iscsiAccessRuleStore);
    icImpl.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore);
    icImpl.setVolumeStore(volumeStore);
    icImpl.setAppContext(appContext);
    icImpl.setSecurityManager(securityManager);
    icImpl.setOperationStore(operationStore);
    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for create iscsi access rules. In the case, new access rules should save to db.
   */
  @Test
  public void testCreateIscsiAccessRules()
      throws IscsiAccessRuleDuplicateThrift, InvalidInputExceptionThrift,
      TException {
    IscsiAccessRuleThrift iscsiAccessRuleFromRemote = new IscsiAccessRuleThrift();
    iscsiAccessRuleFromRemote.setRuleId(0L);
    iscsiAccessRuleFromRemote.setInitiatorName("iqn:");
    iscsiAccessRuleFromRemote.setUser("user1");
    iscsiAccessRuleFromRemote.setPassed("passwd1");
    iscsiAccessRuleFromRemote.setOutPassed("user2");
    iscsiAccessRuleFromRemote.setOutUser("passwd2");
    iscsiAccessRuleFromRemote.setPermission(AccessPermissionTypeThrift.READWRITE);
    CreateIscsiAccessRulesRequest request = new CreateIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.addToAccessRules(iscsiAccessRuleFromRemote);

    icImpl.createIscsiAccessRules(request);
    Mockito.verify(iscsiAccessRuleStore, Mockito.times(1))
        .save(any(IscsiAccessRuleInformation.class));
  }


  /**
   * A test for create iscsi access rules. In the case, create error.
   */
  @Test
  public void testCreateChapSameUserPasswd()
      throws IscsiAccessRuleDuplicateThrift, InvalidInputExceptionThrift,
      TException {
    IscsiAccessRuleThrift iscsiAccessRuleFromRemote = new IscsiAccessRuleThrift();
    iscsiAccessRuleFromRemote.setRuleId(0L);
    iscsiAccessRuleFromRemote.setInitiatorName("iqn:");
    iscsiAccessRuleFromRemote.setUser("user1");
    iscsiAccessRuleFromRemote.setPassed("passwd1");
    iscsiAccessRuleFromRemote.setOutUser("user1");
    iscsiAccessRuleFromRemote.setOutPassed("passwd2");
    iscsiAccessRuleFromRemote.setPermission(AccessPermissionTypeThrift.READWRITE);
    CreateIscsiAccessRulesRequest request = new CreateIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.addToAccessRules(iscsiAccessRuleFromRemote);

    boolean isException = false;
    try {
      icImpl.createIscsiAccessRules(request);
    } catch (ChapSameUserPasswdErrorThrift e) {
      logger.error("create with the same user of incoming and outgoing user");
      isException = true;
    }
    assertTrue(isException);
  }

  /**
   * A test for create iscsi access rules. In the case, create error.
   */
  @Test
  public void testCreateOnlyOutgoingUser()
      throws IscsiAccessRuleDuplicateThrift, InvalidInputExceptionThrift,
      TException {
    IscsiAccessRuleThrift iscsiAccessRuleFromRemote = new IscsiAccessRuleThrift();
    iscsiAccessRuleFromRemote.setRuleId(0L);
    iscsiAccessRuleFromRemote.setInitiatorName("iqn:");
    iscsiAccessRuleFromRemote.setUser("");
    iscsiAccessRuleFromRemote.setPassed("");
    iscsiAccessRuleFromRemote.setOutUser("user1");
    iscsiAccessRuleFromRemote.setOutPassed("passwd2");
    iscsiAccessRuleFromRemote.setPermission(AccessPermissionTypeThrift.READWRITE);
    CreateIscsiAccessRulesRequest request = new CreateIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.addToAccessRules(iscsiAccessRuleFromRemote);

    boolean isException = false;
    try {
      icImpl.createIscsiAccessRules(request);
    } catch (InvalidInputExceptionThrift e) {
      isException = true;
    }
    assertTrue(isException);
  }

}
