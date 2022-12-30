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

package py.infocenter.qos;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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
import py.RequestResponseHelper;
import py.common.RequestIdBuilder;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.io.qos.MigrationRuleStatus;
import py.test.TestBase;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.ListMigrationRulesRequest;


public class UpdateMigrateSpeedTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(UpdateMigrateSpeedTest.class);

  private InformationCenterImpl informationCenter;

  @Mock
  private MigrationRuleStore migrationRuleStore;

  @Mock
  private StoragePoolStore storagePoolStore;

  @Mock
  private InfoCenterAppContext appContext;

  @Mock
  private PySecurityManager securityManager;

  @Mock
  private OperationStore operationStore;


  @Before
  public void init() throws Exception {
    super.init();

    informationCenter = new InformationCenterImpl();
    informationCenter.setMigrationRuleStore(migrationRuleStore);
    informationCenter.setStoragePoolStore(storagePoolStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setOperationStore(operationStore);
    List<MigrationRuleInformation> migrationSpeedInformationList = new ArrayList<>();
    when(migrationRuleStore.list()).thenReturn(migrationSpeedInformationList);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for create MigrationRule. In the case, new rules should save to db.
   */
  @Test
  public void testListMigrateSpeed() throws InvalidInputExceptionThrift, TException {
    ListMigrationRulesRequest request = new ListMigrationRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);

    informationCenter.listMigrationRules(request);
    Mockito.verify(migrationRuleStore, Mockito.times(1)).list();
  }

}

