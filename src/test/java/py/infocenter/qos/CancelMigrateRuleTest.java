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

package py.infocenter.qos;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.icshare.Domain;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.io.qos.MigrationRuleStatus;
import py.test.TestBase;
import py.thrift.share.CancelMigrationRulesRequest;


public class CancelMigrateRuleTest extends TestBase {

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
   * A test for cancel MigrationRule.
   */
  @Test
  public void testCancelMigrateSpeed() throws Exception {
    CancelMigrationRulesRequest request = new CancelMigrationRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.addToStoragePoolIds(1L);
    request.setRuleId(1L);
    request.setCommit(true);

    when(migrationRuleStore.get(eq(1L))).thenReturn(
        ApplyMigrateRuleTest.buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

    when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

    informationCenter.cancelMigrationRules(request);

    Mockito.verify(storagePoolStore, Mockito.times(1)).saveStoragePool(any(StoragePool.class));
  }


  List<StoragePool> buildStoragePoolList() {
    final List<StoragePool> pools = new ArrayList<StoragePool>();
    Long domainId = RequestIdBuilder.get();
    Long storagePoolId1 = RequestIdBuilder.get();
    final Long storagePoolId2 = RequestIdBuilder.get();
    final String storagePoolName1 = "STORAGEPOOLNAME1";
    final String storagePoolName2 = "STORAGEPOOLNAME2";
    Domain domain = mock(Domain.class);
    when(domain.getDomainId()).thenReturn(domainId);
    StoragePool storagePool1 = new StoragePool();
    storagePool1.setDomainId(domainId);
    storagePool1.setPoolId(storagePoolId1);
    storagePool1.setName(storagePoolName1);
    storagePool1.setMigrationRuleId(1L);
    StoragePool storagePool2 = new StoragePool();
    storagePool2.setDomainId(domainId);
    storagePool2.setPoolId(storagePoolId2);
    storagePool2.setName(storagePoolName2);
    storagePool2.setMigrationRuleId(2L);

    pools.add(storagePool1);
    pools.add(storagePool2);

    return pools;
  }

  private StoragePool buildStoragePool(long ruleId) {
    Long domainId = RequestIdBuilder.get();
    String storagePoolName1 = "STORAGEPOOLNAME1";
    StoragePool storagePool = new StoragePool();
    storagePool.setDomainId(domainId);
    storagePool.setPoolId(ruleId);
    storagePool.setName(storagePoolName1);
    storagePool.setMigrationRuleId(1L);

    return storagePool;
  }


}

