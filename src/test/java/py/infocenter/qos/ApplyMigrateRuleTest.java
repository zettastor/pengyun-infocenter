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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
import py.icshare.qos.CheckSecondaryInactiveThresholdMode;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.informationcenter.StoragePoolStrategy;
import py.instance.InstanceStatus;
import py.io.qos.MigrationRuleStatus;
import py.io.qos.MigrationStrategy;
import py.test.TestBase;
import py.thrift.share.ApplyMigrationRulesRequest;
import py.thrift.share.GetAppliedStoragePoolsRequest;
import py.thrift.share.GetAppliedStoragePoolsResponse;

public class ApplyMigrateRuleTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ApplyMigrateRuleTest.class);

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


  public static MigrationRuleInformation buildMigrateRuleInformation(long ruleId,
      MigrationRuleStatus status) {
    MigrationRuleInformation migrationSpeedInformation = new MigrationRuleInformation();
    migrationSpeedInformation.setRuleId(ruleId);
    migrationSpeedInformation.setMigrationRuleName("rule");
    migrationSpeedInformation.setStatus(status.toString());
    migrationSpeedInformation.setMigrationStrategy(MigrationStrategy.Smart.name());
    migrationSpeedInformation.setMaxMigrationSpeed(100);

    migrationSpeedInformation.setStartTime(0L);
    migrationSpeedInformation.setEndTime(0L);
    migrationSpeedInformation.setWaitTime(0L);
    migrationSpeedInformation.setCheckSecondaryInactiveThresholdMode(
        CheckSecondaryInactiveThresholdMode.RelativeTime.name());
    migrationSpeedInformation.setIgnoreMissPagesAndLogs(false);
    return migrationSpeedInformation;
  }



  @Before
  public void init() throws Exception {
    super.init();

    informationCenter = new InformationCenterImpl();

    informationCenter.setMigrationRuleStore(migrationRuleStore);
    informationCenter.setStoragePoolStore(storagePoolStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setOperationStore(operationStore);
    List<MigrationRuleInformation> migrationSpeedInformationList =
        new ArrayList<MigrationRuleInformation>();
    when(migrationRuleStore.list()).thenReturn(migrationSpeedInformationList);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for Apply MigrationRule.In the case, new rules id should save to related storage db.
   */
  @Test
  public void testApplyMigrateSpeed() throws Exception {
    List<Long> poolIds = new ArrayList<>();
    poolIds.add(1L);

    ApplyMigrationRulesRequest request = new ApplyMigrationRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setRuleId(1L);
    request.setStoragePoolIds(poolIds);
    request.setCommit(true);

    when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

    List<StoragePool> pools = buildStoragePoolList();
    when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
    when(migrationRuleStore.get(eq(1L)))
        .thenReturn(buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

    informationCenter.applyMigrationRules(request);

    Mockito.verify(storagePoolStore, Mockito.times(1)).saveStoragePool(any(StoragePool.class));
  }

  @Test
  public void testGetAppliedMigrateSpeed() throws Exception {
    GetAppliedStoragePoolsRequest request = new GetAppliedStoragePoolsRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setRuleId(1L);

    when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

    List<StoragePool> pools = buildStoragePoolList();
    when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
    when(migrationRuleStore.get(eq(1L)))
        .thenReturn(buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

    GetAppliedStoragePoolsResponse response = informationCenter.getAppliedStoragePools(request);

    // null list
    assertEquals(0, response.getStoragePoolList().size());
  }

  @Test
  public void testGetAppliedMigrateSpeedOk() throws Exception {
    GetAppliedStoragePoolsRequest request = new GetAppliedStoragePoolsRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setRuleId(2L);

    when(storagePoolStore.getStoragePool(eq(1L))).thenReturn(buildStoragePool(1L));

    List<StoragePool> pools = buildStoragePoolList();
    when(storagePoolStore.listAllStoragePools()).thenReturn(pools);
    when(migrationRuleStore.get(eq(2L)))
        .thenReturn(buildMigrateRuleInformation(1L, MigrationRuleStatus.AVAILABLE));

    GetAppliedStoragePoolsResponse response = informationCenter.getAppliedStoragePools(request);

    assertNotNull(response.getStoragePoolList());
    assertEquals(1, response.getStoragePoolList().size());
  }

  private List<StoragePool> buildStoragePoolList() {
    final List<StoragePool> pools = new ArrayList<>();
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
    storagePool1.setMigrationRuleId(3L);
    storagePool1.setStrategy(StoragePoolStrategy.Performance);

    StoragePool storagePool2 = new StoragePool();
    storagePool2.setDomainId(domainId);
    storagePool2.setPoolId(storagePoolId2);
    storagePool2.setName(storagePoolName2);
    storagePool2.setMigrationRuleId(2L);
    storagePool2.setStrategy(StoragePoolStrategy.Performance);

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
    storagePool.setStrategy(StoragePoolStrategy.Performance);
    return storagePool;
  }


}

