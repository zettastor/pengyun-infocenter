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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeInAction.NULL;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.Assert;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.RequestResponseHelper;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.icshare.AccessRuleInformation;
import py.icshare.AccountMetadata;
import py.icshare.ArchiveInformation;
import py.icshare.CapacityRecord;
import py.icshare.CapacityRecordDbStore;
import py.icshare.CapacityRecordStore;
import py.icshare.DiskInfo;
import py.icshare.Domain;
import py.icshare.DomainDbStore;
import py.icshare.DomainStore;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;
import py.icshare.DriverInformation;
import py.icshare.DriverKey;
import py.icshare.InstanceMetadata;
import py.icshare.RecoverDbSentry;
import py.icshare.RecoverDbSentryStore;
import py.icshare.ServerNode;
import py.icshare.StorageInformation;
import py.icshare.TotalAndUsedCapacity;
import py.icshare.VolumeDeleteDelayInformation;
import py.icshare.VolumeRecycleInformation;
import py.icshare.VolumeRuleRelationshipInformation;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.InMemoryAccountStoreImpl;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.RoleStore;
import py.icshare.exception.AccessDeniedException;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.RebalanceRule;
import py.icshare.qos.RebalanceRuleInformation;
import py.icshare.qos.RebalanceRuleStore;
import py.infocenter.dbmanager.BackupDbManager;
import py.infocenter.dbmanager.BackupDbManagerImpl;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.ArchiveStore;
import py.infocenter.store.DiskInfoStore;
import py.infocenter.store.DriverClientDbStore;
import py.infocenter.store.DriverClientStore;
import py.infocenter.store.DriverDbStore;
import py.infocenter.store.DriverStore;
import py.infocenter.store.InstanceVolumesInformation;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.SegmentStore;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.StorageDbStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.test.base.InformationCenterAppConfigTest;
import py.informationcenter.AccessPermissionType;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolDbStore;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationEntry;
import py.io.qos.IoLimitationStatus;
import py.io.qos.IoLimitationStore;
import py.io.qos.MigrationRuleStatus;
import py.io.qos.RebalanceAbsoluteTime;
import py.io.qos.RebalanceRelativeTime;
import py.monitor.common.DiskInfoLightStatus;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.DomainThrift;
import py.thrift.share.GetDbInfoRequestThrift;
import py.thrift.share.GetDbInfoResponseThrift;
import py.thrift.share.IoLimitationEntryThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.PoolAlreadyAppliedRebalanceRuleExceptionThrift;
import py.thrift.share.RebalanceRuleNotExistExceptionThrift;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class DatabaseStoreTest extends TestBase {

  private static final int amountOfAccounts = 1000;

  StorageDbStore dbStore = null;
  StorageStore storageStore = null;
  DriverDbStore driverDbStore = null;
  DriverStore driverStore = null;
  ArchiveStore archiveStore = null;
  SegmentStore segmentStore = null;
  VolumeStore volumeStore = null;
  VolumeRuleRelationshipStore volumeRuleRelationshipStore = null;
  AccessRuleStore accessRuleStore = null;
  IscsiRuleRelationshipStore iscsiRuleRelationshipStore = null;
  IscsiAccessRuleStore iscsiAccessRuleStore = null;
  DomainDbStore domainDbStore = null;
  DomainStore domainStore = null;
  StoragePoolDbStore storagePoolDbStore = null;
  StoragePoolStore storagePoolStore = null;
  CapacityRecordDbStore capacityRecordDbStore = null;
  CapacityRecordStore capacityRecordStore = null;
  AccountStore accountStore = null;
  RoleStore roleStore = null;
  ApiStore apiStore = null;
  ResourceStore resourceStore = null;
  ServerNodeStore serverNodeStore = null;
  DiskInfoStore diskInfoStore = null;
  IoLimitationStore ioLimitationStore = null;
  MigrationRuleStore migrationRuleStore = null;
  RebalanceRuleStore rebalanceRuleStore = null;
  InstanceVolumesInformationStore instanceVolumesInformationStore = null;
  VolumeJobStoreDb volumeJobStoreDb = null;
  VolumeDelayStore volumeDelayStore = null;
  VolumeRecycleStore volumeRecycleStore = null;
  DriverClientStore driverClientStore = null;
  DriverClientDbStore driverClientDbStore = null;
  RecoverDbSentryStore recoverDbSentryStore = null;

  private boolean switchStoreTools;


  public void init() throws Exception {
    super.init();
    @SuppressWarnings("resource")
    ApplicationContext ctx = new AnnotationConfigApplicationContext(
        InformationCenterAppConfigTest.class);
    dbStore = (StorageDbStore) ctx.getBean("dbStorageStore");
    storageStore = (StorageStore) ctx.getBean("dbStorageStore1");
    driverDbStore = (DriverDbStore) ctx.getBean("dbDriverStore");
    driverStore = (DriverStore) ctx.getBean("driverStore");
    archiveStore = (ArchiveStore) ctx.getBean("dbArchiveStore");
    segmentStore = (SegmentStore) ctx.getBean("dbSegmentStore");
    volumeRuleRelationshipStore = (VolumeRuleRelationshipStore) ctx
        .getBean("dbVolumeRuleRelationshipStore");
    accessRuleStore = (AccessRuleStore) ctx.getBean("dbAccessRuleStore");
    iscsiRuleRelationshipStore = (IscsiRuleRelationshipStore) ctx
        .getBean("dbIscsiRuleRelationshipStore");
    iscsiAccessRuleStore = (IscsiAccessRuleStore) ctx.getBean("dbIscsiAccessRuleStore");
    volumeStore = (VolumeStore) ctx.getBean("dbVolumeStore");
    domainDbStore = (DomainDbStore) ctx.getBean("dbDomainStore");
    domainStore = (DomainStore) ctx.getBean("domainStore");
    storagePoolDbStore = (StoragePoolDbStore) ctx.getBean("dbStoragePoolStore");
    storagePoolStore = (StoragePoolStore) ctx.getBean("storagePoolStore");
    capacityRecordDbStore = (CapacityRecordDbStore) ctx.getBean("dbCapacityRecordStore");
    capacityRecordStore = (CapacityRecordStore) ctx.getBean("capacityRecordStore");
    accountStore = (InMemoryAccountStoreImpl) ctx.getBean("inMemAccountStore");
    roleStore = (RoleStore) ctx.getBean("roleStore");
    apiStore = (ApiStore) ctx.getBean("apiDbStore");
    resourceStore = (ResourceStore) ctx.getBean("resourceDbStore");
    serverNodeStore = (ServerNodeStore) ctx.getBean("serverNodeStore");
    diskInfoStore = (DiskInfoStore) ctx.getBean("diskInfoStore");
    ioLimitationStore = (IoLimitationStore) ctx.getBean("ioLimitationStore");
    migrationRuleStore = (MigrationRuleStore) ctx.getBean("migrationRuleStore");
    rebalanceRuleStore = (RebalanceRuleStore) ctx.getBean("rebalanceRuleStore");
    instanceVolumesInformationStore = (InstanceVolumesInformationStore) ctx
        .getBean("instanceVolumesInformationStore");

    volumeJobStoreDb = (VolumeJobStoreDb) ctx.getBean("volumeJobStoreDb");
    volumeDelayStore = (VolumeDelayStore) ctx.getBean("volumeDelayStore");
    volumeRecycleStore = (VolumeRecycleStore) ctx.getBean("volumeRecycleStore");
    driverClientStore = (DriverClientStore) ctx.getBean("driverClientStore");
    driverClientDbStore = (DriverClientDbStore) ctx.getBean("driverClientDbStore");
    recoverDbSentryStore = (RecoverDbSentryStore) ctx.getBean("recoverDbSentryStore");
    //db
    switchStoreTools = true;
  }


  @Before
  public void beforeTest() throws Exception {
    List<StoragePool> storagePools = storagePoolStore.listAllStoragePools();
    if (storagePools != null && !storagePools.isEmpty()) {
      storagePools.forEach(storage -> storagePoolStore.deleteStoragePool(storage.getPoolId()));
    }
    assertTrue(storagePoolStore.listAllStoragePools().isEmpty());

    List<StorageInformation> storages = dbStore.listFromDb();
    if (storages != null && !storages.isEmpty()) {
      storages.forEach(storage -> dbStore.deleteFromDb(storage.getInstanceId()));
    }
    storageStore.clearMemoryData();
    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    if (instanceMetadataList != null && !instanceMetadataList.isEmpty()) {
      instanceMetadataList.forEach(
          instanceMetadata -> storageStore.delete(instanceMetadata.getInstanceId().getId()));
    }
    assertTrue(dbStore.listFromDb().isEmpty());

    List<Domain> domains = domainStore.listAllDomains();
    if (domains != null && !domains.isEmpty()) {
      domains.forEach(domain -> domainStore.deleteDomain(domain.getDomainId()));
    }
    domainStore.clearMemoryMap();
    assertTrue(domainStore.listAllDomains().isEmpty());

    List<DriverInformation> drivers = driverDbStore.listFromDb();
    if (drivers != null && !drivers.isEmpty()) {
      drivers
          .forEach(driver -> driverDbStore.deleteFromDb(driver.getDriverKeyInfo().getVolumeId()));
    }
    assertTrue(driverDbStore.listFromDb().isEmpty());

    driverStore.clearMemoryData();
    assertTrue(driverStore.list().isEmpty());

    List<VolumeRuleRelationshipInformation> volumeRuleRelationshipInformationList =
        volumeRuleRelationshipStore.list();
    if (null != volumeRuleRelationshipInformationList && !volumeRuleRelationshipInformationList
        .isEmpty()) {
      volumeRuleRelationshipInformationList
          .forEach(volumeRule -> volumeRuleRelationshipStore.deleteByRuleIdandVolumeId(
              volumeRule.getVolumeId(), volumeRule.getRuleId()
          ));
    }
    assertTrue(volumeRuleRelationshipStore.list().isEmpty());

    List<AccessRuleInformation> accessRuleInformationList = accessRuleStore.list();
    if (null != accessRuleInformationList && !accessRuleInformationList.isEmpty()) {
      accessRuleInformationList.forEach(
          accessRuleInformation -> accessRuleStore.delete(accessRuleInformation.getRuleId()));
    }
    assertTrue(accessRuleStore.list().isEmpty());

    List<IscsiRuleRelationshipInformation> iscsiRuleRelationshipInformationList =
        iscsiRuleRelationshipStore.list();
    if (null != iscsiRuleRelationshipInformationList && !iscsiRuleRelationshipInformationList
        .isEmpty()) {
      iscsiRuleRelationshipInformationList
          .forEach(iscsiRule -> iscsiRuleRelationshipStore.deleteByRuleIdandDriverKey(

              new DriverKey(iscsiRule.getDriverContainerId(), iscsiRule.getVolumeId(),
                  iscsiRule.getSnapshotId(),
                  DriverType.valueOf(iscsiRule.getDriverType())), iscsiRule.getRuleId()
          ));
    }
    assertTrue(iscsiRuleRelationshipStore.list().isEmpty());

    List<IscsiAccessRuleInformation> iscsiAccessRuleInformationList = iscsiAccessRuleStore.list();
    if (null != iscsiAccessRuleInformationList && !iscsiAccessRuleInformationList.isEmpty()) {
      iscsiAccessRuleInformationList.forEach(iscsiAccessRuleInformation -> iscsiAccessRuleStore
          .delete(iscsiAccessRuleInformation.getRuleId()));
    }
    assertTrue(iscsiAccessRuleStore.list().isEmpty());

    accountStore.deleteAllAccounts();
    assertTrue(accountStore.listAccounts().isEmpty());

    roleStore.cleanRoles();
    assertTrue(roleStore.listRoles().isEmpty());

    apiStore.cleanApis();
    assertTrue(apiStore.listApis().isEmpty());

    resourceStore.cleanResources();
    assertTrue(resourceStore.listResources().isEmpty());

    serverNodeStore.clearDb();
    assertTrue(serverNodeStore.listAllServerNodes().isEmpty());

    diskInfoStore.clearDb();
    assertTrue(diskInfoStore.listDiskInfos().isEmpty());

    List<IoLimitation> ioLimitationList = ioLimitationStore.list();
    if (null != ioLimitationList && !ioLimitationList.isEmpty()) {
      ioLimitationList.forEach(ioLimitation -> ioLimitationStore.delete(ioLimitation.getId()));
    }
    assertTrue(ioLimitationStore.list().isEmpty());

    List<MigrationRuleInformation> migrationSpeedInformationList = migrationRuleStore.list();
    if (null != migrationSpeedInformationList && !migrationSpeedInformationList.isEmpty()) {
      migrationSpeedInformationList.forEach(migrationSpeedInformation -> migrationRuleStore
          .delete(migrationSpeedInformation.getRuleId()));
    }
    assertTrue(migrationRuleStore.list().isEmpty());

    List<RebalanceRuleInformation> rebalanceInformationList = rebalanceRuleStore.list();
    if (null != rebalanceInformationList && !rebalanceInformationList.isEmpty()) {
      rebalanceInformationList
          .forEach(rebalanceRule -> rebalanceRuleStore.delete(rebalanceRule.getRuleId()));
    }
    assertTrue(rebalanceRuleStore.list().isEmpty());

    //instanceVolumesInformationStore
    List<InstanceVolumesInformation> instanceVolumesInformationList =
        instanceVolumesInformationStore.reloadAllInstanceVolumesInformationFromDb();

    for (InstanceVolumesInformation instanceVolumesInformation : instanceVolumesInformationList) {
      instanceVolumesInformationStore
          .deleteInstanceVolumesInformationFromDb(instanceVolumesInformation.getInstanceId());
    }

    //volumeDelayStore
    volumeDelayStore.clearAll();
    volumeRecycleStore.clearAll();

    driverClientStore.clearMemoryData();
    assertTrue(driverClientStore.list().isEmpty());

    List<DriverClientInformation> driverClientInformationList = driverClientDbStore.listFromDb();
    if (driverClientInformationList != null && !driverClientInformationList.isEmpty()) {
      for (DriverClientInformation driverClientInformation : driverClientInformationList) {
        drivers.forEach(driver -> driverClientDbStore.deleteFromDb(driverClientInformation));
      }
    }
    assertTrue(driverClientDbStore.listFromDb().isEmpty());

    recoverDbSentryStore.clearAll();
  }

  @Test
  public void testAccountStore() {
    //save
    AccountMetadata accountMetadata = new AccountMetadata();
    accountMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    accountMetadata.setAccountName("admin");
    accountMetadata.setHashedPassword("admin");
    accountStore.saveAccount(accountMetadata);

    //
    AccountMetadata accountMetadataGet = accountStore.getAccount("admin");
    assertTrue(accountMetadataGet.getHashedPassword().equals("admin"));

    accountMetadataGet = accountStore.getAccount("Admin");
    assertTrue(accountMetadataGet == null);
  }

  @Test
  public void testVolumeDelayStore() {
    long volumeId = 11111L;

    VolumeDeleteDelayInformation volumeDeleteDelayInformation = new VolumeDeleteDelayInformation();
    volumeDeleteDelayInformation.setVolumeId(volumeId);
    volumeDeleteDelayInformation.setStopDelay(false);
    volumeDeleteDelayInformation.setTimeForDelay(123456);

    //save value
    volumeDelayStore.saveVolumeDelayInfo(volumeDeleteDelayInformation);

    //get value
    VolumeDeleteDelayInformation volumeDelayInfoGet = volumeDelayStore.getVolumeDelayInfo(volumeId);
    Assert.assertEquals(volumeDelayInfoGet.getTimeForDelay(), 123456);
    Assert.assertEquals(volumeDelayInfoGet.isStopDelay(), false);

    //update
    volumeDelayStore.updateVolumeDelayStatusInfo(volumeId, true);
    volumeDelayStore.updateVolumeDelayTimeInfo(volumeId, 23456);

    //get to check
    VolumeDeleteDelayInformation volumeDelayInfoGet2 = volumeDelayStore
        .getVolumeDelayInfo(volumeId);
    Assert.assertEquals(volumeDelayInfoGet2.getTimeForDelay(), 23456);
    Assert.assertEquals(volumeDelayInfoGet2.isStopDelay(), true);

    //list
    List<VolumeDeleteDelayInformation> volumeDeleteDelayInformationList = volumeDelayStore
        .listVolumesDelayInfo();
    Assert.assertEquals(volumeDeleteDelayInformationList.size(), 1);
    for (VolumeDeleteDelayInformation volumeDeleteDelayInformation1 :
        volumeDeleteDelayInformationList) {
      Assert.assertEquals(volumeDeleteDelayInformation1.getTimeForDelay(), 23456);
      Assert.assertEquals(volumeDeleteDelayInformation1.isStopDelay(), true);
    }

    //delete
    volumeDelayStore.deleteVolumeDelayInfo(volumeId);
    //list to check
    volumeDeleteDelayInformationList = volumeDelayStore.listVolumesDelayInfo();
    Assert.assertEquals(volumeDeleteDelayInformationList.isEmpty(), true);
  }

  @Test
  public void testVolumeRecycleStore() {
    long volumeId = 11111L;

    VolumeRecycleInformation volumeRecycleInformation = new VolumeRecycleInformation();
    volumeRecycleInformation.setVolumeId(volumeId);
    volumeRecycleInformation.setTimeInRecycle(123456);

    //save value
    volumeRecycleStore.saveVolumeRecycleInfo(volumeRecycleInformation);

    //get value
    VolumeRecycleInformation volumeRecycleInfoGet = volumeRecycleStore
        .getVolumeRecycleInfo(volumeId);
    Assert.assertEquals(volumeRecycleInfoGet.getTimeInRecycle(), 123456);

    //list
    List<VolumeRecycleInformation> volumeRecycleInformationList = volumeRecycleStore
        .listVolumesRecycleInfo();
    Assert.assertEquals(volumeRecycleInformationList.size(), 1);
    for (VolumeRecycleInformation volumeRecycleInformation1 : volumeRecycleInformationList) {
      Assert.assertEquals(volumeRecycleInfoGet.getTimeInRecycle(), 123456);
    }

    //delete
    volumeRecycleStore.deleteVolumeRecycleInfo(volumeId);
    //list to check
    volumeRecycleInformationList = volumeRecycleStore.listVolumesRecycleInfo();
    Assert.assertEquals(volumeRecycleInformationList.isEmpty(), true);
  }

  @Test
  public void testInstanceVolumesInformationStore() throws IOException, SQLException {
    long instanceId = 1;
    long volumeId = 11111L;

    InstanceVolumesInformation instanceVolumesInformation = new InstanceVolumesInformation();
    Set<Long> testSet = new HashSet<>();
    testSet.add(volumeId);
    instanceVolumesInformation.setInstanceId(instanceId);
    instanceVolumesInformation.setVolumeIds(testSet);
    logger.warn("------ :{}", instanceVolumesInformationStore);

    InstanceVolumesInformation instanceVolumesInformationGet;

    //save value
    instanceVolumesInformationStore.saveInstanceVolumesInformationToDb(instanceVolumesInformation);

    //get value
    instanceVolumesInformationGet = instanceVolumesInformationStore
        .getInstanceVolumesInformationFromDb(instanceId);

    Assert.assertEquals(instanceVolumesInformationGet.getInstanceId(), instanceId);
    Assert.assertEquals(instanceVolumesInformationGet.getVolumeIds().contains(volumeId), true);

    //load
    List<InstanceVolumesInformation> instanceVolumesInformationList =
        instanceVolumesInformationStore.reloadAllInstanceVolumesInformationFromDb();

    for (InstanceVolumesInformation value : instanceVolumesInformationList) {
      Assert.assertEquals(value.getInstanceId(), instanceId);
      Assert.assertEquals(value.getVolumeIds().contains(volumeId), true);
    }

    Assert.assertEquals(instanceVolumesInformationList.size(), 1);

    //delete
    instanceVolumesInformationStore.deleteInstanceVolumesInformationFromDb(instanceId);

    List<InstanceVolumesInformation> instanceVolumesInformations =
        instanceVolumesInformationStore.reloadAllInstanceVolumesInformationFromDb();

    Assert.assertEquals(instanceVolumesInformations.isEmpty(), true);

  }

  @Test
  public void testDriverClientDbStore() {
    //save
    long driverContainerId = 111;
    long volumeId1 = RequestIdBuilder.get();
    String volumeName = "check";
    String volumeDescription = "check_volume";
    DriverClientKey driverClientKey = new DriverClientKey(driverContainerId, volumeId1, 0,
        DriverType.ISCSI, "10.0.0.80");
    DriverClientInformation driverClientInformation = new DriverClientInformation(driverClientKey,
        123456, "driverNameTest",
        "hostNameTest", true, volumeName, volumeDescription);
    driverClientDbStore.saveToDb(driverClientInformation);

    long driverContainerId1 = 222;
    DriverClientKey driverClientKey2 = new DriverClientKey(driverContainerId1, volumeId1, 0,
        DriverType.ISCSI, "10.0.0.81");
    DriverClientInformation driverClientInformation1 = new DriverClientInformation(driverClientKey2,
        123456, "driverNameTest1", "hostNameTest1",
        true, volumeName, volumeDescription);
    driverClientDbStore.saveToDb(driverClientInformation1);

    //getByVolumeIdFromDb
    List<DriverClientInformation> driverClientInformationList = driverClientDbStore
        .getByVolumeIdFromDb(volumeId1);
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 2);

    //getByDriverKeyFromDb
    driverClientInformationList = driverClientDbStore.getByDriverKeyFromDb(driverClientKey);
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 1);
    org.junit.Assert
        .assertTrue(driverClientInformationList.get(0).getDriverName().equals("driverNameTest"));
    org.junit.Assert.assertTrue(
        driverClientInformationList.get(0).getVolumeDescription().equals(volumeDescription));

    //getByDriverContainerIdFromDb(long driverContainerId);
    driverClientInformationList = driverClientDbStore
        .getByDriverContainerIdFromDb(driverContainerId);
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 1);
    org.junit.Assert
        .assertTrue(driverClientInformationList.get(0).getDriverName().equals("driverNameTest"));

    //List<DriverClientInformation> listFromDb();
    driverClientInformationList = driverClientDbStore.listFromDb();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 2);

    //deleteFromDb(DriverClientInformation driverClientInformation);
    driverClientDbStore.deleteFromDb(driverClientInformation);
    //list  to check
    driverClientInformationList = driverClientDbStore.listFromDb();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 1);
    org.junit.Assert
        .assertTrue(driverClientInformationList.get(0).getDriverName().equals("driverNameTest1"));

    //deleteFromDb(DriverClientKey driverClientKey);
    driverClientDbStore.saveToDb(driverClientInformation);
    driverClientDbStore.deleteFromDb(driverClientKey);
    //list  to check
    driverClientInformationList = driverClientDbStore.listFromDb();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 1);
    org.junit.Assert
        .assertTrue(driverClientInformationList.get(0).getDriverName().equals("driverNameTest1"));

    //deleteFromDb(long volumeId, DriverType driverType, int snapshotId, long driverContainerId);
    driverClientDbStore.saveToDb(driverClientInformation);
    driverClientDbStore.deleteFromDb(driverClientKey.getVolumeId(), driverClientKey.getDriverType(),
        driverClientKey.getSnapshotId(), driverClientKey.getDriverContainerId());
    //list  to check
    driverClientInformationList = driverClientDbStore.listFromDb();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 1);
    org.junit.Assert
        .assertTrue(driverClientInformationList.get(0).getDriverName().equals("driverNameTest1"));

    //deleteFromDb(long volumeId);
    driverClientDbStore.saveToDb(driverClientInformation);
    driverClientDbStore.deleteFromDb(volumeId1);
    //list  to check
    driverClientInformationList = driverClientDbStore.listFromDb();
    org.junit.Assert.assertTrue(driverClientInformationList.isEmpty());
  }

  @Test
  public void testDriverClientInfoStore() {
    //save
    long driverContainerId = 111;
    long volumeId1 = RequestIdBuilder.get();
    String volumeName = "check";
    String volumeDescription = "check_volume";
    DriverClientKey driverClientKey = new DriverClientKey(driverContainerId, volumeId1, 0,
        DriverType.ISCSI, "10.0.0.80");
    DriverClientInformation driverClientInformation = new DriverClientInformation(driverClientKey,
        123456, "driverNameTest", "hostNameTest",
        true, volumeName, volumeDescription);
    driverClientStore.save(driverClientInformation);

    long driverContainerId1 = 222;
    DriverClientKey driverClientKey2 = new DriverClientKey(driverContainerId1, volumeId1, 0,
        DriverType.ISCSI, "10.0.0.81");
    DriverClientInformation driverClientInformation2 = new DriverClientInformation(driverClientKey2,
        123456, "driverNameTest1", "hostNameTest1",
        true, volumeName, volumeDescription);
    driverClientStore.save(driverClientInformation2);

    DriverClientInformation driverClientInformation3 = new DriverClientInformation(driverClientKey2,
        223456, "driverNameTest1", "hostNameTest1",
        false, volumeName, volumeDescription);
    driverClientStore.save(driverClientInformation3);

    //getLastTimeValue
    DriverClientInformation driverClientInformationGet = driverClientStore
        .getLastTimeValue(driverClientKey2);
    org.junit.Assert.assertTrue(!driverClientInformationGet.isStatus());
    org.junit.Assert
        .assertTrue(driverClientInformationGet.getVolumeDescription().equals(volumeDescription));

    //list
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 3);

    //deleteValue(DriverClientInformation driverClientInformation);
    driverClientStore.deleteValue(driverClientInformation3);
    //get the check
    driverClientInformationGet = driverClientStore.getLastTimeValue(driverClientKey2);
    org.junit.Assert.assertTrue(driverClientInformationGet.isStatus());
    //db check
    driverClientInformationList = driverClientDbStore.listFromDb();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 2);

    //delete(DriverClientKey driverClientKey);s
    driverClientStore.delete(driverClientKey);
    //list  to check
    driverClientInformationList = driverClientStore.list();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 1);
    org.junit.Assert
        .assertTrue(driverClientInformationList.get(0).getDriverName().equals("driverNameTest1"));

    //deleteFromDb(long volumeId);
    driverClientStore.clearMemoryData();
    //list  to check
    driverClientInformationList = driverClientStore.list();
    for (DriverClientInformation driverClientInformation1 : driverClientInformationList) {
      driverClientStore.deleteValue(driverClientInformation1);
    }
    driverClientInformationList = driverClientStore.list();
    org.junit.Assert.assertTrue(driverClientInformationList.isEmpty());
  }

  @Test
  public void testDriverTable() {

    DriverInformation driverInformation;

    // write a record
    long driverContainerId = RequestIdBuilder.get();
    long volumeId1 = RequestIdBuilder.get();
    driverInformation = new DriverInformation(driverContainerId, volumeId1, 0, DriverType.ISCSI);
    driverInformation.setDriverStatus(DriverStatus.START.name());
    driverDbStore.saveToDb(driverInformation);

    // write a next record
    long driverContainerId1 = RequestIdBuilder.get();
    driverInformation = new DriverInformation(driverContainerId1, volumeId1, 0, DriverType.NBD);
    driverInformation.setDriverStatus(DriverStatus.START.name());
    driverDbStore.saveToDb(driverInformation);

    long volumeId2 = RequestIdBuilder.get();
    driverInformation = new DriverInformation(driverContainerId, volumeId2, 0, DriverType.ISCSI);
    driverInformation.setDriverStatus(DriverStatus.START.name());
    driverInformation.setCreateTime(123456);
    driverDbStore.saveToDb(driverInformation);

    assertEquals(2, driverDbStore.getByVolumeIdFromDb(volumeId1).size());
    assertEquals(1, driverDbStore.getByVolumeIdFromDb(volumeId2).size());
    logger.warn("get the value:{}",
        driverDbStore.getByVolumeIdFromDb(volumeId2).get(0).getCreateTime());
    assertEquals(123456, driverDbStore.getByVolumeIdFromDb(volumeId2).get(0).getCreateTime());

    driverInformation.setDriverStatus(DriverStatus.LAUNCHING.name());
    driverDbStore.updateToDb(driverInformation);
    driverDbStore.updateStatusToDb(volumeId1, DriverType.ISCSI, 0, DriverStatus.LAUNCHING.name());
    driverDbStore.updateStatusToDb(volumeId1, DriverType.NBD, 0, DriverStatus.LAUNCHING.name());
    List<DriverInformation> listDrivers = driverDbStore.listFromDb();
    assertEquals(3, listDrivers.size());

    for (DriverInformation driver : listDrivers) {
      logger.warn("current driver: {}", driver);
      assertTrue(driver.getDriverStatus().equals(DriverStatus.LAUNCHING.name()));
    }

    driverDbStore
        .updateMakeUnmountDriverForCsiToDb(driverContainerId, volumeId1, DriverType.ISCSI, 0, true);
    //get the check
    listDrivers = driverDbStore.getByDriverKeyFromDb(volumeId1, DriverType.ISCSI, 0);
    assertEquals(1, listDrivers.size());
    assertEquals(true, listDrivers.get(0).isMakeUnmountForCsi());

    // delete a record by ip+port
    assertEquals(2, driverDbStore.getByVolumeIdFromDb(volumeId1).size());
    driverDbStore.deleteFromDb(volumeId1, DriverType.NBD, 0);
    assertEquals(1, driverDbStore.getByVolumeIdFromDb(volumeId1).size());

    driverDbStore.deleteFromDb(volumeId1);
    assertEquals(1, driverDbStore.listFromDb().size());

    driverDbStore.deleteFromDb(volumeId2);
    assertTrue(driverDbStore.listFromDb().isEmpty());
  }


  @Test
  public void testDriverLimitationStore() {
    try {
      long volumeId = 1111;
      long accountId = 1862755152385798555L;
      long driverContainerId = RequestIdBuilder.get();
      DriverType driverType = DriverType.NBD;
      int snapshotId = 0;

      long limitId1 = 1;
      long limitId2 = 2;

      DriverMetadata driverMetadata = new DriverMetadata();
      driverMetadata.setVolumeId(volumeId);
      driverMetadata.setDriverContainerId(driverContainerId);
      driverMetadata.setSnapshotId(snapshotId);
      driverMetadata.setDriverType(driverType);
      driverMetadata.setAccountId(accountId);
      driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);

      IoLimitation limit1 = new IoLimitation(limitId1, "rule", 100, 10,
          1000, 100, LocalTime.now().plusSeconds(5),
          LocalTime.now().plusSeconds(15));

      IoLimitation limit2 = new IoLimitation(limitId2, "rule", 200, 20,
          2000, 200, LocalTime.now().plusSeconds(20),
          LocalTime.now().plusSeconds(25));

      List<IoLimitation> limitList = new ArrayList<>();
      limitList.add(limit1);
      limitList.add(limit2);
      driverMetadata.setDynamicIoLimitationId(100);
      driverMetadata.setStaticIoLimitationId(101);

      driverStore.save(driverMetadata);

      driverStore.updateIoLimit(driverContainerId, volumeId, driverType, snapshotId, limit1);
      driverStore.deleteIoLimit(driverContainerId, volumeId, driverType, snapshotId, limitId2);
      DriverMetadata driverMetadata1 = driverStore
          .get(driverContainerId, volumeId, driverType, snapshotId);


    } catch (Exception e) {
      logger.error("exception catch", e);
      Validate.isTrue(false);
    }
  }

  @Test
  public void testStorageDbTable() {

    StorageInformation storageMetadata;
    long instanceId1 = RequestIdBuilder.get();
    storageMetadata = new StorageInformation(instanceId1, 100, 50, 50, new Date(), 1L);

    storageMetadata.setSsdCacheSize(100000);
    storageMetadata.setSsdCacheStatus(1);
    storageMetadata.setTagKey("corperate");
    storageMetadata.setTagValue("pengyunnetwork");
    dbStore.saveToDb(storageMetadata);

    long instanceId2 = RequestIdBuilder.get();
    storageMetadata = new StorageInformation(instanceId2, 100, 50, 50, new Date(), 2L);
    storageMetadata.setSsdCacheStatus(1);
    dbStore.saveToDb(storageMetadata);

    long instanceId3 = RequestIdBuilder.get();
    storageMetadata = new StorageInformation(instanceId3, 100, 50, 50, new Date(), 3L);
    dbStore.saveToDb(storageMetadata);

    assertTrue(dbStore.getByInstanceIdFromDb(0) == null);
    assertTrue(dbStore.getByInstanceIdFromDb(instanceId1) != null);

    storageMetadata.setSsdCacheStatus(1);
    dbStore.saveToDb(storageMetadata);

    List<StorageInformation> storages = dbStore.listFromDb();
    assertTrue(storages.size() == 3);
    for (StorageInformation storage : storages) {
      assertTrue(storage.getSsdCacheStatus() == 1);
    }

    dbStore.deleteFromDb(instanceId1);
    assertTrue(dbStore.listFromDb().size() == 2);

    dbStore.deleteFromDb(instanceId2);
    assertTrue(dbStore.listFromDb().size() == 1);

    dbStore.deleteFromDb(instanceId3);
    assertTrue(dbStore.listFromDb().size() == 0);
  }

  // add by wzy @2014-10-08 13:45:53
  @Test
  public void testStorageTable() {

    InstanceMetadata instanceMetadata;
    long instanceId1 = RequestIdBuilder.get();
    instanceMetadata = new InstanceMetadata(new InstanceId(instanceId1));

    instanceMetadata.setCapacity(100000);
    instanceMetadata.setFreeSpace(1);
    instanceMetadata.setEndpoint("py123");
    instanceMetadata.setLastUpdated(199L);
    instanceMetadata.setDatanodeType(SIMPLE);

    // add archive list to instanceMetadata
    List<RawArchiveMetadata> archivesList = new ArrayList<>();
    for (long i = 0; i < 3; i++) {
      RawArchiveMetadata amdata = new RawArchiveMetadata();
      amdata.setArchiveId(i);
      amdata.setInstanceId(new InstanceId(instanceId1));
      amdata.setStatus(ArchiveStatus.GOOD);
      archivesList.add(amdata);
    }

    instanceMetadata.setArchives(archivesList);
    storageStore.save(instanceMetadata);

    List<InstanceMetadata> storages = storageStore.list();
    assertTrue(storages.size() == 1);
    storageStore.delete(instanceId1);

    assertTrue(storageStore.size() == 0);
  }

  @Test
  public void testArchiveTable() {
    long instanceId = 20000000;
    long archiveId = 10000000;
    ArchiveInformation archiveInformation;
    // no more archives in a instance
    archiveInformation = new ArchiveInformation(archiveId + 1, instanceId, StorageType.SATA,
        ArchiveStatus.GOOD,
        10000, 1L);
    archiveStore.save(archiveInformation);
    archiveInformation = new ArchiveInformation(archiveId + 2, instanceId, StorageType.SATA,
        ArchiveStatus.GOOD,
        10000, 2L);
    archiveStore.save(archiveInformation);
    archiveInformation = new ArchiveInformation(archiveId + 3, instanceId, StorageType.SATA,
        ArchiveStatus.GOOD,
        10000, 3L);
    archiveStore.save(archiveInformation);

    archiveInformation = new ArchiveInformation(archiveId + 4, instanceId + 1, StorageType.SAS,
        ArchiveStatus.GOOD,
        10000, 4L);
    archiveStore.save(archiveInformation);
    archiveInformation = new ArchiveInformation(archiveId + 5, instanceId + 1, StorageType.SAS,
        ArchiveStatus.GOOD,
        10000, 5L);
    archiveStore.save(archiveInformation);

    assertTrue(archiveStore.get(archiveId + 1).getInstanceId() == instanceId);
    assertTrue(archiveStore.get(archiveId + 4).getInstanceId() == instanceId + 1);

    assertTrue(archiveStore.getByInstanceId(instanceId + 1).size() == 2);
    assertTrue(archiveStore.getByInstanceId(instanceId).size() == 3);

    assertTrue(archiveStore.list().size() == 5);

    assertTrue(archiveStore.get(archiveId + 1).getInstanceId() == instanceId);
    assertTrue(archiveStore.get(archiveId + 7) == null);

    assertTrue(archiveStore.deleteByInstanceId(instanceId) == 3);
    assertTrue(archiveStore.list().size() == 2);

    assertTrue(archiveStore.delete(archiveId + 4) == 1);
    assertTrue(archiveStore.list().size() == 1);

    assertTrue(archiveStore.delete(archiveId + 5) == 1);
    assertTrue(archiveStore.list().size() == 0);
  }

  @Test
  public void testAccessRuleStore() {

    AccessRuleInformation accessRuleInformation;
    long ruleId = 10000000;

    accessRuleInformation = new AccessRuleInformation(ruleId, "10.0.1.101",
        AccessPermissionType.READ.getValue());
    accessRuleStore.save(accessRuleInformation);
    accessRuleInformation = new AccessRuleInformation(ruleId + 1, "10.0.1.102",
        AccessPermissionType.WRITE.getValue());
    accessRuleStore.save(accessRuleInformation);
    accessRuleInformation = new AccessRuleInformation(ruleId + 2, "10.0.1.103",
        AccessPermissionType.WRITE.getValue());
    accessRuleStore.save(accessRuleInformation);
    accessRuleInformation = new AccessRuleInformation(ruleId + 3, "10.0.1.104",
        AccessPermissionType.WRITE.getValue());
    accessRuleStore.save(accessRuleInformation);

    assertTrue(accessRuleStore.list().size() == 4);
    assertTrue(accessRuleStore.get(ruleId + 2).getIpAddress().equals("10.0.1.103"));
    accessRuleStore.delete(ruleId + 3);
    assertTrue(accessRuleStore.list().size() == 3);

    accessRuleInformation.setRuleId(ruleId);
    accessRuleInformation.setIpAddress("10.0.1.101");
    accessRuleInformation.permission(AccessPermissionType.WRITE);
    accessRuleStore.update(accessRuleInformation);

    for (AccessRuleInformation rule : accessRuleStore.list()) {
      assertTrue(rule.getPermission() == AccessPermissionType.WRITE.getValue());
    }

    accessRuleStore.delete(ruleId);
    accessRuleStore.delete(ruleId + 1);
    accessRuleStore.delete(ruleId + 2);
    assertTrue(accessRuleStore.list().size() == 0);

  }

  @Test
  public void testVolumeRuleRelationshipStore() {
    long ruleId = 10000000;
    long volumeId = 2000000;
    long relationshipId = 3000000;
    VolumeRuleRelationshipInformation relationship;
    relationship = new VolumeRuleRelationshipInformation(relationshipId, volumeId, ruleId);
    volumeRuleRelationshipStore.save(relationship);

    relationship = new VolumeRuleRelationshipInformation(relationshipId + 2, volumeId, ruleId + 1);
    volumeRuleRelationshipStore.save(relationship);

    relationship = new VolumeRuleRelationshipInformation(relationshipId + 3, volumeId + 1,
        ruleId + 3);
    volumeRuleRelationshipStore.save(relationship);

    relationship = new VolumeRuleRelationshipInformation(relationshipId + 4, volumeId + 2,
        ruleId + 1);
    volumeRuleRelationshipStore.save(relationship);

    assertTrue(volumeRuleRelationshipStore.list().size() == 4);
    assertTrue(volumeRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
    assertTrue(volumeRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
    assertTrue(volumeRuleRelationshipStore.getByVolumeId(volumeId).size() == 2);
    assertTrue(volumeRuleRelationshipStore.deleteByRuleId(ruleId + 1) == 2);
    assertTrue(volumeRuleRelationshipStore.deleteByVolumeId(volumeId) == 1);
    assertTrue(volumeRuleRelationshipStore.deleteByRuleId(ruleId + 3) == 1);
  }


  @Test
  public void testIscsiAccessRuleStore() {

    IscsiAccessRuleInformation iscsiAccessRuleInformation = null;
    long ruleId = 10000000;

    iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId, "rule1", "10.0.1.101",
        "root", "root",
        "root", "root", AccessPermissionType.READ.getValue());
    iscsiAccessRuleStore.save(iscsiAccessRuleInformation);
    iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId + 1, "rule2", "10.0.1.102",
        "root", "root",
        "root", "root", AccessPermissionType.WRITE.getValue());
    iscsiAccessRuleStore.save(iscsiAccessRuleInformation);
    iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId + 2, "rule3", "10.0.1.103",
        "root", "root",
        "root", "root", AccessPermissionType.WRITE.getValue());
    iscsiAccessRuleStore.save(iscsiAccessRuleInformation);
    iscsiAccessRuleInformation = new IscsiAccessRuleInformation(ruleId + 3, "rule4", "10.0.1.104",
        "root", "root",
        "root", "root", AccessPermissionType.WRITE.getValue());
    iscsiAccessRuleStore.save(iscsiAccessRuleInformation);

    assertTrue(iscsiAccessRuleStore.list().size() == 4);
    assertTrue(iscsiAccessRuleStore.get(ruleId + 2).getInitiatorName().equals("10.0.1.103"));
    iscsiAccessRuleStore.delete(ruleId + 3);
    assertTrue(iscsiAccessRuleStore.list().size() == 3);

    iscsiAccessRuleInformation.setRuleId(ruleId);
    iscsiAccessRuleInformation.setInitiatorName("10.0.1.101");
    iscsiAccessRuleInformation.permission(AccessPermissionType.WRITE);
    iscsiAccessRuleStore.update(iscsiAccessRuleInformation);

    for (IscsiAccessRuleInformation rule : iscsiAccessRuleStore.list()) {
      assertTrue(rule.getPermission() == AccessPermissionType.WRITE.getValue());
    }

    iscsiAccessRuleStore.delete(ruleId);
    iscsiAccessRuleStore.delete(ruleId + 1);
    iscsiAccessRuleStore.delete(ruleId + 2);
    assertTrue(iscsiAccessRuleStore.list().size() == 0);
  }

  @Test
  public void testIscsiRuleRelationshipStore() {
    long ruleId = 10000000;
    long volumeId = 2000000;
    final DriverKey driverKey = new DriverKey(0, 0, 0, DriverType.ISCSI.ISCSI);

    long relationshipId = 3000000;
    IscsiRuleRelationshipInformation relationship;
    relationship = new IscsiRuleRelationshipInformation(relationshipId, 0, 0, 0, "ISCSI", ruleId);
    iscsiRuleRelationshipStore.save(relationship);

    relationship = new IscsiRuleRelationshipInformation(relationshipId + 2, 0, 0, 0, "ISCSI",
        ruleId + 1);
    iscsiRuleRelationshipStore.save(relationship);

    relationship = new IscsiRuleRelationshipInformation(relationshipId + 3, 2, 0, 0, "ISCSI",
        ruleId + 3);
    iscsiRuleRelationshipStore.save(relationship);

    relationship = new IscsiRuleRelationshipInformation(relationshipId + 4, 3, 0, 0, "ISCSI",
        ruleId + 1);
    iscsiRuleRelationshipStore.save(relationship);

    assertTrue(iscsiRuleRelationshipStore.list().size() == 4);
    assertTrue(iscsiRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
    assertTrue(iscsiRuleRelationshipStore.getByRuleId(ruleId + 1).size() == 2);
    assertTrue(iscsiRuleRelationshipStore.getByDriverKey(driverKey).size() == 2);
    assertTrue(iscsiRuleRelationshipStore.deleteByRuleId(ruleId + 1) == 2);
    assertTrue(iscsiRuleRelationshipStore.deleteByDriverKey(driverKey) == 1);
    assertTrue(iscsiRuleRelationshipStore.deleteByRuleId(ruleId + 3) == 1);
  }

  @Test
  public void testIoLimitationStore() {
    long ruleId = 10000000;

    LocalTime startTime0 = LocalTime.now().plusSeconds(0);
    LocalTime endTime0 = LocalTime.now().plusSeconds(4);

    //IoLimitationEntry
    List<IoLimitationEntry> entries = new ArrayList<>();
    IoLimitationEntry entry1 = new IoLimitationEntry(0, 100, 10, 1000, 100, startTime0, endTime0);
    entries.add(entry1);

    //IoLimitation
    IoLimitation ioLimitation = new IoLimitation();
    ioLimitation.setId(ruleId);
    ioLimitation.setLimitType(IoLimitation.LimitType.Dynamic);
    ioLimitation.setEntries(entries);
    ioLimitation.setStatus(IoLimitationStatus.APPLIED);

    //save
    ioLimitationStore.save(ioLimitation);

    //get
    IoLimitation ioLimitation1 = ioLimitationStore.get(ruleId);
    assertNotNull(ioLimitation1);
    assertEquals(1, ioLimitation1.getEntries().size());
    IoLimitationEntry entry = ioLimitation1.getEntries().get(0);
    assertEquals(100, entry.getUpperLimitedIops());
    IoLimitationThrift ioLimitationThrift = RequestResponseHelper
        .buildThriftIoLimitationFrom(ioLimitation1);
    assertNotNull(ioLimitationThrift);
    assertEquals(1, ioLimitationThrift.getEntriesSize());
    IoLimitationEntryThrift ioLimitationEntryThrift = ioLimitationThrift.getEntries().get(0);
    assertEquals(100, ioLimitationEntryThrift.getUpperLimitedIoPs());

    //update
    entry1.setUpperLimitedIops(200);
    entry1.setLowerLimitedIops(50);
    ioLimitationStore.update(ioLimitation);
    //get to check
    ioLimitation1 = ioLimitationStore.get(ruleId);
    entry = ioLimitation1.getEntries().get(0);
    assertEquals(200, entry.getUpperLimitedIops());
    assertEquals(50, entry.getLowerLimitedIops());

    //list
    List<IoLimitation> ioLimitationList = ioLimitationStore.list();
    assertEquals(1, ioLimitationList.size());
    assertEquals(50, ioLimitationList.get(0).getEntries().get(0).getLowerLimitedIops());

    //delete
    ioLimitationStore.delete(ruleId);
    //list check
    ioLimitationList = ioLimitationStore.list();
    assertTrue(ioLimitationList.isEmpty());
  }

  @Test
  public void testMigrationSpeedRuleStore() {

    MigrationRuleInformation migrationSpeedInformation;
    long ruleId = 10000000;

    migrationSpeedInformation = new MigrationRuleInformation(ruleId, "rule", 22,
        MigrationRuleStatus.AVAILABLE.toString());
    migrationRuleStore.save(migrationSpeedInformation);
    migrationSpeedInformation = new MigrationRuleInformation(ruleId + 1, "rule1", 22,
        MigrationRuleStatus.AVAILABLE.toString());
    migrationRuleStore.save(migrationSpeedInformation);
    migrationSpeedInformation = new MigrationRuleInformation(ruleId + 2, "rule2", 22,
        MigrationRuleStatus.AVAILABLE.toString());
    migrationRuleStore.save(migrationSpeedInformation);
    migrationSpeedInformation = new MigrationRuleInformation(ruleId + 3, "rule3", 22,
        MigrationRuleStatus.AVAILABLE.toString());
    migrationRuleStore.save(migrationSpeedInformation);

    assertEquals(4, migrationRuleStore.list().size());
    assertTrue(migrationRuleStore.get(ruleId + 2).getMigrationRuleName().equals("rule2"));
    migrationRuleStore.delete(ruleId + 3);
    assertEquals(3, migrationRuleStore.list().size());

    migrationRuleStore.delete(ruleId);
    migrationRuleStore.delete(ruleId + 1);
    migrationRuleStore.delete(ruleId + 2);
    assertEquals(0, migrationRuleStore.list().size());

  }

  @Test
  public void testRebalanceRuleStore()
      throws PoolAlreadyAppliedRebalanceRuleExceptionThrift, RebalanceRuleNotExistExceptionThrift {
    InformationCenterImpl infoImpl = new InformationCenterImpl();

    RebalanceRule rebalanceRule;
    long ruleId = 10000000;
    long poolId = 10000000;

    //add
    {
      rebalanceRule = new RebalanceRule();
      rebalanceRule.setRuleId(ruleId + 1);
      rebalanceRule.setRuleName("rule1");

      RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
      relativeTime.setWaitTime(1);
      rebalanceRule.setRelativeTime(relativeTime);

      List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();
      {
        RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
        absoluteTime.setId(1);
        absoluteTime.setBeginTime(1);
        absoluteTime.setEndTime(1);
        absoluteTimeList.add(absoluteTime);
      }
      {
        RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
        absoluteTime.setId(2);
        absoluteTime.setBeginTime(20);
        absoluteTime.setEndTime(20);
        Set<RebalanceAbsoluteTime.WeekDay> weekDaySet = new HashSet<>();
        absoluteTime.setWeekDaySet(weekDaySet);
        absoluteTimeList.add(absoluteTime);
      }
      {
        RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
        absoluteTime.setId(3);
        absoluteTime.setBeginTime(30);
        absoluteTime.setEndTime(30);
        Set<RebalanceAbsoluteTime.WeekDay> weekDaySet = new HashSet<>();
        weekDaySet.add(RebalanceAbsoluteTime.WeekDay.FRI);
        absoluteTime.setWeekDaySet(weekDaySet);
        absoluteTimeList.add(absoluteTime);
      }
      {
        RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
        absoluteTime.setId(4);
        absoluteTime.setBeginTime(40);
        absoluteTime.setEndTime(40);
        Set<RebalanceAbsoluteTime.WeekDay> weekDaySet = new HashSet<>();
        weekDaySet.add(RebalanceAbsoluteTime.WeekDay.MON);
        weekDaySet.add(RebalanceAbsoluteTime.WeekDay.FRI);
        absoluteTime.setWeekDaySet(weekDaySet);
        absoluteTimeList.add(absoluteTime);
      }
      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      rebalanceRuleStore.save(rebalanceRule.toRebalanceRuleInformation());
    }
    {
      rebalanceRule = new RebalanceRule();
      rebalanceRule.setRuleId(ruleId + 2);
      rebalanceRule.setRuleName("rule2");

      RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
      relativeTime.setWaitTime(10);
      rebalanceRule.setRelativeTime(relativeTime);

      List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();
      {
        RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
        absoluteTime.setId(1);
        absoluteTime.setBeginTime(100);
        absoluteTime.setEndTime(101);
        absoluteTime.setWeekDaySet(null);
        absoluteTimeList.add(absoluteTime);
      }
      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      rebalanceRuleStore.save(rebalanceRule.toRebalanceRuleInformation());
    }
    {
      rebalanceRule = new RebalanceRule();
      rebalanceRule.setRuleId(ruleId + 3);
      rebalanceRule.setRuleName("rule3");

      RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
      relativeTime.setWaitTime(200);
      rebalanceRule.setRelativeTime(relativeTime);
      rebalanceRule.setAbsoluteTimeList(null);

      rebalanceRuleStore.save(rebalanceRule.toRebalanceRuleInformation());
    }

    //list
    assertEquals(3, rebalanceRuleStore.list().size());
    assertTrue(rebalanceRuleStore.get(ruleId + 2).getRuleName().equals("rule2"));

    //update
    {
      rebalanceRule = rebalanceRuleStore.get(ruleId + 2).toRebalanceRule();

      RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
      relativeTime.setWaitTime(100);
      rebalanceRule.setRelativeTime(relativeTime);

      rebalanceRuleStore.update(rebalanceRule.toRebalanceRuleInformation());

      assertEquals(100, rebalanceRuleStore.get(ruleId + 2).getWaitTime());

      //list
      assertEquals(3, rebalanceRuleStore.list().size());
      assertTrue(rebalanceRuleStore.get(ruleId + 2).getRuleName().equals("rule2"));
    }

    //getAppliedRules
    {
      assert (0 == rebalanceRuleStore.getAppliedRules().size());
    }

    List<RebalanceRuleInformation> rebalanceRuleInformationList;
    //apply
    {
      RebalanceRuleInformation rule = rebalanceRuleStore.get(ruleId + 2);
      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(poolId + 4);
      poolIdList.add(poolId + 2);
      rebalanceRuleStore.applyRule(rule, poolIdList);
      assertEquals(ruleId + 2, rebalanceRuleStore.getRuleOfPool(poolId + 4).getRuleId());
      assertEquals(ruleId + 2, rebalanceRuleStore.getRuleOfPool(poolId + 2).getRuleId());
      rebalanceRuleInformationList = rebalanceRuleStore.list();
    }

    //getAppliedRules
    {
      rebalanceRuleInformationList = rebalanceRuleStore.getAppliedRules();
      assert (1 == rebalanceRuleInformationList.size());
      assert (ruleId + 2 == rebalanceRuleInformationList.get(0).getRuleId());
    }

    //disApply
    {
      RebalanceRuleInformation rule = rebalanceRuleStore.get(ruleId + 2);
      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(poolId + 4);
      rebalanceRuleStore.unApplyRule(rule, poolIdList);
      assertEquals(null, rebalanceRuleStore.getRuleOfPool(poolId + 4));
      rebalanceRuleInformationList = rebalanceRuleStore.list();
    }

    //getAppliedRules
    {
      rebalanceRuleInformationList = rebalanceRuleStore.getAppliedRules();
      assert (1 == rebalanceRuleInformationList.size());
    }

    //delete
    rebalanceRuleStore.delete(ruleId + 3);
    assertEquals(2, rebalanceRuleStore.list().size());

    rebalanceRuleStore.delete(ruleId);
    rebalanceRuleStore.delete(ruleId + 1);
    rebalanceRuleStore.delete(ruleId + 2);
    assertEquals(0, rebalanceRuleStore.list().size());
  }

  @Test
  public void testVolumeStore() throws AccessDeniedException {
    VolumeMetadata volumeMetadata = new VolumeMetadata(20000, 20001, 20002, 20003, null, 0L,
        0L);

    volumeMetadata.setVolumeId(37002);
    volumeMetadata.setRootVolumeId(1003);
    volumeMetadata.setChildVolumeId(null);
    volumeMetadata.setVolumeSize(1005);
    volumeMetadata.setExtendingSize(1006);
    volumeMetadata.setName("stdname");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(1008);
    volumeMetadata.setDeadTime(0L);
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setInAction(NULL);
    volumeMetadata.setVolumeDescription("test1");
    volumeMetadata.setClientLastConnectTime(123456);

    SegId segId = new SegId(37002, 11);
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());

    // segmentMetadata.getSegmentUnitMetadataTable().put(key, value);

    volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

    volumeStore.saveVolume(volumeMetadata);
    VolumeMetadata volumeMetadata1 = volumeStore.getVolume(37002L);
    assertTrue(volumeMetadata1.getVolumeDescription().equals("test1"));
    assertNotNull(volumeMetadata1);

    volumeStore.markVolumeDelete(volumeMetadata.getVolumeId());
    VolumeMetadata volumeMetadata3 = volumeStore.getVolume(37002L);
    assertTrue(volumeMetadata3.isMarkDelete() == true);

    VolumeMetadata volumeMetadata2 = volumeStore.getVolumeNotDeadByName(volumeMetadata.getName());
    assertNull(volumeMetadata2);
    //uo
    volumeStore.updateClientLastConnectTime(volumeMetadata.getVolumeId(), 223456);
    VolumeMetadata volumeMetadata4 = volumeStore.getVolume(volumeMetadata.getVolumeId());
    assertTrue(volumeMetadata4.getVolumeDescription().equals("test1"));
    assertTrue(volumeMetadata4.getClientLastConnectTime() == 223456);
  }

  @Test
  public void testVolumeStoreCreatedTimeAndVolumeSource() throws AccessDeniedException {
    VolumeMetadata volumeMetadata = new VolumeMetadata(20000, 20001, 20002,
        20003, null, 0L, 0L);

    volumeMetadata.setVolumeId(37002);
    volumeMetadata.setRootVolumeId(1003);
    volumeMetadata.setChildVolumeId(null);
    volumeMetadata.setVolumeSize(1005);
    volumeMetadata.setExtendingSize(1006);
    volumeMetadata.setName("stdname");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(1008);
    volumeMetadata.setDeadTime(0L);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setPageWrappCount(128);
    volumeMetadata.setSegmentWrappCount(10);
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setInAction(NULL);
    SegId segId = new SegId(37002, 11);
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());

    volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

    volumeStore.saveVolume(volumeMetadata);
    VolumeMetadata volumeMetadata1;
    volumeMetadata1 = volumeStore.getVolume(37002L);
    Date now = new Date();
    assertTrue("CreatedTime isn't close enough to the testTime!",
        now.getTime() - volumeMetadata1.getVolumeCreatedTime().getTime() < 1000 * 60);
    assertEquals(volumeMetadata1.getVolumeSource(), VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
  }

  @Test
  public void testDomainDbStore() throws SQLException, IOException {
    Long domainId = 0L;
    Set<Long> dataNodes = new HashSet<>();
    for (long key = 0; key < 3; key++) {
      dataNodes.add(key);
    }

    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setDomainName("testForDomainDb");
    domain.setDomainDescription(null);
    domain.setDataNodes(dataNodes);
    domain.setStoragePools(dataNodes);

    domainDbStore.saveDomainToDb(domain);
    Domain domainFromDb = domainDbStore.getDomainFromDb(domainId);
    Set<Long> dataNodesInDb = domainFromDb.getDataNodes();
    assertTrue(dataNodesInDb.size() == 3);
    int sameCount = 0;
    for (Long datanodeFromDb : dataNodesInDb) {
      for (Long datanodeOri : dataNodes) {
        if (datanodeOri.equals(datanodeFromDb)) {
          sameCount++;
        }
      }
    }

    assertTrue(sameCount == 3);
    assertTrue(dataNodes.size() == 3);
    // remove one element
    Long deleteValue = 0L;
    dataNodes.remove(deleteValue);
    assertTrue(dataNodes.size() == 2);

    domainDbStore.saveDomainToDb(domain);
    domainFromDb = domainDbStore.getDomainFromDb(domainId);
    dataNodesInDb = domainFromDb.getDataNodes();
    assertTrue(dataNodesInDb.size() == 2);
    sameCount = 0;
    for (Long datanodeFromDb : dataNodesInDb) {
      for (Long datanodeOri : dataNodes) {
        if (datanodeOri.equals(datanodeFromDb)) {
          sameCount++;
        }
      }
    }
    assertTrue(sameCount == 2);

    domainDbStore.deleteDomainFromDb(domainId);
    domainFromDb = domainDbStore.getDomainFromDb(domainId);
    assertNull(domainFromDb);
  }

  @Test
  public void testDomainStore() throws SQLException, IOException {
    Long domainId = 0L;
    Set<Long> dataNodes = new HashSet<>();
    for (int key = 0; key < 3; key++) {
      dataNodes.add((long) key);
    }
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setDomainName("testForDomainMem");
    domain.setDomainDescription(null);
    domain.setDataNodes(dataNodes);

    domainStore.saveDomain(domain);
    Domain domainFromDb = domainStore.getDomain(domainId);
    Set<Long> dataNodesInMem = domainFromDb.getDataNodes();
    assertTrue(dataNodesInMem.size() == 3);
    int sameCount = 0;
    for (Long datanodeFromDb : dataNodesInMem) {
      for (Long datanodeOri : dataNodes) {
        if (datanodeOri.equals(datanodeFromDb)) {
          sameCount++;
        }
      }
    }
    assertTrue(sameCount == 3);

    assertTrue(dataNodes.size() == 3);
    // remove one element
    Long deleteValue = 0L;
    dataNodes.remove(deleteValue);
    assertTrue(dataNodes.size() == 2);

    domainStore.saveDomain(domain);
    domainFromDb = domainStore.getDomain(domainId);
    dataNodesInMem = domainFromDb.getDataNodes();
    assertTrue(dataNodesInMem.size() == 2);
    sameCount = 0;
    for (Long datanodeFromDb : dataNodesInMem) {
      for (Long datanodeOri : dataNodes) {
        if (datanodeOri.equals(datanodeFromDb)) {
          sameCount++;
        }
      }
    }
    assertTrue(sameCount == 2);

    domainStore.deleteDomain(domainId);
    domainFromDb = domainStore.getDomain(domainId);
    assertTrue(domainFromDb == null);
  }

  @Test   //nyj
  public void testDomainStoreCleanMemAndPutNewDomainBeforeList() throws SQLException, IOException {
    Long domainId1 = 1L;
    Long domainId2 = 2L;
    Long domainId3 = 3L;
    Long domainId4 = 4L;
    Domain domain1 = new Domain(domainId1, "testdomain1", null, new HashSet<>(), new HashSet<>());
    Domain domain2 = new Domain(domainId2, "testdomain2", null, new HashSet<>(), new HashSet<>());
    Domain domain3 = new Domain(domainId3, "testdomain3", null, new HashSet<>(), new HashSet<>());
    final Domain domain4 = new Domain(domainId4, "testdomain4", null, new HashSet<>(),
        new HashSet<>());
    //put1,2,3
    domainStore.saveDomain(domain1);
    domainStore.saveDomain(domain2);
    domainStore.saveDomain(domain3);

    List<Domain> domainList = domainStore.listAllDomains();
    assertTrue(domainList.size() == 3);

    //clear1,2,3
    domainStore.clearMemoryMap();
    //list
    domainList = domainStore.listAllDomains();
    assertTrue(domainList.size() == 3);

    //clear1,2,3
    domainStore.clearMemoryMap();
    //put4
    domainStore.saveDomain(domain4);
    //list
    domainList = domainStore.listAllDomains();
    assertTrue(domainList.size() == 1);
    Assert.assertEquals(domain4.getDomainId(), domainList.get(0).getDomainId());

    // (put4)  clear4
    domainStore.clearMemoryMap();
    //list
    domainList = domainStore.listAllDomains();
    assertTrue(domainList.size() == 4);
  }

  @Test
  public void testDomainStoreMemClean() throws SQLException, IOException {
    Long domainId1 = 0L;
    Long domainId2 = 1L;
    Long domainId3 = 2L;
    long instanceIndex = 0;
    Set<Long> dataNodes = new HashSet<>();

    instanceIndex++;
    dataNodes.add(instanceIndex);

    Domain domain0 = new Domain(domainId1, "testForDomainMem1", null, dataNodes, new HashSet<>());
    Domain domain1 = new Domain(domainId2, "testForDomainMem2", null, dataNodes, new HashSet<>());
    Domain domain2 = new Domain(domainId3, "testForDomainMem3", null, dataNodes, new HashSet<>());
    domainStore.saveDomain(domain0);
    domainStore.saveDomain(domain1);
    domainStore.saveDomain(domain2);
    List<Domain> domainList;
    domainList = domainStore.listAllDomains();
    assertTrue(domainList.size() == 3);
    assertTrue(domainList.contains(domain0));
    assertTrue(domainList.contains(domain1));
    assertTrue(domainList.contains(domain2));

    List<Long> domainIds = new ArrayList<>();
    domainIds.add(0L);
    domainList = domainStore.listDomains(domainIds);
    assertTrue(domainList.size() == 1);
    assertTrue(domainList.contains(domain0));

    // clear memory map
    domainStore.clearMemoryMap();
    domainList = domainStore.listAllDomains();
    assertTrue(domainList.size() == 3);
    int sameCount = 0;
    for (Domain domain : domainList) {
      if (domain.equals(domain0)) {
        sameCount++;
      } else if (domain.equals(domain1)) {
        sameCount++;
      } else if (domain.equals(domain2)) {
        sameCount++;
      }
    }
    assertTrue(sameCount == 3);
    // clear memory map
    domainStore.clearMemoryMap();
    domainList = domainStore.listDomains(domainIds);
    assertTrue(domainList.size() == 1);
    assertTrue(domainList.contains(domain0));
  }


  @Test
  public void testStoragePoolDbStore() throws InterruptedException, SQLException, IOException {
    //test max
    StoragePool originalStoragePoolTemp = TestUtils.buildStoragePool();
    originalStoragePoolTemp.setDescription(TestBase.getRandomString(250));
    logger.warn("get the value:{}", TestBase.getRandomString(250));

    storagePoolDbStore.saveStoragePoolToDb(originalStoragePoolTemp);

    StoragePool originalStoragePool = TestUtils.buildStoragePool();
    storagePoolDbStore.saveStoragePoolToDb(originalStoragePool);

    StoragePool fromDbPool = storagePoolDbStore
        .getStoragePoolFromDb(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));

    Long datanodeId = (Long) originalStoragePool.getArchivesInDataNode().keySet().toArray()[0];
    // test delete one archive id
    assertNotNull(datanodeId);
    Long deleteArchiveId = (Long) originalStoragePool.getArchivesInDataNode().get(datanodeId)
        .toArray()[0];

    originalStoragePool.removeArchiveFromDatanode(datanodeId, deleteArchiveId);

    assertTrue(
        !originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, deleteArchiveId));
    storagePoolDbStore.saveStoragePoolToDb(originalStoragePool);

    fromDbPool = storagePoolDbStore.getStoragePoolFromDb(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));

    // test add one archive id
    Long newArchiveId = RequestIdBuilder.get();
    originalStoragePool.addArchiveInDatanode(datanodeId, newArchiveId);
    assertTrue(originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, newArchiveId));
    storagePoolDbStore.saveStoragePoolToDb(originalStoragePool);

    fromDbPool = storagePoolDbStore.getStoragePoolFromDb(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));

    // test remove volumeId
    Long deleteVolumeId = (Long) originalStoragePool.getVolumeIds().toArray()[0];
    originalStoragePool.removeVolumeId(deleteVolumeId);

    assertTrue(!originalStoragePool.getVolumeIds().contains(deleteVolumeId));
    storagePoolDbStore.saveStoragePoolToDb(originalStoragePool);

    fromDbPool = storagePoolDbStore.getStoragePoolFromDb(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));
    // test add volumeId
    Long newVolumeId = RequestIdBuilder.get();
    originalStoragePool.addVolumeId(newVolumeId);

    assertTrue(originalStoragePool.getVolumeIds().contains(newVolumeId));
    storagePoolDbStore.saveStoragePoolToDb(originalStoragePool);

    fromDbPool = storagePoolDbStore.getStoragePoolFromDb(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));
    storagePoolDbStore.deleteStoragePoolFromDb(fromDbPool.getPoolId());
    fromDbPool = storagePoolDbStore.getStoragePoolFromDb(originalStoragePool.getPoolId());
    assertTrue(fromDbPool == null);
  }

  @Test
  public void testStoragePoolStore() throws SQLException, IOException {
    StoragePool originalStoragePool = TestUtils.buildStoragePool();
    storagePoolStore.saveStoragePool(originalStoragePool);

    StoragePool fromDbPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));

    Long datanodeId = (Long) originalStoragePool.getArchivesInDataNode().keySet().toArray()[0];
    // test delete one archive id
    assertNotNull(datanodeId);
    Long deleteArchiveId = (Long) originalStoragePool.getArchivesInDataNode().get(datanodeId)
        .toArray()[0];

    originalStoragePool.removeArchiveFromDatanode(datanodeId, deleteArchiveId);

    assertTrue(
        !originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, deleteArchiveId));
    storagePoolStore.saveStoragePool(originalStoragePool);

    fromDbPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));

    // test add one archive id
    Long newArchiveId = RequestIdBuilder.get();
    originalStoragePool.addArchiveInDatanode(datanodeId, newArchiveId);
    assertTrue(originalStoragePool.getArchivesInDataNode().containsEntry(datanodeId, newArchiveId));
    storagePoolStore.saveStoragePool(originalStoragePool);

    fromDbPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));

    // test remove volumeId
    Long deleteVolumeId = (Long) originalStoragePool.getVolumeIds().toArray()[0];
    originalStoragePool.removeVolumeId(deleteVolumeId);

    assertTrue(!originalStoragePool.getVolumeIds().contains(deleteVolumeId));
    storagePoolStore.saveStoragePool(originalStoragePool);

    fromDbPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));
    // test add volumeId
    Long newVolumeId = RequestIdBuilder.get();
    originalStoragePool.addVolumeId(newVolumeId);

    assertTrue(originalStoragePool.getVolumeIds().contains(newVolumeId));
    storagePoolStore.saveStoragePool(originalStoragePool);

    fromDbPool = storagePoolStore.getStoragePool(originalStoragePool.getPoolId());
    assertTrue(fromDbPool.equals(originalStoragePool));
  }

  @Test
  public void testListStoragePool() throws SQLException, IOException {
    Long domainId = RequestIdBuilder.get();
    StoragePool storagePool1 = TestUtils.buildStoragePool();
    storagePool1.setDomainId(domainId);
    StoragePool storagePool2 = TestUtils.buildStoragePool();
    storagePool2.setDomainId(domainId);
    StoragePool storagePool3 = TestUtils.buildStoragePool();

    storagePoolStore.saveStoragePool(storagePool1);
    storagePoolStore.saveStoragePool(storagePool2);
    storagePoolStore.saveStoragePool(storagePool3);

    List<StoragePool> storagePools = storagePoolStore.listStoragePools(domainId);

    assertTrue(storagePools.size() == 2);
    for (StoragePool storagePool : storagePools) {
      if (!storagePool.equals(storagePool1) && !storagePool.equals(storagePool2)) {
        assertTrue(false);
      }
    }
  }

  @Test
  public void testCleanStoragePoolStoreMemory() throws SQLException, IOException {
    StoragePool storagePool1 = TestUtils.buildStoragePool();
    StoragePool storagePool2 = TestUtils.buildStoragePool();
    StoragePool storagePool3 = TestUtils.buildStoragePool();
    storagePoolStore.saveStoragePool(storagePool1);
    storagePoolStore.saveStoragePool(storagePool2);
    storagePoolStore.saveStoragePool(storagePool3);

    List<StoragePool> allStoragePools = storagePoolStore.listAllStoragePools();
    assertEquals(3, allStoragePools.size());

    List<Long> listPoolIds = new ArrayList<>();
    listPoolIds.add(storagePool1.getPoolId());
    listPoolIds.add(storagePool2.getPoolId());
    List<StoragePool> someStoragePools = storagePoolStore.listStoragePools(listPoolIds);
    assertEquals(2, someStoragePools.size());

    storagePoolStore.clearMemoryMap();
    allStoragePools = storagePoolStore.listAllStoragePools();
    assertEquals(3, allStoragePools.size());
  }

  @Test
  public void testCapacityRecordDbStore() throws Exception {
    CapacityRecord capacityRecord1 = TestUtils.buildCapacityRecord();

    // save 1 to db
    capacityRecordDbStore.saveToDb(capacityRecord1);
    CapacityRecord loadRecordFromDb1 = capacityRecordDbStore.loadFromDb();
    for (Entry<String, TotalAndUsedCapacity> entry : capacityRecord1.getRecordMap().entrySet()) {
      assertEquals(entry.getValue(), loadRecordFromDb1.getRecordMap().get(entry.getKey()));
    }
    CapacityRecord capacityRecord2 = TestUtils.buildCapacityRecord();

    // save 2 to db
    capacityRecordDbStore.saveToDb(capacityRecord2);
    CapacityRecord loadRecordFromDb2 = capacityRecordDbStore.loadFromDb();
    for (Entry<String, TotalAndUsedCapacity> entry : capacityRecord2.getRecordMap().entrySet()) {
      assertEquals(entry.getValue(), loadRecordFromDb2.getRecordMap().get(entry.getKey()));
    }
  }

  @Test
  public void testCapacityRecordStore() throws Exception {
    CapacityRecord capacityRecord1 = TestUtils.buildCapacityRecord();
    capacityRecordStore.saveCapacityRecord(capacityRecord1);

    CapacityRecord recordFrom1 = capacityRecordStore.getCapacityRecord();
    assertEquals(capacityRecord1, recordFrom1);

    recordFrom1.getRecordMap().clear();
    TotalAndUsedCapacity capacityInfo = new TotalAndUsedCapacity(RequestIdBuilder.get(),
        RequestIdBuilder.get());
    recordFrom1.getRecordMap().put(TestBase.getRandomString(8), capacityInfo);
    CapacityRecord recordFrom2 = capacityRecordStore.getCapacityRecord();
    assertEquals(capacityRecord1, recordFrom2);
  }

  @Test
  public void testCapacityRecordRemoveEarliestRecord() throws InterruptedException {
    CapacityRecord capacityRecord1 = TestUtils.buildCapacityRecord();
    capacityRecord1.getRecordMap().clear();
    TotalAndUsedCapacity capacityInfo = new TotalAndUsedCapacity(RequestIdBuilder.get(),
        RequestIdBuilder.get());

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    Date nowDate = new Date();
    String firstKey = dateFormat.format(nowDate);
    capacityRecord1.addRecord(firstKey, capacityInfo);

    Thread.sleep(1000);
    nowDate = new Date();
    String secondKey = dateFormat.format(nowDate);
    capacityRecord1.addRecord(secondKey, capacityInfo);

    Thread.sleep(1000);
    nowDate = new Date();
    String thirdKey = dateFormat.format(nowDate);
    capacityRecord1.addRecord(thirdKey, capacityInfo);

    Thread.sleep(1000);
    nowDate = new Date();
    String fourthKey = dateFormat.format(nowDate);
    capacityRecord1.addRecord(fourthKey, capacityInfo);

    assertTrue(capacityRecord1.recordCount() == 4);

    capacityRecord1.removeEarliestRecord();
    assertTrue(capacityRecord1.recordCount() == 3);
    assertTrue(!capacityRecord1.getRecordMap().containsKey(firstKey));

    capacityRecord1.removeEarliestRecord();
    assertTrue(capacityRecord1.recordCount() == 2);
    assertTrue(!capacityRecord1.getRecordMap().containsKey(secondKey));

    capacityRecord1.removeEarliestRecord();
    assertTrue(capacityRecord1.recordCount() == 1);
    assertTrue(!capacityRecord1.getRecordMap().containsKey(thirdKey));

    capacityRecord1.removeEarliestRecord();
    assertTrue(capacityRecord1.recordCount() == 0);
  }

  @Test
  public void testBuildJson() {
    String dtoAsString = null;
    ObjectMapper mapper = new ObjectMapper();
    try {
      mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
      dtoAsString = mapper.writeValueAsString(new TotalAndUsedCapacity());
    } catch (JsonProcessingException e) {
      assertTrue(false);
    }
    assertTrue(dtoAsString != null);
  }

  @Test
  public void testBackupDbManagerFirstRound() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 3000; // ms
    int maxBackupCount = 3;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory
        .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient);

    // test first round
    long sequenceId1 = 10;
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    final ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    long sequenceId2 = 9;
    EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
    final ReportDbRequestThrift reportRequest2 = TestUtils
        .buildReportDbRequest(2, endPoint2, sequenceId2);

    long sequenceId3 = 11;
    EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
    ReportDbRequestThrift reportRequest3 = TestUtils
        .buildReportDbRequest(3, endPoint3, sequenceId3);

    GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
    getDbInfoResponse.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse.setDbInfo(reportRequest3);
    when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse);

    backupDbManager.process(reportRequest1);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId1,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());

    backupDbManager.process(reportRequest2);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId1,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());

    backupDbManager.process(reportRequest3);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId3,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());

    // test in first round, same endpoint show up many times, but record won't save any one
    backupDbManager.process(reportRequest1);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    backupDbManager.process(reportRequest2);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    backupDbManager.process(reportRequest3);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());

    // test after first round, biggest sequence id report, but not accept
    Thread.sleep(roundTimeInterval);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());

    long sequenceId4 = 14;
    EndPoint endPoint4 = new EndPoint("10.0.1.4", 1234);
    ReportDbRequestThrift reportRequest4 = TestUtils
        .buildReportDbRequest(4, endPoint4, sequenceId4);

    /*
     * in this process, manager will save request3 to database
     */
    ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest4);

    long afterFirstRoundSequenceId = sequenceId3 + 1;
    assertEquals(afterFirstRoundSequenceId,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    assertEquals(1, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    // test save to db and load from db
    TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

    reportResponseThrift = backupDbManager.process(reportRequest1);
    assertEquals(2, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

    reportResponseThrift = backupDbManager.process(reportRequest1);
    assertEquals(2, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    assertFalse(reportResponseThrift.isSetDomainThriftList());
    assertFalse(reportResponseThrift.isSetStoragePoolThriftList());
    assertFalse(reportResponseThrift.isSetVolume2RuleThriftList());
    assertFalse(reportResponseThrift.isSetAccessRuleThriftList());
    assertFalse(reportResponseThrift.isSetCapacityRecordThriftList());
    assertFalse(reportResponseThrift.isSetAccountMetadataBackupThriftList());
    assertFalse(reportResponseThrift.isSetRoleThriftList());

    reportResponseThrift = backupDbManager.process(reportRequest2);
    assertEquals(3, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

    /*
     * cause max save count is 3, if has been saved 3 datanodes, more datanode come, but manager
     * will not response
     * database info
     */
    reportResponseThrift = backupDbManager.process(reportRequest3);
    assertEquals(3, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    assertFalse(reportResponseThrift.isSetDomainThriftList());
    assertFalse(reportResponseThrift.isSetStoragePoolThriftList());
    assertFalse(reportResponseThrift.isSetVolume2RuleThriftList());
    assertFalse(reportResponseThrift.isSetAccessRuleThriftList());
    assertFalse(reportResponseThrift.isSetCapacityRecordThriftList());
    assertFalse(reportResponseThrift.isSetAccountMetadataBackupThriftList());
    assertFalse(reportResponseThrift.isSetRoleThriftList());
  }

  @Test
  public void testBackupDbManagerAfterFirstRound1() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 1000; // ms
    int maxBackupCount = 1;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory
        .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient);

    // process first round
    long sequenceId1 = 10;
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
    getDbInfoResponse.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse.setDbInfo(reportRequest1);
    when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse);

    backupDbManager.process(reportRequest1);

    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId1,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    Thread.sleep(roundTimeInterval);
    // do loop process
    int loopCount = 100;
    int groupCount = 5;
    long currentSequenceId = sequenceId1;
    for (int i = 0; i < loopCount; i++) {
      logger.debug("== loop:{} times ==", i);
      int groupId0 = i % groupCount;
      if (RandomUtils.nextBoolean()) {
        Thread.sleep(roundTimeInterval);
        ReportDbRequestThrift reportRequest = TestUtils
            .buildReportDbRequest(groupId0, endPoint1, sequenceId1);
        ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
        assertTrue(reportResponseThrift.getSequenceId() > currentSequenceId);
        currentSequenceId = reportResponseThrift.getSequenceId();
        assertTrue(((BackupDbManagerImpl) backupDbManager).getRecordCount() <= maxBackupCount);
        TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
      }
    }
  }

  @Test
  public void testBackupDbManagerAfterFirstRound2() throws Exception {
    // init config for backup Db manager
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    long roundTimeInterval = 1000; // ms
    int maxBackupCount = 3;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory
        .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient);

    // process first round
    long sequenceId1 = 10;
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
    getDbInfoResponse.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse.setDbInfo(reportRequest1);
    when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse);

    backupDbManager.process(reportRequest1);

    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId1,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    Thread.sleep(roundTimeInterval);
    // do loop process
    int loopCount = 100;
    int groupCount = 5;
    long currentSequenceId = sequenceId1;
    for (int i = 0; i < loopCount; i++) {
      logger.debug("== loop:{} times ==", i);
      int groupId0 = i % groupCount;
      if (RandomUtils.nextBoolean()) {
        ReportDbRequestThrift reportRequest = TestUtils
            .buildReportDbRequest(groupId0, endPoint1, sequenceId1);
        ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
        assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
        currentSequenceId = reportResponseThrift.getSequenceId();
        if (!reportResponseThrift.isSetDomainThriftList()) {
          if (!((BackupDbManagerImpl) backupDbManager).getRoundRecordMap()
              .containsKey(new Group(groupId0))) {
            logger.debug("map:{}, current group:{}",
                ((BackupDbManagerImpl) backupDbManager).getRoundRecordMap(), groupId0);
            assertEquals(maxBackupCount, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
          }
        } else {
          TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
        }
      }
      int groupId1 = (i + 1) % groupCount;
      if (RandomUtils.nextBoolean()) {
        ReportDbRequestThrift reportRequest = TestUtils
            .buildReportDbRequest(groupId1, endPoint1, sequenceId1);
        ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
        assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
        currentSequenceId = reportResponseThrift.getSequenceId();
        if (!reportResponseThrift.isSetDomainThriftList()) {
          if (!((BackupDbManagerImpl) backupDbManager).getRoundRecordMap()
              .containsKey(new Group(groupId1))) {
            logger.debug("map:{}, current group:{}",
                ((BackupDbManagerImpl) backupDbManager).getRoundRecordMap(), groupId1);
            assertEquals(maxBackupCount, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
          }
        } else {
          TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
        }
      }
      int groupId2 = (i + 2) % groupCount;
      if (RandomUtils.nextBoolean()) {
        ReportDbRequestThrift reportRequest = TestUtils
            .buildReportDbRequest(groupId2, endPoint1, sequenceId1);
        ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
        assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
        currentSequenceId = reportResponseThrift.getSequenceId();
        if (!reportResponseThrift.isSetDomainThriftList()) {
          if (!((BackupDbManagerImpl) backupDbManager).getRoundRecordMap()
              .containsKey(new Group(groupId2))) {
            logger.debug("map:{}, current group:{}",
                ((BackupDbManagerImpl) backupDbManager).getRoundRecordMap(), groupId2);
            assertEquals(maxBackupCount, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
          }
        } else {
          TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
        }
      }
      int groupId3 = (i + 3) % groupCount;
      if (RandomUtils.nextBoolean()) {
        ReportDbRequestThrift reportRequest = TestUtils
            .buildReportDbRequest(groupId3, endPoint1, sequenceId1);
        ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
        assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
        currentSequenceId = reportResponseThrift.getSequenceId();
        if (!reportResponseThrift.isSetDomainThriftList()) {
          if (!((BackupDbManagerImpl) backupDbManager).getRoundRecordMap()
              .containsKey(new Group(groupId3))) {
            logger.debug("map:{}, current group:{}",
                ((BackupDbManagerImpl) backupDbManager).getRoundRecordMap(), groupId3);
            assertEquals(maxBackupCount, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
          }
        } else {
          TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
        }
      }
      int groupId4 = (i + 4) % groupCount;
      if (RandomUtils.nextBoolean()) {
        Thread.sleep(roundTimeInterval);
        ReportDbRequestThrift reportRequest = TestUtils
            .buildReportDbRequest(groupId4, endPoint1, sequenceId1);
        ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
        assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
        currentSequenceId = reportResponseThrift.getSequenceId();
        if (!reportResponseThrift.isSetDomainThriftList()) {
          if (!((BackupDbManagerImpl) backupDbManager).getRoundRecordMap()
              .containsKey(new Group(groupId4))) {
            logger.debug("map:{}, current group:{}",
                ((BackupDbManagerImpl) backupDbManager).getRoundRecordMap(), groupId4);
            assertEquals(maxBackupCount, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
          }
        } else {
          TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
        }
      }
    }
  }

  @Test(timeout = 180000)
  public void testMultiThreadReportDbRequest() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 1000; // ms
    int maxBackupCount = 3;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory
        .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient);

    long sequenceId1 = 10;
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
    getDbInfoResponse.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse.setDbInfo(reportRequest1);
    when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse);

    backupDbManager.process(reportRequest1);

    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId1,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    Thread.sleep(roundTimeInterval);

    //todo:
    int threadCount = 2;
    int eachThreadRunLoop = 2;
    final CountDownLatch allThreadLatch = new CountDownLatch(threadCount);
    AtomicBoolean meetError = new AtomicBoolean(false);
    for (int i = 0; i < threadCount; i++) {
      final int groupId = i;
      Thread thread = new Thread() {
        long sequenceId = 0;
        long currentSequenceId = 0;
        Group group = new Group(groupId % 5);
        EndPoint endPoint = new EndPoint("10.0.1.1", 1234);
        ReportDbResponseThrift saveReportResponse = null;
        int runTimes = 0;

        @Override
        public void run() {
          while (true) {

            ReportDbRequestThrift reportRequest = TestUtils
                .buildReportDbRequest(group.getGroupId(),
                    endPoint, sequenceId);
            ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
            assertTrue(reportResponseThrift.getSequenceId() >= currentSequenceId);
            currentSequenceId = reportResponseThrift.getSequenceId();
            if (!reportResponseThrift.isSetDomainThriftList()) {
              if (!((BackupDbManagerImpl) backupDbManager).getRoundRecordMap().containsKey(group)) {
                logger.debug("map:{}, current group:{}",
                    ((BackupDbManagerImpl) backupDbManager).getRoundRecordMap(), group);
                if (maxBackupCount != ((BackupDbManagerImpl) backupDbManager).getRecordCount()) {
                  logger.debug(
                      "meetError set true! maxBackupCount:{}; backupDbManager.getRecordCount:{}",
                      maxBackupCount,
                      ((BackupDbManagerImpl) backupDbManager).getRecordCount());
                  meetError.set(true);
                }
              }
            } else {
              if (saveReportResponse == null) {
                saveReportResponse = reportResponseThrift;
              }
              if (!TestUtils.compareTwoReportDbResponse(saveReportResponse, reportResponseThrift)) {
                logger.debug("meetError set true! saveReportResponse:{}; reportResponseThrift:{}",
                    saveReportResponse,
                    reportResponseThrift);
                meetError.set(true);
              }
            }
            try {
              logger.warn("== run times:{}==", runTimes);
              sequenceId++;
              runTimes++;
              if (runTimes == eachThreadRunLoop) {
                allThreadLatch.countDown();
              }
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              logger.error("failed to sleep", e);
            }
          }
        }
      };
      thread.start();
    }
    allThreadLatch.await();
    assertTrue(!meetError.get());
  }

  @Test
  public void testActiveBackupDatabaseAfterFirstRoundActiveBackupBeforeReport() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 3000; // ms
    int maxBackupCount = 3;
    InstanceStore instanceStore = mock(InstanceStore.class);
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, instanceStore, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);
    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    final DataNodeService.Iface dataNodeClient1 = mock(DataNodeService.Iface.class);
    final DataNodeService.Iface dataNodeClient2 = mock(DataNodeService.Iface.class);
    final DataNodeService.Iface dataNodeClient3 = mock(DataNodeService.Iface.class);

    Instance datanode1 = mock(Instance.class);
    Instance datanode2 = mock(Instance.class);
    Instance datanode3 = mock(Instance.class);
    final EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    final EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
    final EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
    Group group1 = new Group(1);
    Group group2 = new Group(2);
    Group group3 = new Group(3);
    when(datanode1.getGroup()).thenReturn(group1);
    when(datanode2.getGroup()).thenReturn(group2);
    when(datanode3.getGroup()).thenReturn(group3);
    Set<Instance> okDatanodes = new HashSet<>();
    okDatanodes.add(datanode1);
    okDatanodes.add(datanode2);
    okDatanodes.add(datanode3);
    when(instanceStore.getAll(PyService.DATANODE.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(okDatanodes);

    when(dataNodeClientFactory
        .generateSyncClient(eq(endPoint1), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient1);
    when(dataNodeClientFactory
        .generateSyncClient(eq(endPoint2), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient2);
    when(dataNodeClientFactory
        .generateSyncClient(eq(endPoint3), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient3);

    when(datanode1.getEndPoint()).thenReturn(endPoint1);
    when(datanode2.getEndPoint()).thenReturn(endPoint2);
    when(datanode3.getEndPoint()).thenReturn(endPoint3);

    // test first round
    long sequenceId1 = 10;
    ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    long sequenceId2 = 9;
    final ReportDbRequestThrift reportRequest2 = TestUtils
        .buildReportDbRequest(2, endPoint2, sequenceId2);

    long sequenceId3 = 11;
    final ReportDbRequestThrift reportRequest3 = TestUtils
        .buildReportDbRequest(3, endPoint3, sequenceId3);

    GetDbInfoResponseThrift getDbInfoResponse1 = new GetDbInfoResponseThrift();
    getDbInfoResponse1.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse1.setDbInfo(reportRequest1);
    when(dataNodeClient1.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse1);

    GetDbInfoResponseThrift getDbInfoResponse2 = new GetDbInfoResponseThrift();
    getDbInfoResponse2.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse2.setDbInfo(reportRequest2);
    when(dataNodeClient2.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse2);

    GetDbInfoResponseThrift getDbInfoResponse3 = new GetDbInfoResponseThrift();
    getDbInfoResponse3.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse3.setDbInfo(reportRequest3);
    when(dataNodeClient3.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse3);

    logger.warn("Process reportRequest1.");
    backupDbManager.process(reportRequest1);

    backupDbManager.backupDatabase();

    logger.warn("Process reportRequest2.");
    ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest2);

    assertEquals(sequenceId1, reportResponseThrift.getSequenceId());
    logger.warn("Process reportRequest3.");
    backupDbManager.process(reportRequest3);

    // test after first round, biggest sequence id report, but not accept
    Thread.sleep(roundTimeInterval + 1);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());

    backupDbManager.backupDatabase();

    // now all datanode will have reportRequest3 database info
    when(dataNodeClient1.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse3);
    when(dataNodeClient2.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse3);
    when(dataNodeClient3.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse3);

    long sequenceId4 = 14;
    EndPoint endPoint4 = new EndPoint("10.0.1.4", 1234);
    ReportDbRequestThrift reportRequest4 = TestUtils
        .buildReportDbRequest(4, endPoint4, sequenceId4);

    /*
     * in this process, manager will save request3 to database
     */
    logger.warn("Process reportRequest4.");
    reportResponseThrift = backupDbManager.process(reportRequest4);

    long backupRoundRefreshAmount = 2;

    long afterFirstRoundSequenceId = sequenceId3 + backupRoundRefreshAmount;
    assertEquals(afterFirstRoundSequenceId,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    assertEquals(1, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(6, ((BackupDbManagerImpl) backupDbManager).getLastRecordCount());
    // test save to db and load from db
    TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);
  }

  @Test
  public void testActiveBackupDatabaseAfterFirstRoundReportBeforeActiveBackup() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 3000; // ms
    int maxBackupCount = 3;
    InstanceStore instanceStore = mock(InstanceStore.class);
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, instanceStore, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    final DataNodeService.Iface dataNodeClient1 = mock(DataNodeService.Iface.class);
    final DataNodeService.Iface dataNodeClient2 = mock(DataNodeService.Iface.class);
    final DataNodeService.Iface dataNodeClient3 = mock(DataNodeService.Iface.class);

    Instance datanode1 = mock(Instance.class);
    Instance datanode2 = mock(Instance.class);
    Instance datanode3 = mock(Instance.class);
    final EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    final EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
    final EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
    Group group1 = new Group(1);
    Group group2 = new Group(2);
    Group group3 = new Group(3);
    when(datanode1.getGroup()).thenReturn(group1);
    when(datanode2.getGroup()).thenReturn(group2);
    when(datanode3.getGroup()).thenReturn(group3);
    Set<Instance> okDatanodes = new HashSet<>();
    okDatanodes.add(datanode1);
    okDatanodes.add(datanode2);
    okDatanodes.add(datanode3);
    when(instanceStore.getAll(PyService.DATANODE.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(okDatanodes);

    when(dataNodeClientFactory
        .generateSyncClient(eq(endPoint1), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient1);
    when(dataNodeClientFactory
        .generateSyncClient(eq(endPoint2), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient2);
    when(dataNodeClientFactory
        .generateSyncClient(eq(endPoint3), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient3);

    when(datanode1.getEndPoint()).thenReturn(endPoint1);
    when(datanode2.getEndPoint()).thenReturn(endPoint2);
    when(datanode3.getEndPoint()).thenReturn(endPoint3);

    // test first round
    long sequenceId1 = 10;
    ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    long sequenceId2 = 9;
    final ReportDbRequestThrift reportRequest2 = TestUtils
        .buildReportDbRequest(2, endPoint2, sequenceId2);

    long sequenceId3 = 11;
    final ReportDbRequestThrift reportRequest3 = TestUtils
        .buildReportDbRequest(3, endPoint3, sequenceId3);

    GetDbInfoResponseThrift getDbInfoResponse1 = new GetDbInfoResponseThrift();
    getDbInfoResponse1.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse1.setDbInfo(reportRequest1);
    when(dataNodeClient1.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse1);

    GetDbInfoResponseThrift getDbInfoResponse2 = new GetDbInfoResponseThrift();
    getDbInfoResponse2.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse2.setDbInfo(reportRequest2);
    when(dataNodeClient2.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse2);

    GetDbInfoResponseThrift getDbInfoResponse3 = new GetDbInfoResponseThrift();
    getDbInfoResponse3.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse3.setDbInfo(reportRequest3);
    when(dataNodeClient3.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse3);

    backupDbManager.process(reportRequest1);

    backupDbManager.backupDatabase();

    ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest2);

    assertEquals(sequenceId1, reportResponseThrift.getSequenceId());
    backupDbManager.process(reportRequest3);

    // test after first round, biggest sequence id report, but not accept
    Thread.sleep(roundTimeInterval);
    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());

    long sequenceId4 = 14;
    EndPoint endPoint4 = new EndPoint("10.0.1.4", 1234);
    ReportDbRequestThrift reportRequest4 = TestUtils
        .buildReportDbRequest(4, endPoint4, sequenceId4);

    /*
     * in this process, manager will save request3 to database
     */
    reportResponseThrift = backupDbManager.process(reportRequest4);

    long reportRoundRefreshAmount = 1;

    long afterFirstRoundSequenceId = sequenceId3 + reportRoundRefreshAmount;
    assertEquals(afterFirstRoundSequenceId,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    assertEquals(afterFirstRoundSequenceId, reportResponseThrift.getSequenceId());
    assertEquals(1, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(3, ((BackupDbManagerImpl) backupDbManager).getLastRecordCount());
    // test save to db and load from db
    TestUtils.compareReportRequestAndReportResponse(reportRequest3, reportResponseThrift);

    backupDbManager.backupDatabase();

    long backupRoundRefreshAmount = 2;
    afterFirstRoundSequenceId += backupRoundRefreshAmount;
    assertEquals(afterFirstRoundSequenceId,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    assertEquals(7, ((BackupDbManagerImpl) backupDbManager).getLastRecordCount());
  }

  @Test
  public void testUpdateSomeDbWhenReportDbRequest() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 1000; // ms
    int maxBackupCount = 3;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory
        .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient);

    long sequenceId1 = 10;
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
    getDbInfoResponse.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse.setDbInfo(reportRequest1);
    when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse);

    backupDbManager.process(reportRequest1);

    assertEquals(0, ((BackupDbManagerImpl) backupDbManager).getRecordCount());
    assertEquals(sequenceId1,
        ((BackupDbManagerImpl) backupDbManager).getRoundSequenceId().longValue());
    Thread.sleep(roundTimeInterval + 100);

    ReportDbRequestThrift reportRequest = TestUtils
        .buildReportDbRequest(2, endPoint1, sequenceId1);
    ReportDbResponseThrift reportResponseThrift = backupDbManager.process(reportRequest);
    assertEquals(sequenceId1 + 1, reportResponseThrift.getSequenceId());
    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);

    // clear all domains in memory and database
    for (Domain domain : domainStore.listAllDomains()) {
      domainStore.deleteDomain(domain.getDomainId());
    }
    assertEquals(0, domainStore.listAllDomains().size());
    // save new domains to database
    int newDomainCount = 5;
    List<DomainThrift> newDomainThriftList = new ArrayList<>();
    for (int i = 0; i < newDomainCount; i++) {
      Domain newDomain = TestUtils.buildDomain();
      newDomainThriftList.add(RequestResponseHelper.buildDomainThriftFrom(newDomain));
      domainStore.saveDomain(newDomain);
    }
    assertEquals(newDomainCount, domainStore.listAllDomains().size());
    assertEquals(newDomainCount, newDomainThriftList.size());
    // update these new domains to report request for compare
    reportRequest1.setDomainThriftList(newDomainThriftList);

    // report Db request again
    reportRequest = TestUtils.buildReportDbRequest(3, endPoint1, sequenceId1);
    reportResponseThrift = backupDbManager.process(reportRequest);
    assertEquals(reportResponseThrift.getSequenceId(), sequenceId1 + 1);
    assertTrue(((BackupDbManagerImpl) backupDbManager).getRecordCount() == 2);
    TestUtils.compareReportRequestAndReportResponse(reportRequest1, reportResponseThrift);
  }

  @Test
  public void testBackupDbManagerPassedRecoveryTime() throws Exception {
    for (int i = 0; i < 20; i++) {
      // init config for backup Db manager
      long roundTimeInterval = 500; // ms
      int maxBackupCount = 3;
      BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
          volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
          capacityRecordStore, accountStore,
          apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
          iscsiAccessRuleStore,
          ioLimitationStore, migrationRuleStore, rebalanceRuleStore, volumeDelayStore,
          volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);
      long sequenceId1 = 10;
      EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
      final ReportDbRequestThrift reportRequest1 = TestUtils
          .buildReportDbRequest(1, endPoint1, sequenceId1);

      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
          GenericThriftClientFactory.class);
      ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
      DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
      when(dataNodeClientFactory
          .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
          .thenReturn(dataNodeClient);

      GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
      getDbInfoResponse.setRequestId(RequestIdBuilder.get());
      getDbInfoResponse.setDbInfo(reportRequest1);
      when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
          .thenReturn(getDbInfoResponse);

      logger.debug("loop time:{}", i);
      assertTrue(!backupDbManager.passedRecoveryTime());

      Thread.sleep(800);

      // round 0 over, round = 0
      backupDbManager.process(reportRequest1);

      assertTrue(!backupDbManager.passedRecoveryTime());

      Thread.sleep(800);
      // round 0 over, round = 1
      backupDbManager.process(reportRequest1);

      assertTrue(!backupDbManager.passedRecoveryTime());

      Thread.sleep(800);
      // round 1 over, round = 2
      backupDbManager.process(reportRequest1);

      assertTrue(backupDbManager.passedRecoveryTime());
    }
  }

  @Test
  public void testBackupDbManagerForCsiInfo() throws Exception {
    // init config for backup Db manager
    long roundTimeInterval = 500; // ms
    int maxBackupCount = 3;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore, iscsiAccessRuleStore,
        ioLimitationStore, migrationRuleStore, rebalanceRuleStore, volumeDelayStore,
        volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);

    long sequenceId1 = 10;
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    final ReportDbRequestThrift reportRequest1 = TestUtils
        .buildReportDbRequest(1, endPoint1, sequenceId1);

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory = mock(
        GenericThriftClientFactory.class);
    ((BackupDbManagerImpl) backupDbManager).setDataNodeClientFactory(dataNodeClientFactory);
    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory
        .generateSyncClient(any(EndPoint.class), any(Long.class), any(Integer.class)))
        .thenReturn(dataNodeClient);

    GetDbInfoResponseThrift getDbInfoResponse = new GetDbInfoResponseThrift();
    getDbInfoResponse.setRequestId(RequestIdBuilder.get());
    getDbInfoResponse.setDbInfo(reportRequest1);
    when(dataNodeClient.getDbInfo(any(GetDbInfoRequestThrift.class)))
        .thenReturn(getDbInfoResponse);

    assertTrue(!backupDbManager.passedRecoveryTime());
    Thread.sleep(800);

    // round 0 over, round = 0
    backupDbManager.process(reportRequest1);
    assertTrue(!backupDbManager.passedRecoveryTime());

    Thread.sleep(800);
    // round 0 over, round = 1,save to db
    backupDbManager.process(reportRequest1);
    assertTrue(!backupDbManager.passedRecoveryTime());
    //check in db

    Thread.sleep(800);
    // round 1 over, round = 2
    backupDbManager.process(reportRequest1);

    assertTrue(backupDbManager.passedRecoveryTime());
  }

  @Test
  public void testSaveOrUpdateServerNode() {
    ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");

    DiskInfo diskInfo = generateDiskInfo();
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    diskInfoSet.add(diskInfo);
    serverNode.setDiskInfoSet(diskInfoSet);

    serverNodeStore.saveOrUpdateServerNode(serverNode);

    List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();
    System.out.println(serverNodeList.get(0));
    assertEquals(1, serverNodeList.size());

    diskInfo = generateDiskInfo();
    diskInfoSet.clear();
    diskInfoSet.add(diskInfo);
    serverNode.setDiskInfoSet(diskInfoSet);
    serverNodeStore.saveOrUpdateServerNode(serverNode);
    serverNodeList = serverNodeStore.listAllServerNodes();
    System.out.println(serverNodeList.get(0));
    assertEquals(1, serverNodeList.get(0).getDiskInfoSet().size());
  }

  @Test
  public void testDeleteServerNodes() {
    ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");
    DiskInfo diskInfo = generateDiskInfo();
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    diskInfoSet.add(diskInfo);
    serverNode.setDiskInfoSet(diskInfoSet);
    serverNodeStore.saveOrUpdateServerNode(serverNode);

    assertEquals(1, serverNodeStore.getCountTotle());
    serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");
    serverNodeStore.saveOrUpdateServerNode(serverNode);
    assertEquals(2, serverNodeStore.getCountTotle());

    List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();
    List<String> ids = new ArrayList<>();
    for (ServerNode serverNode1 : serverNodeList) {
      ids.add(serverNode1.getId());
    }
    serverNodeStore.deleteServerNodes(ids);
    assertEquals(0, serverNodeStore.getCountTotle());
  }

  @Test
  public void testUpdateServerNode() {
    ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");
    serverNodeStore.saveOrUpdateServerNode(serverNode);
    assertEquals(1, serverNodeStore.getCountTotle());
    serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB", "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");
    serverNodeStore.saveOrUpdateServerNode(serverNode);
    assertEquals(2, serverNodeStore.getCountTotle());

    List<ServerNode> serverNodeList = serverNodeStore.listAllServerNodes();

    String updatedInfo = "updatedCpuInfo";
    ServerNode serverNode1 = serverNodeList.get(0);
    serverNode1.setCpuInfo(updatedInfo);
    serverNodeStore.updateServerNode(serverNode1);

    List<ServerNode> serverNodeList1 = serverNodeStore.listAllServerNodes();
    int count = 0;
    for (ServerNode serverNode2 : serverNodeList1) {
      if (serverNode2.getId().equals(serverNode1.getId())) {
        assertEquals(serverNode2.getCpuInfo(), updatedInfo);
      } else {
        count++;
      }
    }
    assertEquals(1, count);
  }

  @Test
  public void testListServerNodes() {
    ServerNode serverNode1 = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rack-001", "001");
    final ServerNode serverNode2 = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4",
        "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rack-001", "002");
    final ServerNode serverNode3 = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4",
        "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rack-001", "003");
    final ServerNode serverNode4 = generateServerNode("Intel® Core™ i7-7500 CPU @ 3.40GHz × 4",
        "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rack-002", "001");
    final ServerNode serverNode5 = generateServerNode("Intel® Core™ i7-7500 CPU @ 3.40GHz × 4",
        "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rack-002", "002");
    final ServerNode serverNode6 = generateServerNode("Intel® Pentium™  CPU @ 3.40GHz × 4",
        "1.2 tB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "xx:1B:0D:Db:C3:48", "10.0.0.2", "manageIpTest",
        "storeIpTest",
        "rack-002", "003");

    DiskInfo diskInfo = generateDiskInfo();
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    diskInfoSet.add(diskInfo);
    serverNode1.setDiskInfoSet(diskInfoSet);

    serverNodeStore.saveOrUpdateServerNode(serverNode1);
    serverNodeStore.saveOrUpdateServerNode(serverNode2);
    serverNodeStore.saveOrUpdateServerNode(serverNode3);
    serverNodeStore.saveOrUpdateServerNode(serverNode4);
    serverNodeStore.saveOrUpdateServerNode(serverNode5);
    serverNodeStore.saveOrUpdateServerNode(serverNode6);

    String s = null;

    assertEquals(6, serverNodeStore.getCountTotle());

    List<ServerNode> serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, null, null,
            "rack-001", null);
    assertEquals(3, serverNodeList.size());

    serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, null, null,
            null, "002");
    assertEquals(2, serverNodeList.size());

    serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, null, null,
            "rack-001", "002");
    assertEquals(1, serverNodeList.size());

    serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, "core", null, null, null, null, null, null,
            null, null);
    assertEquals(5, serverNodeList.size());

    serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, null, null, "1.2 T", null, null, null, null,
            null, null);
    assertEquals(1, serverNodeList.size());

    serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, null, null, "1.2 T", "Xx", null, null, null,
            null, null);
    assertEquals(1, serverNodeList.size());

    serverNodeList = serverNodeStore
        .listServerNodes(0, 10, null, null, null, null, null, null, null, null, null, "0.0.2",
            "storeIptest", null, null);
    assertEquals(1, serverNodeList.size());

  }

  @Test
  public void testGetServerNodeByIp() {
    ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");
    serverNode.setNetworkCardInfoName("10.0.2.231##172.16.10.231");
    serverNodeStore.saveOrUpdateServerNode(serverNode);

    ServerNode serverNodeByIp = serverNodeStore.getServerNodeByIp("10.0.2.231");

    assertEquals(serverNode.getId(), serverNodeByIp.getId());
  }

  @Test
  public void testListDiskInfo() {

    ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");

    DiskInfo diskInfo = generateDiskInfo();
    diskInfo.setServerNode(serverNode);
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    diskInfoSet.add(diskInfo);
    serverNode.setDiskInfoSet(diskInfoSet);

    serverNodeStore.saveOrUpdateServerNode(serverNode);

    assertEquals(1, diskInfoStore.listDiskInfos().size());

    DiskInfo diskInfoFromDb = diskInfoStore.listDiskInfoById(diskInfo.getId());

    assertEquals(diskInfo.toString(), diskInfoFromDb.toString());
  }

  @Test
  public void testUpdateDiskInfo() {
    ServerNode serverNode = generateServerNode("Intel® Core™ i5-7500 CPU @ 3.40GHz × 4", "1.1 TB",
        "31.3 GiB",
        "HP DL360G6 E5506 PROMO 9018AP Server", "1C:1B:0D:Db:C3:48", "10.0.0.1", "manageIp",
        "storeIp",
        "rackNo", "slotNo");

    DiskInfo diskInfo = generateDiskInfo();
    diskInfo.setServerNode(serverNode);
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    diskInfoSet.add(diskInfo);
    serverNode.setDiskInfoSet(diskInfoSet);

    serverNodeStore.saveOrUpdateServerNode(serverNode);

    assertEquals(1, diskInfoStore.listDiskInfos().size());

    assertEquals(null, diskInfoStore.listDiskInfoById(diskInfo.getId()).getSwith());

    diskInfoStore
        .updateDiskInfoLightStatusById(diskInfo.getId(), DiskInfoLightStatus.ON.toString());

    assertEquals(DiskInfoLightStatus.ON.toString(),
        diskInfoStore.listDiskInfoById(diskInfo.getId()).getSwith());
  }

  @Test
  public void testRecoverDbSentryStore() {
    List<RecoverDbSentry> beforeSave = recoverDbSentryStore.list();
    assertTrue(beforeSave.isEmpty());

    recoverDbSentryStore.saveOrUpdate(new RecoverDbSentry(1));

    List<RecoverDbSentry> afterSave = recoverDbSentryStore.list();
    assertEquals(1, afterSave.size());
  }

  @Test
  public void testNeedRecoverDb() {
    long roundTimeInterval = 3000; // ms
    int maxBackupCount = 3;
    BackupDbManager backupDbManager = new BackupDbManagerImpl(roundTimeInterval, maxBackupCount,
        volumeRuleRelationshipStore, accessRuleStore, domainStore, storagePoolStore,
        capacityRecordStore, accountStore,
        apiStore, roleStore, resourceStore, null, iscsiRuleRelationshipStore,
        iscsiAccessRuleStore, ioLimitationStore, migrationRuleStore, rebalanceRuleStore,
        volumeDelayStore, volumeRecycleStore, volumeJobStoreDb, recoverDbSentryStore);
    boolean needRecoverDbBoolean = backupDbManager.needRecoverDb();
    assertTrue(needRecoverDbBoolean);
    recoverDbSentryStore.saveOrUpdate(new RecoverDbSentry(1));

    boolean needRecoverAfterRecoverDbNotNull = backupDbManager.needRecoverDb();
    assertFalse(needRecoverAfterRecoverDbNotNull);
  }

  private ServerNode generateServerNode(String cpuInfo, String diskInfo, String memoryInfo,
      String modelInfo,
      String networkCardInfo, String gatewayIp, String manageIp, String storeIp, String rackNo,
      String slotNo) {
    ServerNode serverNode = new ServerNode();
    serverNode.setId(UUID.randomUUID().toString());
    serverNode.setCpuInfo(cpuInfo);
    serverNode.setDiskInfo(diskInfo);
    serverNode.setMemoryInfo(memoryInfo);
    serverNode.setModelInfo(modelInfo);
    serverNode.setNetworkCardInfo(networkCardInfo);
    serverNode.setGatewayIp(gatewayIp);
    serverNode.setManageIp(manageIp);
    serverNode.setStoreIp(storeIp);
    serverNode.setRackNo(rackNo);
    serverNode.setSlotNo(slotNo);
    return serverNode;
  }

  private DiskInfo generateDiskInfo() {
    DiskInfo diskInfo = new DiskInfo();
    diskInfo.setId(String.valueOf(RequestIdBuilder.get()));
    diskInfo.setSn(String.valueOf(RequestIdBuilder.get()));
    diskInfo.setName("sda");
    diskInfo.setVendor("vendor");
    diskInfo.setSsdOrHdd("hdd");
    diskInfo.setRate(65535);
    diskInfo.setModel("model");
    diskInfo.setSerialNumber("0101019230");

    return diskInfo;
  }


  @After
  public void cleanUp() {
    //volumeDelayStore
    volumeDelayStore.clearAll();
    volumeRecycleStore.clearAll();
    recoverDbSentryStore.clearAll();

    driverClientStore.clearMemoryData();
    assertTrue(driverClientStore.list().isEmpty());
  }
}
