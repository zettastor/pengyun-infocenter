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

package py.infocenter.test.base;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.icshare.CapacityRecordStore;
import py.icshare.CapacityRecordStoreImpl;
import py.icshare.DomainDbStore;
import py.icshare.DomainStore;
import py.icshare.DomainStoreImpl;
import py.icshare.RecoverDbSentryStore;
import py.icshare.RecoverDbSentryStoreImpl;
import py.icshare.StoragePoolStoreImpl;
import py.icshare.authorization.ApiDbStoreImpl;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ResourceDbStoreImpl;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.RoleDbStoreImpl;
import py.icshare.authorization.RoleStore;
import py.icshare.qos.IoLimitationRelationshipStore;
import py.icshare.qos.IoLimitationRelationshipStoreImpl;
import py.icshare.qos.IoLimitationStoreImpl;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.MigrationRuleStoreImpl;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.driver.client.manger.DriverClientManger;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.AccessRuleStoreImpl;
import py.infocenter.store.ArchiveStore;
import py.infocenter.store.ArchiveStoreImpl;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.DiskInfoStore;
import py.infocenter.store.DiskInfoStoreImpl;
import py.infocenter.store.DriverClientDbStore;
import py.infocenter.store.DriverClientStore;
import py.infocenter.store.DriverClientStoreImpl;
import py.infocenter.store.DriverDbStore;
import py.infocenter.store.DriverStore;
import py.infocenter.store.DriverStoreImpl;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.InstanceVolumesInformationStoreImpl;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiAccessRuleStoreImpl;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.IscsiRuleRelationshipStoreImpl;
import py.infocenter.store.ScsiDriverStore;
import py.infocenter.store.ScsiDriverStoreImpl;
import py.infocenter.store.SegmentStore;
import py.infocenter.store.SegmentStoreImpl;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.ServerNodeStoreImpl;
import py.infocenter.store.StorageDbStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.StorageStoreImpl;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeDelayStoreImpl;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRecycleStoreImpl;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeRuleRelationshipStoreImpl;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.OperationStoreImpl;
import py.infocenter.store.control.VolumeJobProcess;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.store.control.VolumeJobStoreImpl;
import py.informationcenter.StoragePoolDbStore;
import py.informationcenter.StoragePoolStore;
import py.io.qos.IoLimitationStore;

@Configuration
@ImportResource({"classpath:spring-config/hibernate.xml"})
public class InformationCenterAppConfigTest {

  @Autowired
  private SessionFactory sessionFactory;


  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }


  @Bean
  InstanceVolumesInformationStore instanceVolumesInformationStore() {
    InstanceVolumesInformationStoreImpl instanceVolumesInformationStore =
        new InstanceVolumesInformationStoreImpl();
    instanceVolumesInformationStore.setSessionFactory(sessionFactory);
    return instanceVolumesInformationStore;
  }


  @Bean
  public VolumeJobProcess volumeJobProcess() throws Exception {
    VolumeJobProcess volumeJobProcess = new VolumeJobProcess();
    volumeJobProcess.setVolumeJobStoredb(volumeJobStoreDb());
    return volumeJobProcess;
  }


  @Bean
  public VolumeStore dbVolumeStore() {
    DbVolumeStoreImpl volumeStoreImpl = new DbVolumeStoreImpl();
    volumeStoreImpl.setSessionFactory(sessionFactory);
    return volumeStoreImpl;
  }


  @Bean
  public VolumeDelayStore volumeDelayStore() {
    VolumeDelayStoreImpl volumeDelayStore = new VolumeDelayStoreImpl();
    volumeDelayStore.setSessionFactory(sessionFactory);
    return volumeDelayStore;
  }


  @Bean
  public VolumeRecycleStore volumeRecycleStore() {
    VolumeRecycleStoreImpl volumeRecycleStore = new VolumeRecycleStoreImpl();
    volumeRecycleStore.setSessionFactory(sessionFactory);
    return volumeRecycleStore;
  }


  @Bean
  public ScsiDriverStore scsiDriverStore() {
    ScsiDriverStoreImpl scsiDriverStore = new ScsiDriverStoreImpl();
    scsiDriverStore.setSessionFactory(sessionFactory);
    return scsiDriverStore;
  }


  @Bean
  public DriverClientStore driverClientStore() throws Exception {
    DriverClientStoreImpl driverClientStore = new DriverClientStoreImpl();
    driverClientStore.setSessionFactory(sessionFactory);
    return driverClientStore;
  }


  @Bean
  public DriverClientDbStore driverClientDbStore() throws Exception {
    DriverClientStoreImpl driverClientStore = new DriverClientStoreImpl();
    driverClientStore.setSessionFactory(sessionFactory);
    return driverClientStore;
  }



  @Bean
  public DriverClientManger driverClientManger() throws Exception {
    DriverClientManger driverClientManger = new DriverClientManger(driverClientStore(), 10, 30);
    return driverClientManger;
  }


  @Bean
  public StorageDbStore dbStorageStore() {
    StorageStoreImpl storageStoreImpl = new StorageStoreImpl();
    storageStoreImpl.setSessionFactory(sessionFactory);
    return storageStoreImpl;
  }


  @Bean
  @Qualifier("memory")
  public StorageStore dbStorageStore1() {
    StorageStoreImpl storageStoreImpl = new StorageStoreImpl();
    storageStoreImpl.setSessionFactory(sessionFactory);
    storageStoreImpl.setArchiveStore(dbArchiveStore());
    return storageStoreImpl;
  }


  @Bean
  public ArchiveStore dbArchiveStore() {
    ArchiveStoreImpl archiveStoreImpl = new ArchiveStoreImpl();
    archiveStoreImpl.setSessionFactory(sessionFactory);
    return archiveStoreImpl;
  }


  @Bean
  public DriverDbStore dbDriverStore() {
    DriverStoreImpl driverStore = new DriverStoreImpl();
    driverStore.setSessionFactory(sessionFactory);
    return driverStore;
  }


  @Bean
  public DriverStore driverStore() {
    DriverStoreImpl driverStore = new DriverStoreImpl();
    driverStore.setSessionFactory(sessionFactory);
    return driverStore;
  }


  @Bean
  public SegmentStore dbSegmentStore() {
    SegmentStoreImpl segmentStore = new SegmentStoreImpl();
    segmentStore.setSessionFactory(sessionFactory);
    return segmentStore;
  }


  @Bean
  public VolumeRuleRelationshipStore dbVolumeRuleRelationshipStore() {
    VolumeRuleRelationshipStoreImpl volumeRuleRelationshipStore =
        new VolumeRuleRelationshipStoreImpl();
    volumeRuleRelationshipStore.setSessionFactory(sessionFactory);
    return volumeRuleRelationshipStore;
  }


  @Bean
  public AccessRuleStore dbAccessRuleStore() {
    AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
    accessRuleStore.setSessionFactory(sessionFactory);
    return accessRuleStore;
  }


  @Bean
  public IscsiRuleRelationshipStore dbIscsiRuleRelationshipStore() {
    IscsiRuleRelationshipStoreImpl iscsiRuleRelationshipStore =
        new IscsiRuleRelationshipStoreImpl();
    iscsiRuleRelationshipStore.setSessionFactory(sessionFactory);
    return iscsiRuleRelationshipStore;
  }


  @Bean
  public IscsiAccessRuleStore dbIscsiAccessRuleStore() {
    IscsiAccessRuleStoreImpl iscsiAccessRuleStore = new IscsiAccessRuleStoreImpl();
    iscsiAccessRuleStore.setSessionFactory(sessionFactory);
    return iscsiAccessRuleStore;
  }


  @Bean
  public IoLimitationStore ioLimitationStore() {
    IoLimitationStoreImpl ioLimitationStore = new IoLimitationStoreImpl();
    ioLimitationStore.setSessionFactory(sessionFactory);
    return ioLimitationStore;
  }


  @Bean
  public IoLimitationRelationshipStore ioLimitationRelationshipStore() {
    IoLimitationRelationshipStoreImpl ioLimitationRelationshipStore =
        new IoLimitationRelationshipStoreImpl();
    ioLimitationRelationshipStore.setSessionFactory(sessionFactory);
    return ioLimitationRelationshipStore;
  }


  @Bean
  public MigrationRuleStore migrationRuleStore() {
    MigrationRuleStoreImpl migrationSpeedRuleStore = new MigrationRuleStoreImpl();
    migrationSpeedRuleStore.setSessionFactory(sessionFactory);
    return migrationSpeedRuleStore;
  }


  @Bean
  public RebalanceRuleStore rebalanceRuleStore() {
    RebalanceRuleStoreImpl rebalanceRuleStore = new RebalanceRuleStoreImpl();
    rebalanceRuleStore.setSessionFactory(sessionFactory);
    return rebalanceRuleStore;
  }


  @Bean
  public DomainDbStore dbDomainStore() {
    DomainStoreImpl domainStore = new DomainStoreImpl();
    domainStore.setSessionFactory(sessionFactory);
    return domainStore;
  }


  @Bean
  @Qualifier("memory")
  public DomainStore domainStore() {
    DomainStoreImpl domainStore = new DomainStoreImpl();
    domainStore.setSessionFactory(sessionFactory);
    return domainStore;
  }


  @Bean
  public StoragePoolDbStore dbStoragePoolStore() {
    StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
    storagePoolStore.setSessionFactory(sessionFactory);
    return storagePoolStore;
  }


  @Bean
  public StoragePoolStore storagePoolStore() {
    StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
    storagePoolStore.setSessionFactory(sessionFactory);
    return storagePoolStore;
  }


  @Bean
  public CapacityRecordStore dbCapacityRecordStore() {
    CapacityRecordStoreImpl capacityRecordStore = new CapacityRecordStoreImpl();
    capacityRecordStore.setSessionFactory(sessionFactory);
    return capacityRecordStore;
  }


  @Bean
  public CapacityRecordStore capacityRecordStore() {
    CapacityRecordStoreImpl capacityRecordStore = new CapacityRecordStoreImpl();
    capacityRecordStore.setSessionFactory(sessionFactory);
    return capacityRecordStore;
  }


  @Bean
  ServerNodeStore serverNodeStore() {
    ServerNodeStoreImpl serverNodeStore = new ServerNodeStoreImpl();
    serverNodeStore.setSessionFactory(sessionFactory);
    return serverNodeStore;
  }


  @Bean
  DiskInfoStore diskInfoStore() {
    DiskInfoStoreImpl diskInfoStore = new DiskInfoStoreImpl();
    diskInfoStore.setSessionFactory(sessionFactory);
    return diskInfoStore;
  }


  @Bean
  public py.icshare.authorization.AccountStore inMemAccountStore() {
    py.icshare.authorization.InMemoryAccountStoreImpl inMemoryAccountStore =
        new py.icshare.authorization.InMemoryAccountStoreImpl();
    inMemoryAccountStore.setAccountStore(dbAccountStore());
    return inMemoryAccountStore;
  }


  @Bean
  public py.icshare.authorization.AccountStore dbAccountStore() {
    py.icshare.authorization.AccountStoreDbImpl dbAccountStore =
        new py.icshare.authorization.AccountStoreDbImpl();
    dbAccountStore.setSessionFactory(sessionFactory);
    return dbAccountStore;
  }


  @Bean
  public RoleStore roleStore() {
    RoleDbStoreImpl dbStore = new RoleDbStoreImpl();
    dbStore.setSessionFactory(sessionFactory);
    return dbStore;
  }


  @Bean
  public ApiStore apiDbStore() {
    ApiDbStoreImpl apiDbStore = new ApiDbStoreImpl();
    apiDbStore.setSessionFactory(sessionFactory);
    return apiDbStore;
  }


  @Bean
  public ResourceStore resourceDbStore() {
    ResourceDbStoreImpl dbStore = new ResourceDbStoreImpl();
    dbStore.setSessionFactory(sessionFactory);
    return dbStore;
  }


  @Bean
  public VolumeJobStoreDb volumeJobStoreDb() throws Exception {
    VolumeJobStoreImpl volumeJobStoreImpl = new VolumeJobStoreImpl();
    volumeJobStoreImpl.setSessionFactory(sessionFactory);
    return volumeJobStoreImpl;
  }


  @Bean
  public OperationStore operationStore() {
    OperationStoreImpl operationStore = new OperationStoreImpl();
    operationStore.setSessionFactory(sessionFactory);
    return operationStore;
  }

  @Bean
  public RecoverDbSentryStore recoverDbSentryStore() {
    RecoverDbSentryStoreImpl recoverDbSentryStore = new RecoverDbSentryStoreImpl();
    recoverDbSentryStore.setSessionFactory(sessionFactory);
    return recoverDbSentryStore;
  }
}
