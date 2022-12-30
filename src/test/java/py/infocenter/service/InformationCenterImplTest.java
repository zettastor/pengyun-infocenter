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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeInAction.NULL;
import static py.volume.VolumeMetadata.VolumeSourceType.CREATE_VOLUME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import junit.framework.Assert;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.glassfish.grizzly.utils.ArraySet;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.RequestResponseHelper;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.icshare.AccessRuleInformation;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;
import py.icshare.DriverClientKeyInformation;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceInformation;
import py.icshare.InstanceMetadata;
import py.icshare.RecoverDbSentryStore;
import py.icshare.VolumeRuleRelationshipInformation;
import py.icshare.authorization.PyResource;
import py.icshare.exception.VolumeNotFoundException;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.InfoCenterConfiguration;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.driver.client.manger.DriverClientManger;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.service.selection.InstanceSelectionStrategy;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.DriverClientStore;
import py.infocenter.store.DriverStore;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.OrphanVolumeStore;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStatusTransitionStoreImpl;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.store.control.VolumeJobStoreImpl;
import py.infocenter.test.utils.TestBeans;
import py.infocenter.worker.ReportVolumeInfoSweeper;
import py.infocenter.worker.StorageStoreSweeper;
import py.infocenter.worker.VolumeActionSweeper;
import py.infocenter.worker.VolumeSweeper;
import py.informationcenter.AccessRuleStatus;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolLevel;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.querylog.eventdatautil.EventDataWorker;
import py.storage.EdRootpathSingleton;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.icshare.DeleteVolumeRequest;
import py.thrift.icshare.DeleteVolumeResponse;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.icshare.ListArchivesRequestThrift;
import py.thrift.icshare.ListArchivesResponseThrift;
import py.thrift.icshare.ListDriverClientInfoRequest;
import py.thrift.icshare.ListDriverClientInfoResponse;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.infocenter.service.CheckVolumeIsReadOnlyRequest;
import py.thrift.infocenter.service.ExtendVolumeRequest;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataRequest;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataResponse;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.AccountNotFoundExceptionThrift;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.CacheTypeThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverStatusThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsRequest;
import py.thrift.share.ListVolumeAccessRulesByVolumeIdsResponse;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.PortalTypeThrift;
import py.thrift.share.ReadWriteTypeThrift;
import py.thrift.share.RootVolumeNotFoundExceptionThrift;
import py.thrift.share.SegmentMetadataSwitchThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeBeingDeletedExceptionThrift;
import py.thrift.share.VolumeExistingExceptionThrift;
import py.thrift.share.VolumeInExtendingExceptionThrift;
import py.thrift.share.VolumeIsAppliedWriteAccessRuleExceptionThrift;
import py.thrift.share.VolumeIsMarkWriteExceptionThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.thrift.share.VolumeTypeThrift;
import py.volume.CacheType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class InformationCenterImplTest extends TestBase {

  @Autowired
  private SessionFactory sessionFactory;

  private InformationCenterImpl informationCenter;

  @Mock
  private DbVolumeStoreImpl dbVolumeStore;

  @Mock
  private SegmentUnitTimeoutStore timeoutStore;

  private VolumeStatusTransitionStore statusStore = new VolumeStatusTransitionStoreImpl();

  @Mock
  private InstanceStore instanceStore;

  @Mock
  private StorageStore storageStore;

  private TwoLevelVolumeStoreImpl volumeStore;
  @Mock
  private TwoLevelVolumeStoreImpl volumeStore1;

  @Mock
  private TwoLevelVolumeStoreImpl volumeStore2;

  @Mock
  private InstanceSelectionStrategy instanceSelectionStrategy;

  @Mock
  private OrphanVolumeStore orphanVolumeStore;

  @Mock
  private DomainStore domainStore;
  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private VolumeDelayStore volumeDelayStore;
  @Mock
  private VolumeRecycleStore volumeRecycleStore;

  @Mock
  private InstanceMaintenanceDbStore instanceMaintenanceDbStore;

  @Mock
  private InfoCenterAppContext appContext;

  private DriverStore driverStore;
  private AccessRuleStore accessRuleStore;
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;

  private long segmentSize = 100;

  private long deadVolumeToRemove = 15552000;

  private VolumeSweeper volumeSweeper = null;

  private VolumeActionSweeper volumeActionSweeper;

  private StorageStoreSweeper storageStoreSweeper;

  private VolumeInformationManger volumeInformationManger;

  @Mock
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;

  @Mock
  private PySecurityManager securityManager;

  @Mock
  private OperationStore operationStore;
  @Mock
  private ServerNodeStore serverNodeStore;

  @Mock
  private InfoCenterConfiguration infoCenterConfiguration;

  @Mock
  private InformationCenterClientFactory infoCenterClientFactory;

  @Mock
  private HaInstanceManger haInstanceManger;

  private ReportVolumeInfoSweeper reportVolumeInfoSweeper;

  @Mock
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  private ExceptionForOperation exceptionForOperation;

  private DriverClientStore driverClientStore = null;

  private DriverClientManger driverClientManger;

  @Mock
  private VolumeJobStoreImpl volumeJobStore;

  @Mock
  private VolumeJobStoreDb volumeJobStoreDb;

  
  @Before
  public void setup() {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(appContext.getInstanceId()).thenReturn(new InstanceId(100));

    ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);
    driverStore = ctx.getBean(DriverStore.class);
    accessRuleStore = ctx.getBean(AccessRuleStore.class);
    volumeRuleRelationshipStore = ctx.getBean(VolumeRuleRelationshipStore.class);
    driverClientStore = ctx.getBean(DriverClientStore.class);
    driverClientManger = ctx.getBean(DriverClientManger.class);

    volumeStore = new TwoLevelVolumeStoreImpl(new MemoryVolumeStoreImpl(), dbVolumeStore);
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();
    exceptionForOperation = new ExceptionForOperation();

    informationCenter = new InformationCenterImpl();
    informationCenter.setInstanceStore(instanceStore);
    informationCenter.setVolumeStore(volumeStore);
    informationCenter.setDriverStore(driverStore);
    informationCenter.setSegmentSize(segmentSize);
    informationCenter.setDeadVolumeToRemove(deadVolumeToRemove);
    informationCenter.setSegmentUnitTimeoutStore(timeoutStore);
    informationCenter.setVolumeStatusStore(statusStore);
    informationCenter.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setOrphanVolumes(orphanVolumeStore);
    informationCenter.setDomainStore(domainStore);
    informationCenter.setStoragePoolStore(storagePoolStore);
    informationCenter.setAccessRuleStore(accessRuleStore);
    informationCenter.setStorageStore(storageStore);
    informationCenter.setPageWrappCount(128);
    informationCenter.setSegmentWrappCount(10);
    informationCenter.setServerNodeStore(serverNodeStore);
    informationCenter.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setOperationStore(operationStore);
    informationCenter.setInfoCenterConfiguration(infoCenterConfiguration);
    informationCenter.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    informationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);
    informationCenter.setExceptionForOperation(exceptionForOperation);
    informationCenter.setDriverClientManger(driverClientManger);
    informationCenter.setVolumeDelayStore(volumeDelayStore);
    informationCenter.setVolumeRecycleStore(volumeRecycleStore);
    informationCenter.setVolumeJobStoreDb(volumeJobStoreDb);

    //update volume status and clone status
    volumeSweeper = new VolumeSweeper();
    volumeSweeper.setVolumeStatusTransitionStore(statusStore);
    volumeSweeper.setAppContext(appContext);
    volumeSweeper.setVolumeStore(volumeStore);
    volumeSweeper.setVolumeJobStoreDb(volumeJobStore);
    volumeSweeper.setOperationStore(operationStore);

    //RebalanceRuleStore
    RebalanceRuleStore rebalanceRuleStore = new RebalanceRuleStoreImpl();

    //SegmentUnitsDistributionManagerImpl
    final SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
        new SegmentUnitsDistributionManagerImpl(
            segmentSize, volumeStore, storageStore, storagePoolStore, rebalanceRuleStore,
            domainStore);

    //action
    volumeActionSweeper = new VolumeActionSweeper();
    volumeActionSweeper.setVolumeStore(volumeStore);
    volumeActionSweeper.setAppContext(appContext);
    volumeActionSweeper.setStoragePoolStore(storagePoolStore);
    volumeActionSweeper.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);
    volumeActionSweeper.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);

    //StorageStoreSweeper for update volume status
    storageStoreSweeper = new StorageStoreSweeper();
    storageStoreSweeper.setAppContext(appContext);
    storageStoreSweeper.setStoragePoolStore(storagePoolStore);
    storageStoreSweeper.setInstanceMetadataStore(storageStore);
    storageStoreSweeper.setDomainStore(domainStore);
    storageStoreSweeper.setVolumeStore(volumeStore);
    storageStoreSweeper.setInstanceMaintenanceDbStore(instanceMaintenanceDbStore);
    storageStoreSweeper.setWaitCollectVolumeInfoSecond(0);

    DriverContainerSelectionStrategy balancedDriverContainerSelectionStrategy =
        new BalancedDriverContainerSelectionStrategy();
    informationCenter.setDriverContainerSelectionStrategy(balancedDriverContainerSelectionStrategy);

    //VolumeInformationManger
    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setDriverStore(driverStore);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    volumeInformationManger.setInfoCenterClientFactory(infoCenterClientFactory);
    volumeInformationManger.setAppContext(appContext);

    informationCenter.setVolumeInformationManger(volumeInformationManger);
    volumeActionSweeper.setVolumeInformationManger(volumeInformationManger);

    EdRootpathSingleton.getInstance().setRootPath("/tmp/aaa/");
    //reportVolumeInfoSweeper
    reportVolumeInfoSweeper = new ReportVolumeInfoSweeper();

    //instanceVolumeInEquilibriumManger
    Pair<Boolean, Long> resultValue = new Pair<>();
    resultValue.setFirst(true);
    resultValue.setSecond(100L);
    when(instanceVolumeInEquilibriumManger
        .checkVolumeFormNewInstanceEquilibrium(anyLong(), anyLong())).thenReturn(resultValue);

    //clean
    logger.warn("begin to clear info");
    List<VolumeRuleRelationshipInformation> volumeRuleRelationshipInformationList =
        volumeRuleRelationshipStore
            .list();
    for (VolumeRuleRelationshipInformation volumeRuleRelationshipInformation :
        volumeRuleRelationshipInformationList) {
      volumeRuleRelationshipStore.deleteByRuleId(volumeRuleRelationshipInformation.getRuleId());
    }

    driverClientStore.clearMemoryData();
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    for (DriverClientInformation driverClientInformation : driverClientInformationList) {
      driverClientStore.deleteValue(driverClientInformation);
    }

    volumeStore.clearData();
    statusStore.clear();
  }

  @Test
  public void testSwitchVolumeForbuildThriftVolumeFromWhenGetVolume() throws Exception {
    final long volumeId = RequestIdBuilder.get();

    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 0; i < 4; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(new InstanceId((i + 1) * 24242424));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId((long) k * 1234567);
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archives.add(archive);
        archive.setWeight(1);
        archivesInDataNode.put(Long.valueOf(i), Long.valueOf(k));
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 3;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      List<InstanceId> arbiterInstanceList = new ArrayList<>();
      if (segmentIndex == 2) {
        arbiterInstanceList.add(instanceList.get(3).getInstanceId());
      } else {
        arbiterInstanceList.add(instanceList.get(2).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Arbiter, arbiterInstanceList);

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      if (segmentIndex == 2) {
        normalInstanceList.add(instanceList.get(2).getInstanceId());
      } else {
        normalInstanceList.add(instanceList.get(3).getInstanceId());
      }
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    VolumeMetadata volumeMetadata = createavolumeswitch(volumeSize, VolumeType.SMALL, volumeId,
        storagePoolId,
        domainId, segmentIndex2SegmentMap);

    //        logger.warn("get the volume is :" + volumeMetadata);
    VolumeMetadataThrift volumeMetadataThrift = RequestResponseHelper
        .buildThriftVolumeFrom(volumeMetadata, true);
    //        logger.warn("get the VolumeMetadataThrift is :" + volumeMetadataThrift);

    VolumeMetadata volumeMetadataSwitch = RequestResponseHelper
        .buildVolumeFrom(volumeMetadataThrift);
    logger.warn("get the volumeMetadataSwitch is :" + volumeMetadataSwitch);

  }

  /**
   * volume not exist, but after process first segment unit, it will produce a new volume;.
   */
  @Test
  public void testReportSegmentUnitMetadata_volumeNotExist() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
    VolumeMetadata saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertNull(saveVolume);
    // report a segment unit, it will create a new volume in info center
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Primary,
        1, 0);
    volumeStore.saveVolumeForReport(volume);
    informationCenter.reportSegmentUnitsMetadata(request);
    saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertNotNull(saveVolume);
    saveVolume.updateStatus();

    // continue to report other two secondary segment units
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    saveVolume.updateStatus();
    assertEquals(saveVolume.getSegments().size(), 1);
    assertTrue(saveVolume.isVolumeAvailable());
  }

  @Test
  public void testSwitchVolumeForbuildThriftVolumeFromWhenGetVolumePss() throws Exception {
    final long volumeId = RequestIdBuilder.get();
    List<InstanceMetadata> instanceList = new ArrayList<>();
    Long domainId = 10010L;
    Long storagePoolId = 10086L;
    Set<Long> storagePoolIdList = new HashSet<>();
    storagePoolIdList.add(storagePoolId);
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    storagePool.setArchivesInDataNode(archivesInDataNode);
    for (int i = 1; i < 5; i++) {
      Group group = new Group();
      group.setGroupId(i);

      InstanceMetadata instanceMetadata = new InstanceMetadata(
          new InstanceId(RequestIdBuilder.get()));
      instanceMetadata.setGroup(group);
      instanceMetadata.setCapacity(10 * segmentSize);
      instanceMetadata.setFreeSpace(instanceMetadata.getCapacity());
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);

      List<RawArchiveMetadata> archives = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId(RequestIdBuilder.get());
        archive.setStatus(ArchiveStatus.GOOD);
        archive.setStorageType(StorageType.SATA);
        archive.setStoragePoolId(storagePoolId);
        archive.setLogicalFreeSpace(3 * segmentSize);
        archive.setWeight(1);
        archives.add(archive);
        archivesInDataNode.put(instanceMetadata.getInstanceId().getId(), archive.getArchiveId());
      }
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      instanceList.add(instanceMetadata);
    }
    // new Domain
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setStoragePools(storagePoolIdList);

    // TestUtils.generateVolumeMetadata()
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(storageStore.list()).thenReturn(instanceList);
    for (InstanceMetadata instanceMetadata : instanceList) {
      when(storageStore.get(instanceMetadata.getInstanceId().getId())).thenReturn(instanceMetadata);
    }

    int volumeSize = 1000;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap =
        new HashMap<>();
    for (int segmentIndex = 0; segmentIndex < volumeSize; segmentIndex++) {
      Map<SegmentUnitTypeThrift, List<InstanceId>> segment = new HashMap<>();

      segment.put(SegmentUnitTypeThrift.Arbiter, new ArrayList<>());

      List<InstanceId> normalInstanceList = new ArrayList<>();
      normalInstanceList.add(instanceList.get(0).getInstanceId());
      normalInstanceList.add(instanceList.get(2).getInstanceId());
      normalInstanceList.add(instanceList.get(3).getInstanceId());
      segment.put(SegmentUnitTypeThrift.Normal, normalInstanceList);

      segmentIndex2SegmentMap.put(segmentIndex, segment);
    }

    VolumeMetadata volumeMetadata = createavolumeswitch(volumeSize, VolumeType.REGULAR, volumeId,
        storagePoolId,
        domainId, segmentIndex2SegmentMap);

    VolumeMetadataThrift volumeMetadataThrift = RequestResponseHelper
        .buildThriftVolumeFrom(volumeMetadata, true);
    //        logger.warn("get the VolumeMetadataThrift is :" + volumeMetadataThrift);

    VolumeMetadata volumeMetadataSwitch = RequestResponseHelper
        .buildVolumeFrom(volumeMetadataThrift);

    for (int i = 0; i < volumeSize; i++) {
      SegmentMetadata beforeSegment = volumeMetadata.getSegmentByIndex(i);
      SegmentMetadata afterSegment = volumeMetadataSwitch.getSegmentByIndex(i);
      compareTwoSegment(beforeSegment, afterSegment);
    }
    logger.warn("get the volumeMetadataSwitch is :" + volumeMetadataSwitch);
  }

  /**
   * the extend volume is change, not have the childVolumeId, * see the extend volume test.
   */
  @Ignore
  @Test
  public void testReportSegmentUnitsMetadata() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    // report the first volume
    VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volumeMetadata,
        SegmentUnitStatus.Secondary);

    when(dbVolumeStore.getVolume(anyLong())).thenReturn(null);
    informationCenter.reportSegmentUnitsMetadata(request);
    // check volume
    VolumeMetadata volumeFromStore = volumeStore.getVolumeForReport(1L);
    assertEquals(1, volumeFromStore.getVersion());
    assertEquals(null, volumeFromStore.getChildVolumeId());
    assertEquals(0, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
    assertTrue(volumeFromStore.isUpdatedToDataNode());

    // now the volume store has the first volume, update volume
    volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 2);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
    informationCenter.reportSegmentUnitsMetadata(request);

    // check volume
    volumeFromStore = volumeStore.getVolumeForReport(1L);
    assertEquals(2, volumeFromStore.getVersion());
    assertEquals(null, volumeFromStore.getChildVolumeId());
    assertEquals(0, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
    assertTrue(volumeFromStore.isUpdatedToDataNode());

    // report new volume with child volume
    long childVolumeId = 10;
    final int positionOfFirstSegmentInLogicVolume = 10;

    volumeMetadata = generateVolumeMetadata(2, childVolumeId, 6, 3, 1);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
    informationCenter.reportSegmentUnitsMetadata(request);

    // check volume
    volumeFromStore = volumeStore.getVolumeForReport(2L);
    assertEquals(1, volumeFromStore.getVersion());
    assertEquals(volumeFromStore.getChildVolumeId(), new Long(childVolumeId));
    assertEquals(0, volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
    assertTrue(volumeFromStore.isUpdatedToDataNode());

    // report child volume
    volumeMetadata = generateVolumeMetadata(childVolumeId, null, 6, 3, 1);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(positionOfFirstSegmentInLogicVolume);
    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
    informationCenter.reportSegmentUnitsMetadata(request);

    // check volume
    volumeFromStore = volumeStore.getVolumeForReport(childVolumeId);
    assertEquals(1, volumeFromStore.getVersion());
    assertEquals(null, volumeFromStore.getChildVolumeId());
    assertEquals(positionOfFirstSegmentInLogicVolume,
        volumeFromStore.getPositionOfFirstSegmentInLogicVolume());
    assertTrue(volumeFromStore.isUpdatedToDataNode());
  }

  @Test
  public void testReportSegmentUnitMetadata_normalProcess() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);

    VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
    volumeStore.saveVolumeForReport(volume);

    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Primary,
        1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    volume = volumeStore.getVolumeForReport(1L);
    assertEquals(volume.getSegments().size(), 1);
    volume.updateStatus();

    assertTrue(volume.isVolumeAvailable());
  }

  /**
   * volume exists with status dead but segment unit is not dead, info center will save segment unit
   * to notice data node to delete it.
   */
  @Test
  public void testReportSegmentUnitMetadata_volumeDead_segmentnotdead() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
    volume.setVolumeStatus(VolumeStatus.Dead);
    volumeStore.saveVolumeForReport(volume);

    ReportSegmentUnitsMetadataResponse response1;
    ReportSegmentUnitsMetadataResponse response2;
    final ReportSegmentUnitsMetadataResponse response3;

    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Primary,
        1, 0);
    response1 = informationCenter.reportSegmentUnitsMetadata(request);

    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
    response2 = informationCenter.reportSegmentUnitsMetadata(request);

    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
    response3 = informationCenter.reportSegmentUnitsMetadata(request);

    // for primary, info center not notify data node to delete
    assertEquals(response1.getConflicts().size(), 0);
    assertEquals(response2.getConflicts().size(), 1);
    assertEquals(response3.getConflicts().size(), 1);
  }

  /**
   * volume exists with status is dead. segment unit is also deleted or deleting. nothing to do
   */
  @Test
  public void testReportSegmentUnitMetadata_volumeDead_segmentdead() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
    volume.setVolumeStatus(VolumeStatus.Dead);
    volumeStore.saveVolume(volume);

    ReportSegmentUnitsMetadataResponse response1;
    ReportSegmentUnitsMetadataResponse response2;
    final ReportSegmentUnitsMetadataResponse response3;

    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Deleted,
        1, 0);
    response1 = informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Deleting, 2, 0);
    response2 = informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Deleting, 3, 0);
    response3 = informationCenter.reportSegmentUnitsMetadata(request);

    assertEquals(response1.getConflicts().size(), 0);
    assertEquals(response2.getConflicts().size(), 0);
    assertEquals(response3.getConflicts().size(), 0);
  }

  /**
   * test the last updated time beyond 6 months and the segment is not the first. Then info center
   * create a fake volume
   */
  @Test
  public void testReportSegmentUnitMetadata_notfirst_createAnFakeVolume() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    long beforeTime = System.currentTimeMillis() / 1000 - (deadVolumeToRemove + 1);

    VolumeMetadata volume = generateVolumeMetadata(1, null, 2 * segmentSize, segmentSize, 1);
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Secondary,
        1, 1);
    SegmentUnitMetadataThrift segUnit = request.getSegUnitsMetadata().get(0);
    segUnit.setLastUpdated(beforeTime);

    informationCenter.reportSegmentUnitsMetadata(request);
    volumeStore.saveVolumeForReport(volume);
    VolumeMetadata fakeVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertEquals(fakeVolume.getVolumeStatus(), VolumeStatus.ToBeCreated);
    assertTrue(fakeVolume.getName().contains(InfoCenterConstants.name));

    // when the first segment come, it id will be changed;
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary, 1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    fakeVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertEquals(fakeVolume.getName(), volume.getName());
  }

  /**
   * test the last updated time beyond 6 months and the segment is the first. Not to create a fake
   * volume
   */
  @Test
  public void testReportSegmentUnitMetadata_firstSegment_notcreatefakevolume() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    long beforeTime = System.currentTimeMillis() / 1000 - (deadVolumeToRemove + 1);
    VolumeMetadata volume = generateVolumeMetadata(1, null, 2 * segmentSize, segmentSize, 1);
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Primary,
        1, 0);
    SegmentUnitMetadataThrift segUnit = request.getSegUnitsMetadata().get(0);
    segUnit.setLastUpdated(beforeTime);

    informationCenter.reportSegmentUnitsMetadata(request);
    VolumeMetadata saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertEquals(saveVolume.getVolumeStatus(), VolumeStatus.Unavailable);
    assertEquals(saveVolume.getName(), volume.getName());

    saveVolume.updateStatus();
    assertEquals(saveVolume.getVolumeStatus(), VolumeStatus.Unavailable);
  }

  /**
   * test the normal situation and the segment unit data is changed.
   */
  @Test
  public void testReportSegmentUnitMetadata_segunit_datachange() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Secondary,
        2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary, 1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    VolumeMetadata saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertEquals(saveVolume.getName(), volume.getName());

    saveVolume.updateStatus();
    assertTrue(saveVolume.isVolumeAvailable());
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Primary, 1, 0);
    SegmentUnitMetadataThrift segUnit = request.getSegUnitsMetadata().get(0);
    segUnit.getMembership().setEpoch(2);
    informationCenter.reportSegmentUnitsMetadata(request);

    SegmentMetadata segment = saveVolume.getSegmentByIndex(0);
    SegmentUnitMetadata segmentUnit = segment.getSegmentUnitMetadata(new InstanceId(1));
    assertEquals(segmentUnit.getMembership().getSegmentVersion().getEpoch(), 2);
  }

  @Test
  public void testReportSegmentUnit_normalStatus_withinfocenter_sweeper() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.SUSPEND);
    VolumeMetadata volume = generateVolumeMetadata(1, null, segmentSize, segmentSize, 1);
    volume.setVolumeSource(CREATE_VOLUME);
    VolumeMetadata saveVolume = volumeStore.getVolume(volume.getVolumeId());
    assertNull(saveVolume);

    // report a segment unit, it will create a new volume in info center
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volume,
        SegmentUnitStatus.Primary,
        1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertNotNull(saveVolume);

    volumeSweeper.doWork();
    saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertEquals(VolumeStatus.Unavailable, saveVolume.getVolumeStatus());

    // continue to report other two secondary segment units
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volume, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    volumeSweeper.doWork();
    saveVolume = volumeStore.getVolumeForReport(volume.getVolumeId());
    assertEquals(1, saveVolume.getSegments().size());
    assertEquals(VolumeStatus.Stable, saveVolume.getVolumeStatus());
  }

  @Test
  public void testReportDriverMetadata() throws Exception {
    VolumeMetadata testVolume = new VolumeMetadata();
    testVolume.setVolumeId(1L);
    testVolume.setName("test_volume");
    volumeStore.saveVolume(testVolume);
    // report first driver
    Long driverContainerId = RequestIdBuilder.get();
    DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
    driverMetadataThrift.setVolumeId(1L);
    driverMetadataThrift.setDriverType(DriverTypeThrift.NBD);
    driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
    driverMetadataThrift.setHostName("hostname1");
    driverMetadataThrift.setPort(1);
    driverMetadataThrift.setDriverContainerId(driverContainerId);
    driverMetadataThrift.setDynamicIoLimitationId(100);
    driverMetadataThrift.setStaticIoLimitationId(101);
    driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);
    ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.addToDrivers(driverMetadataThrift);
    informationCenter.reportDriverMetadata(request);

    DriverMetadata driverMetadata1 = driverStore.get(driverContainerId, 1L, DriverType.NBD, 0);
    logger.warn("{}", driverMetadata1);
    long lastReportTime1 = driverMetadata1.getLastReportTime();

    // report second driver
    request.setRequestId(RequestIdBuilder.get());
    informationCenter.reportDriverMetadata(request);

    DriverMetadata driverMetadata2 = driverStore.get(driverContainerId, 1L, DriverType.NBD, 0);
    long lastReportTime2 = driverMetadata2.getLastReportTime();

    // check driver
    assertTrue(lastReportTime2 > lastReportTime1);
    assertTrue(driverMetadata1.getHostName().equals(driverMetadata2.getHostName()));
    assertTrue(driverMetadata1.getVolumeId() == driverMetadata2.getVolumeId());
  }

  @Test
  public void testReportDriverMetadataForDriverClient() throws Exception {
    VolumeMetadata testVolume = new VolumeMetadata();
    testVolume.setVolumeId(1L);
    testVolume.setName("test_volume");
    volumeStore.saveVolume(testVolume);

    // 1. report first driver, not any connect
    logger.warn("-------------- report 1 -----------");
    Long driverContainerId = 11L;
    DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
    driverMetadataThrift.setDriverName("testDriver");
    driverMetadataThrift.setVolumeId(1L);
    driverMetadataThrift.setDriverType(DriverTypeThrift.NBD);
    driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
    driverMetadataThrift.setHostName("hostname1");
    driverMetadataThrift.setPort(1);
    driverMetadataThrift.setDriverContainerId(driverContainerId);
    driverMetadataThrift.setDynamicIoLimitationId(100);
    driverMetadataThrift.setStaticIoLimitationId(101);
    driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);

    ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDrivercontainerId(driverContainerId);
    request.addToDrivers(driverMetadataThrift);
    informationCenter.reportDriverMetadata(request);
    //get the check
    List<DriverClientInformation> driverClientInformationList = driverClientManger
        .listAllDriverClientInfo();
    org.junit.Assert.assertTrue(driverClientInformationList.isEmpty());

    //2 .report have connect, two connect
    logger.warn("-------------- report 2 -----------");
    driverMetadataThrift = new DriverMetadataThrift();
    driverMetadataThrift.setDriverName("testDriver");
    driverMetadataThrift.setVolumeId(1L);
    driverMetadataThrift.setDriverType(DriverTypeThrift.NBD);
    driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
    driverMetadataThrift.setHostName("hostname1");
    driverMetadataThrift.setPort(1);
    driverMetadataThrift.setDriverContainerId(driverContainerId);
    driverMetadataThrift.setDynamicIoLimitationId(100);
    driverMetadataThrift.setStaticIoLimitationId(101);
    driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);

    Map<String, AccessPermissionTypeThrift> clientHostAccessRule = new HashMap<>();
    clientHostAccessRule.put("10.0.0.80", AccessPermissionTypeThrift.READ);
    clientHostAccessRule.put("10.0.0.81", AccessPermissionTypeThrift.READ);
    driverMetadataThrift.setClientHostAccessRule(clientHostAccessRule);

    request = new ReportDriverMetadataRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDrivercontainerId(driverContainerId);
    request.addToDrivers(driverMetadataThrift);
    informationCenter.reportDriverMetadata(request);
    //get the check
    driverClientInformationList = driverClientManger.listAllDriverClientInfo();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 2);

    //3 .report have connect, one dis connect
    logger.warn("-------------- report 3 -----------");
    clientHostAccessRule = new HashMap<>();
    clientHostAccessRule.put("10.0.0.81", AccessPermissionTypeThrift.READ);
    driverMetadataThrift.setClientHostAccessRule(clientHostAccessRule);

    request = new ReportDriverMetadataRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDrivercontainerId(driverContainerId);
    request.addToDrivers(driverMetadataThrift);
    informationCenter.reportDriverMetadata(request);
    //get the check
    driverClientInformationList = driverClientManger.listAllDriverClientInfo();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 3);
    //the client 10.0.0.80 dis connect
    boolean find = false;
    boolean find2 = false;
    for (DriverClientInformation driverClientInformation : driverClientInformationList) {
      DriverClientKeyInformation driverClientKeyInformation = driverClientInformation
          .getDriverClientKeyInformation();
      String clientInfo = driverClientKeyInformation.getClientInfo();
      if (clientInfo.equals("10.0.0.80")) {
        //dis connect
        find = true;
        DriverClientKey driverClientKey = new DriverClientKey(driverClientKeyInformation);
        DriverClientInformation driverClientInformationGet = driverClientStore
            .getLastTimeValue(driverClientKey);
        org.junit.Assert.assertTrue(!driverClientInformationGet.isStatus());
      }

      if (clientInfo.equals("10.0.0.81")) {
        //connect
        find2 = true;
        DriverClientKey driverClientKey = new DriverClientKey(driverClientKeyInformation);
        DriverClientInformation driverClientInformationGet = driverClientStore
            .getLastTimeValue(driverClientKey);
        org.junit.Assert.assertTrue(driverClientInformationGet.isStatus());
      }
    }
    org.junit.Assert.assertTrue(find && find2);

    //4 .all driver umount
    logger.warn("-------------- report 4 -----------");
    request = new ReportDriverMetadataRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setDrivercontainerId(driverContainerId);
    request.setDrivers(new ArrayList<>());
    informationCenter.reportDriverMetadata(request);
    //get the check
    driverClientInformationList = driverClientManger.listAllDriverClientInfo();
    org.junit.Assert.assertTrue(driverClientInformationList.size() == 4);
    //the client 10.0.0.80 dis connect
    boolean find3 = false;
    boolean find4 = false;
    for (DriverClientInformation driverClientInformation : driverClientInformationList) {
      DriverClientKeyInformation driverClientKeyInformation = driverClientInformation
          .getDriverClientKeyInformation();
      String clientInfo = driverClientKeyInformation.getClientInfo();
      if (clientInfo.equals("10.0.0.80")) {
        //dis connect
        find3 = true;
        DriverClientKey driverClientKey = new DriverClientKey(driverClientKeyInformation);
        DriverClientInformation driverClientInformationGet = driverClientStore
            .getLastTimeValue(driverClientKey);
        org.junit.Assert.assertTrue(!driverClientInformationGet.isStatus());
      }

      if (clientInfo.equals("10.0.0.81")) {
        //dis connect
        find4 = true;
        DriverClientKey driverClientKey = new DriverClientKey(driverClientKeyInformation);
        DriverClientInformation driverClientInformationGet = driverClientStore
            .getLastTimeValue(driverClientKey);
        org.junit.Assert.assertTrue(!driverClientInformationGet.isStatus());
      }
    }
    org.junit.Assert.assertTrue(find3 && find4);
  }

  @Test
  public void testListDriverClientInfo() throws Exception {
    //save the  value
    long driverContainerId = 111;
    long volumeId1 = RequestIdBuilder.get();
    long volumeId2 = RequestIdBuilder.get();
    String volumeName = "test_volume";
    String volumeName2 = "test_volume2";
    String volumeDescription = "check_volume";
    String volumeDescription2 = "check_volume2";
    DriverClientKey driverClientKey = new DriverClientKey(driverContainerId, volumeId2, 0,
        DriverType.ISCSI, "10.0.0.80");
    DriverClientInformation driverClientInformation = new DriverClientInformation(driverClientKey,
        123456, "driverNameTest2", "hostNameTest",
        true, volumeName2, volumeDescription2);
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

    //1. list all
    //DriverClientInfoThrift(driverContainerId:111, volumeId:3094613418242390839, snapshotId:0,
    // driverType:ISCSI, clientInfo:10.0.0.80,
    // time:123456, driverName:driverNameTest2, hostName:hostNameTest, status:true,
    // volumeName:test_volume2, volumeDescription:check_volume2),
    //
    //DriverClientInfoThrift(driverContainerId:222, volumeId:4308699288417489097, snapshotId:0,
    // driverType:ISCSI, clientInfo:10.0.0.81,
    // time:123456, driverName:driverNameTest1, hostName:hostNameTest1, status:true,
    // volumeName:test_volume, volumeDescription:check_volume),
    //
    // DriverClientInfoThrift(driverContainerId:222, volumeId:4308699288417489097, snapshotId:0,
    // driverType:ISCSI, clientInfo:10.0.0.81,
    // time:223456, driverName:driverNameTest1, hostName:hostNameTest1, status:false,
    // volumeName:test_volume, volumeDescription:check_volume)])
    ListDriverClientInfoRequest request = new ListDriverClientInfoRequest();
    ListDriverClientInfoResponse response = informationCenter.listDriverClientInfo(request);
    assertTrue(response.getDriverClientInfothrift().size() == 3);

    //2. list by driverName
    request = new ListDriverClientInfoRequest();
    request.setDriverName("driverNameTest1");
    response = informationCenter.listDriverClientInfo(request);
    assertTrue(response.getDriverClientInfothrift().size() == 2);

    //3. list by volumeName
    request = new ListDriverClientInfoRequest();
    request.setVolumeName("test_volume");
    response = informationCenter.listDriverClientInfo(request);
    assertTrue(response.getDriverClientInfothrift().size() == 3);

    //4. list by volumeDescription
    request = new ListDriverClientInfoRequest();
    request.setVolumeDescription("check_volume2");
    response = informationCenter.listDriverClientInfo(request);
    assertTrue(response.getDriverClientInfothrift().size() == 1);

    //5. list by driverName volumeName
    request = new ListDriverClientInfoRequest();
    request.setDriverName("driverNameTest1");
    request.setVolumeName("test_volume3");
    response = informationCenter.listDriverClientInfo(request);
    assertTrue(response.getDriverClientInfothrift().size() == 0);
  }

  @Test
  public void testDriverClientManger() throws Exception {
    VolumeMetadata testVolume = new VolumeMetadata();
    testVolume.setVolumeId(1L);
    final Long driverContainerId = 11L;
    testVolume.setName("test_volume");
    volumeStore.saveVolume(testVolume);

    //10s number 30
    int number = 5;
    long time = 10;
    driverClientManger.setDriverClientInfoKeepTime(time);
    driverClientManger.setDriverClientInfoKeepNumber(number);

    Map<String, AccessPermissionTypeThrift> clientHostAccessRule = new HashMap<>();
    Map<String, AccessPermissionTypeThrift> clientHostAccessRule2 = new HashMap<>();

    clientHostAccessRule.put("10.0.0.80", AccessPermissionTypeThrift.READ);
    clientHostAccessRule.put("10.0.0.81", AccessPermissionTypeThrift.READ);
    DriverMetadataThrift driverMetadataThrift = makeDriverMetadataThrift(driverContainerId,
        clientHostAccessRule);
    DriverMetadataThrift driverMetadataThrift2 = makeDriverMetadataThrift(driverContainerId,
        clientHostAccessRule2);

    //1. report check number
    int count = 10;
    for (int i = 0; i < count; i++) {
      ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
      request.setRequestId(RequestIdBuilder.get());
      request.setDrivercontainerId(driverContainerId);

      if (i % 2 == 0) {
        //connect
        request.addToDrivers(driverMetadataThrift);
      } else {
        //dis connect
        request.addToDrivers(driverMetadataThrift2);
      }
      informationCenter.reportDriverMetadata(request);
    }

    //check, have two client. report 20
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    logger.warn("get the size:{}", driverClientInformationList.size());
    Assert.assertEquals(driverClientInformationList.size(), count * 2);

    //remove the old value, more number
    long begin = System.currentTimeMillis();
    logger.warn("the begin time:{}", begin);
    driverClientManger.removeOldDriverClientInfo();
    logger.warn("the end time:{}", System.currentTimeMillis() - begin);
    //check
    driverClientInformationList = driverClientStore.list();
    Assert.assertEquals(driverClientInformationList.size(), number * 2);

    //2. check the time, remove in time
    Thread.sleep(10000);
    driverClientManger.removeOldDriverClientInfo();
    driverClientInformationList = driverClientStore.list();
    Assert.assertEquals(driverClientInformationList.size(), 0);
  }

  private DriverMetadataThrift makeDriverMetadataThrift(long driverContainerId,
      Map<String, AccessPermissionTypeThrift> clientHostAccessRule) {
    DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
    driverMetadataThrift.setDriverName("testDriver");
    driverMetadataThrift.setVolumeId(1L);
    driverMetadataThrift.setDriverType(DriverTypeThrift.NBD);
    driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
    driverMetadataThrift.setHostName("hostname1");
    driverMetadataThrift.setPort(1);
    driverMetadataThrift.setDriverContainerId(driverContainerId);
    driverMetadataThrift.setDynamicIoLimitationId(100);
    driverMetadataThrift.setStaticIoLimitationId(101);
    driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);
    driverMetadataThrift.setClientHostAccessRule(clientHostAccessRule);
    return driverMetadataThrift;
  }

  /**
   * This is a test for driver container allocation. In the test, total 2 driver container instance
   * are available. First allocation and second allocation are successful. But third allocation
   * would not allocate any driver container due to the two driver container is used up.
   */
  @Test
  public void testAllocDriverContainer() throws Exception {
    long volumeId = 1L;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1);
    volumeStore.saveVolume(volume);

    long driverContainerId = RequestIdBuilder.get();
    Instance instance1 = new Instance(new InstanceId(driverContainerId), null,
        PyService.DRIVERCONTAINER.name(),
        InstanceStatus.HEALTHY);
    instance1.putEndPointByServiceName(PortType.CONTROL, new EndPoint("0.0.0.1", 1));
    instance1.putEndPointByServiceName(PortType.IO, new EndPoint("0.0.0.1", 2));

    Set<Instance> instances = new HashSet<>();
    instances.add(instance1);

    when(instanceStore.getAll(any(String.class), any(InstanceStatus.class))).thenReturn(instances);

    // get driver container from information center
    List<DriverContainerCandidate> driverContainerCandidateList = informationCenter
        .allocDriverContainer(volumeId, 1, false);

    // check first driver container allocation
    Assert.assertEquals(1, driverContainerCandidateList.size());

    driverStore.delete(1L);
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("0.0.0.1");
    driverMetadata.setVolumeId(1L);
    driverMetadata.setDriverType(DriverType.NBD);
    driverMetadata.setSnapshotId(0);
    driverMetadata.setDriverContainerId(driverContainerId);
    driverMetadata.setDynamicIoLimitationId(100);
    driverMetadata.setStaticIoLimitationId(101);
    driverStore.save(driverMetadata);

    // check two NBD cannot on same driver container allocation
    informationCenter.allocDriverContainer(volumeId, 1, false);

    // check FSD and NBD can on the same driver container
    List<DriverContainerCandidate> containerCandidateList = informationCenter
        .allocDriverContainer(volumeId, 1, false);
    Assert.assertEquals(1, containerCandidateList.size());

    DriverMetadata driverMetadata1 = new DriverMetadata();
    driverMetadata1.setHostName("0.0.0.1");
    driverMetadata1.setVolumeId(volumeId);
    driverMetadata1.setDriverType(DriverType.FSD);
    driverMetadata1.setSnapshotId(0);
    driverMetadata1.setDynamicIoLimitationId(100);
    driverMetadata1.setStaticIoLimitationId(101);
    driverStore.save(driverMetadata1);

    informationCenter.allocDriverContainer(volumeId, 1, false);

    driverStore.delete(volumeId);

    // check first have ISCSI
    containerCandidateList = informationCenter.allocDriverContainer(volumeId, 1, false);
    Assert.assertEquals(1, containerCandidateList.size());

    driverMetadata1.setHostName("0.0.0.1");
    driverMetadata1.setVolumeId(volumeId);
    driverMetadata1.setDriverType(DriverType.ISCSI);
    driverMetadata1.setSnapshotId(0);
    driverMetadata1.setDriverContainerId(driverContainerId);
    driverStore.save(driverMetadata1);

    // check new FSD or NBD cannot get the container already has a ISCSI
    informationCenter.allocDriverContainer(volumeId, 1, false);

    driverStore.delete(volumeId);
  }

  /**
   * Test case steps: 1. Prepare 1 volume and 1 driver container; 2. Launch first driver with 0
   * snapshot id, success; 3. Launch second driver with 1 snapshot id, success; 4. Launch third
   * driver with 0 snapshot id again, catch too many driver exception.
   */

  @Test
  public void testAllocDriverContainerWithSnapshotId() throws Exception {
    long volumeId = 1L;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1);
    volumeStore.saveVolume(volume);

    long driverContainerId = RequestIdBuilder.get();
    Instance instance = new Instance(new InstanceId(driverContainerId), null,
        PyService.DRIVERCONTAINER.name(),
        InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("0.0.0.1", 1));
    instance.putEndPointByServiceName(PortType.IO, new EndPoint("0.0.0.1", 2));

    Set<Instance> instances = new HashSet<>();
    instances.add(instance);

    when(instanceStore.getAll(any(String.class), any(InstanceStatus.class))).thenReturn(instances);

    // get driver container from information center
    List<DriverContainerCandidate> driverContainerCandidateList = informationCenter
        .allocDriverContainer(volumeId, 1, false);

    // check first driver container allocation
    Assert.assertEquals(1, driverContainerCandidateList.size());

    driverStore.delete(volumeId);
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("0.0.0.1");
    driverMetadata.setVolumeId(volumeId);
    driverMetadata.setDriverType(DriverType.NBD);
    driverMetadata.setSnapshotId(0);
    driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);
    driverMetadata.setDriverContainerId(driverContainerId);
    driverMetadata.setDynamicIoLimitationId(100);
    driverMetadata.setStaticIoLimitationId(101);
    driverStore.save(driverMetadata);

    // check launch another NBD driver with different snapshot id success
    driverContainerCandidateList = informationCenter
        .allocDriverContainer(volumeId, 1, false);

    // check second driver container allocation

    Assert.assertEquals(1, driverContainerCandidateList.size());

  }

  @Test(expected = VolumeNotFoundExceptionThrift.class)
  public void testDeleteVolumeNotFoundVolume() throws Exception {
    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setAccountId(111);
    deleteVolumeRequest.setVolumeId(111);

    informationCenter.deleteVolume(deleteVolumeRequest);
  }

  @Test(expected = VolumeInExtendingExceptionThrift.class)
  public void testDeleteVolumeInExtending() throws Exception {
    VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
    volume.setExtendingSize(100);
    volume.setInAction(VolumeInAction.EXTENDING);

    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);

    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(111);
    deleteVolumeRequest.setVolumeId(1);
    informationCenter.deleteVolume(deleteVolumeRequest);
  }

  @Test
  public void testDeleteVolumeNormal() throws Exception {
    // report the first volume
    long volumeId = 1;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1);
    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);
    assertEquals(VolumeStatus.ToBeCreated, volume.getVolumeStatus());

    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(111);
    deleteVolumeRequest.setVolumeId(volumeId);

    DriverStore driverStoreMock = mock(DriverStore.class);
    List<DriverMetadata> driverMetadataList = new ArrayList<>();
    when(driverStoreMock.get(anyLong())).thenReturn(driverMetadataList);
    informationCenter.setDriverStore(driverStoreMock);

    DeleteVolumeResponse response = informationCenter.deleteVolume(deleteVolumeRequest);
    assertEquals(response.getRequestId(), 111);
    VolumeMetadata newVolume = volumeStore.getVolume(volumeId);
    assertEquals(VolumeStatus.Deleting, newVolume.getVolumeStatus());
  }

  @Test
  public void testDeleteVolumeWithCloningSnapshotId() throws TException, VolumeNotFoundException {
    long volumeId = 2;
    VolumeMetadata volumeMetadata = generateVolumeMetadata(volumeId, null, 6, 3, 1);
    String srcVolumeName = "999";
    String srcSnpName = "888";
    volumeStore.saveVolume(volumeMetadata);
    assertEquals(VolumeStatus.ToBeCreated, volumeMetadata.getVolumeStatus());

    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(222);
    deleteVolumeRequest.setVolumeId(2);

    DeleteVolumeResponse deleteVolumeResponse = informationCenter.deleteVolume(deleteVolumeRequest);
    assertEquals(222, deleteVolumeResponse.getRequestId());
    VolumeMetadata newVolume = volumeStore.getVolume(volumeId);
    assertEquals(VolumeStatus.Deleting, newVolume.getVolumeStatus());
  }

  @Test(expected = VolumeBeingDeletedExceptionThrift.class)
  public void testDeleteVolumeInDeletingStatus() throws Exception {
    VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
    volume.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);

    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(111);
    deleteVolumeRequest.setVolumeId(1);

    informationCenter.deleteVolume(deleteVolumeRequest);
    fail("This test case not throw exception");
  }

  @Test(expected = VolumeBeingDeletedExceptionThrift.class)
  public void testDeleteVolumeInDeletedStatus() throws Exception {
    VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
    volume.setVolumeStatus(VolumeStatus.Deleted);
    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);

    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(111);
    deleteVolumeRequest.setVolumeId(1);

    informationCenter.deleteVolume(deleteVolumeRequest);
    fail("This test case not throw exception");
  }

  @Test(expected = ServiceHavingBeenShutdownThrift.class)
  public void testDeleteVolumeInfoCenterShutdown() throws Exception {
    VolumeMetadata volume = generateVolumeMetadata(1, null, 6, 3, 1);
    volumeStore.saveVolume(volume);

    informationCenter.shutdownForTest();
    ;
    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(111);
    deleteVolumeRequest.setVolumeId(1);

    informationCenter.deleteVolume(deleteVolumeRequest);
    fail("This test case not throw exception");
  }

  @Test(expected = ServiceHavingBeenShutdownThrift.class)
  public void testCreateVolume_Shutdown() throws Exception {
    CreateVolumeRequest request = new CreateVolumeRequest();
    informationCenter.shutdownForTest();
    ;
    informationCenter.createVolume(request);
    fail("This test case not throw exception");
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void testCreateVolume_VolumeNameIsEmpty() throws Exception {
    CreateVolumeRequest request = new CreateVolumeRequest();
    request.setName(null);
    informationCenter.createVolume(request);
    fail("This test case not throw exception");
  }

  @Test(expected = InvalidInputExceptionThrift.class)
  public void testCreateVolume_VolumeNameTooLong() throws Exception {
    CreateVolumeRequest request = new CreateVolumeRequest();
    StringBuilder name = new StringBuilder("1");
    for (int i = 0; i < 200; i++) {
      name.append(i);
    }

    request.setName(name.toString());
    informationCenter.createVolume(request);
    fail("This test case not throw exception");
  }

  @Test(expected = VolumeExistingExceptionThrift.class)
  public void testCreateVolume_VolumeExisting() throws Exception {
    CreateVolumeRequest request = new CreateVolumeRequest();
    request.setName("wjhsuper");
    request.setAccountId(123456);
    request.setVolumeSize(100);
    request.setVolumeType(VolumeTypeThrift.LARGE);
    final List<InstanceMetadata> ls = new ArrayList<InstanceMetadata>();

    InstanceId instanceId = new InstanceId(1);
    InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
    instanceMetadata.setFreeSpace(800);
    instanceMetadata.setCapacity(200);
    instanceMetadata.setDatanodeStatus(OK);

    ls.add(instanceMetadata);

    when(storageStore.list()).thenReturn(ls);
    informationCenter.setStorageStore(storageStore);

    //informationCenter.setActualFreeSpace(800);
    RefreshTimeAndFreeSpace.getInstance().setActualFreeSpace(800);

    request.setVolumeId(1);
    Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Set<Long> storagePoolIds = new HashSet<Long>();
    storagePoolIds.add(storagePoolId);
    request.setDomainId(domainId);
    request.setStoragePoolId(storagePoolId);
    VolumeMetadata volumeMetadata = new VolumeMetadata();

    when(volumeStore1.getVolume(request.getVolumeId())).thenReturn(volumeMetadata);
    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);
    when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
    when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);

    // TODO:
    when(domain.isDeleting()).thenReturn(false);
    when(storagePool.isDeleting()).thenReturn(false);
    when(domain.getStoragePools()).thenReturn(storagePoolIds);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    informationCenter.setVolumeStore(volumeStore1);

    informationCenter.createVolume(request);
    fail("This test case not throw exception");
  }

  @Test(expected = RootVolumeNotFoundExceptionThrift.class)
  public void testExtendVolume_RootVolumeNotFound() throws Exception {
    long srcVolumeId = RequestIdBuilder.get();
    Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();

    ExtendVolumeRequest extendVolumeRequest = new ExtendVolumeRequest();
    extendVolumeRequest.setVolumeId(srcVolumeId);
    extendVolumeRequest.setAccountId(111L);
    extendVolumeRequest.setRequestId(2);
    extendVolumeRequest.setExtendSize(segmentSize);
    extendVolumeRequest.setDomainId(domainId);
    extendVolumeRequest.setStoragePoolId(storagePoolId);

    Set<Long> storagePoolIds = new HashSet<>();
    storagePoolIds.add(storagePoolId);
    final List<InstanceMetadata> ls = new ArrayList<>();

    InstanceId instanceId = new InstanceId(1);
    InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
    instanceMetadata.setFreeSpace(800);
    instanceMetadata.setCapacity(200);
    instanceMetadata.setDatanodeStatus(OK);

    ls.add(instanceMetadata);

    when(storageStore.list()).thenReturn(ls);
    informationCenter.setStorageStore(storageStore);

    informationCenter.setUserMaxCapacityByte(201);

    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);
    when(domainStore.getDomain(anyLong())).thenReturn(domain);
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);
    when(domain.isDeleting()).thenReturn(false);
    when(storagePool.isDeleting()).thenReturn(false);
    when(domain.getStoragePools()).thenReturn(storagePoolIds);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    informationCenter.extendVolume(extendVolumeRequest);
    fail("This test case not throw exception");
  }

  @Test(expected = NotEnoughSpaceExceptionThrift.class)
  public void testCreateVolume_NotEnoughSpaceException() throws Exception {
    CreateVolumeRequest request = new CreateVolumeRequest();
    request.setName("wjhsuper");
    request.setAccountId(123456);
    request.setVolumeSize(100);
    request.setVolumeType(VolumeTypeThrift.LARGE);
    Long domainId = RequestIdBuilder.get();
    Long storagePoolId = RequestIdBuilder.get();
    Set<Long> storagePoolIds = new HashSet<>();
    storagePoolIds.add(storagePoolId);
    request.setDomainId(domainId);
    request.setStoragePoolId(storagePoolId);
    final List<InstanceMetadata> ls = new ArrayList<>();

    InstanceId instanceId = new InstanceId(1);
    InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
    instanceMetadata.setFreeSpace(150);
    instanceMetadata.setCapacity(200);
    instanceMetadata.setDatanodeStatus(OK);

    ls.add(instanceMetadata);

    when(storageStore.list()).thenReturn(ls);
    informationCenter.setStorageStore(storageStore);

    informationCenter.setUserMaxCapacityByte(201);

    request.setVolumeId(0);

    Domain domain = mock(Domain.class);
    StoragePool storagePool = mock(StoragePool.class);
    when(domainStore.getDomain(request.getDomainId())).thenReturn(domain);
    when(storagePoolStore.getStoragePool(request.getStoragePoolId())).thenReturn(storagePool);
    when(domain.isDeleting()).thenReturn(false);
    when(storagePool.isDeleting()).thenReturn(false);
    when(domain.getStoragePools()).thenReturn(storagePoolIds);
    when(storagePool.getPoolId()).thenReturn(storagePoolId);

    when(volumeStore1.getVolume(request.getVolumeId())).thenReturn(null);
    informationCenter.setVolumeStore(volumeStore1);

    request.setRootVolumeId(1);
    VolumeMetadata volumeMetadata1 = new VolumeMetadata();

    when(volumeStore2.getVolume(request.getRootVolumeId())).thenReturn(volumeMetadata1);
    informationCenter.setVolumeStore(volumeStore2);
    // volumeStore2.updateStatus(request.getRootVolumeId(), "");

    informationCenter.createVolume(request);
    fail("This test case not throw exception");
  }

  @Test
  public void testDateConvert() throws ParseException {
    Date nowDate = new Date();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    String dateString = dateFormat.format(nowDate);
    Date newDate = dateFormat.parse(dateString);
    assertTrue(newDate != null);
    assertEquals(nowDate.toString(), newDate.toString());
  }

  /**
   * Two volumes: one status is dead and another status is not dead. Can get volume which status is
   * not dead. Cannot get volume which status is dead.
   */
  @Test
  public void testGetVolumeFilterDead() {
    VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
    volumeMetadata.setVolumeStatus(VolumeStatus.Dead);
    volumeMetadata.setAccountId(123456);
    volumeMetadata.setInAction(NULL);
    VolumeMetadata volumeMetadata1 = generateVolumeMetadata(2, null, 6, 3, 1);
    volumeMetadata1.setAccountId(123456);
    volumeMetadata1.setInAction(NULL);
    volumeStore.saveVolume(volumeMetadata);
    volumeStore.saveVolume(volumeMetadata1);
    informationCenter.setVolumeStore(volumeStore);

    GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), 1, 123456, null, true);
    request.setContainDeadVolume(false);
    // cannot get volume which status is dead
    try {
      GetVolumeResponse response = informationCenter.getVolume(request);
      assertTrue(false);
    } catch (Exception e) {
      logger.error("Cannot get volume!", e);
      assertTrue(e instanceof VolumeNotFoundExceptionThrift);
    }

    // no problem to get volume which status is not dead
    try {
      request.setVolumeId(2);
      GetVolumeResponse response = informationCenter.getVolume(request);
      assertTrue(response.isSetVolumeMetadata());
    } catch (Exception e) {
      logger.error("Cannot get volume!", e);
      assertTrue(false);
    }

  }

  @Test
  @Ignore // the ExtendVolume change, no include the childVolume
  public void testGetTwiceExtendVolume() {
    int segmentSize = 1;
    int rootVolumeSize = 10;
    int childVolumeSize1 = 5;
    final int childVolumeSize2 = 12;

    Long rootVolumeId = RequestIdBuilder.get();
    Long childVolumeId1 = RequestIdBuilder.get();
    Long childVolumeId2 = RequestIdBuilder.get();

    List<SegId> oriSegIdList = new ArrayList<>();

    VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId,
        childVolumeId1,
        "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);
    rootVolumeMetadata.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
    rootVolumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);

    VolumeMetadata volumeMetadata1 = generateVolumeMetadata2(rootVolumeId, childVolumeId1,
        childVolumeId2,
        "childVolumeId1", childVolumeSize1, segmentSize, 1, oriSegIdList);
    volumeMetadata1.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
    volumeMetadata1.setPositionOfFirstSegmentInLogicVolume(rootVolumeSize);

    VolumeMetadata volumeMetadata2 = generateVolumeMetadata2(rootVolumeId, childVolumeId2, null,
        "childVolumeId2",
        childVolumeSize2, segmentSize, 1, oriSegIdList);
    volumeMetadata2.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
    volumeMetadata2.setPositionOfFirstSegmentInLogicVolume(rootVolumeSize + childVolumeSize1);

    volumeStore.saveVolume(rootVolumeMetadata);
    volumeStore.saveVolume(volumeMetadata1);
    volumeStore.saveVolume(volumeMetadata2);
    informationCenter.setVolumeStore(volumeStore);

    GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
        Constants.SUPERADMIN_ACCOUNT_ID, null, false);
    request.setContainDeadVolume(false);

    List<SegId> gotSegIdList = new ArrayList<>();

    try {
      GetVolumeResponse response = informationCenter.getVolume(request);

      VolumeMetadata volumeMetadata = RequestResponseHelper
          .buildVolumeFrom(response.getVolumeMetadata());
      for (SegmentMetadata segmentMetadata : volumeMetadata.getSegmentTable().values()) {
        SegId segId = new SegId(segmentMetadata.getSegId().getVolumeId(),
            segmentMetadata.getSegId().getIndex());
        gotSegIdList.add(segId);
      }
      //            List<SegmentMetadataSwitchThrift> segmentMetadataThriftList = response
      //            .getVolumeMetadata().getSegmentsMetadataSwitch();
      //            for (SegmentMetadataSwitchThrift segmentMetadataThrift :
      //            segmentMetadataThriftList) {
      //                SegId segId = new SegId(segmentMetadataThrift.getVolumeIdSwitch(),
      //                segmentMetadataThrift.getSegId());
      //                gotSegIdList.add(segId);
      //            }

    } catch (Exception e) {
      fail();
    }

    assertEquals(oriSegIdList.size(), gotSegIdList.size());

    int totalSegmentCount = rootVolumeSize + childVolumeSize1 + childVolumeSize2;

    for (int i = 0; i < totalSegmentCount; i++) {
      assertEquals(oriSegIdList.get(i), gotSegIdList.get(i));
    }

  }

  @Test
  public void testEnablePaginationGetVolume() {
    setLogLevel(Level.WARN);

    for (int i = 0; i < 100; i++) { //test 100 time
      int segmentSize = 1;
      int rootVolumeSize = RandomUtils.nextInt(20) + 1;

      Long rootVolumeId = RequestIdBuilder.get();
      Long childVolumeId1 = RequestIdBuilder.get();

      List<SegId> oriSegIdList = new ArrayList<>();

      VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId,
          childVolumeId1,
          "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);
      rootVolumeMetadata.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
      rootVolumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);

      //save volume
      volumeStore.saveVolume(rootVolumeMetadata);
      volumeStore.saveVolumeForReport(rootVolumeMetadata);

      GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
          Constants.SUPERADMIN_ACCOUNT_ID, null, false);
      request.setContainDeadVolume(false);

      request.setEnablePagination(true);
      request.setStartSegmentIndex(0);
      int paginationNumber = RandomUtils.nextInt(5) + 1;
      request.setPaginationNumber(paginationNumber);

      List<SegId> paginationSegIdList = new ArrayList<>();
      while (true) {
        try {
          GetVolumeResponse response = informationCenter.getVolume(request);

          List<SegmentMetadataSwitchThrift> segmentMetadataThriftList = response
              .getVolumeMetadata()
              .getSegmentsMetadataSwitch();

          for (SegmentMetadataSwitchThrift segmentMetadataThrift : segmentMetadataThriftList) {
            SegId segId = new SegId(segmentMetadataThrift.getVolumeIdSwitch(),
                segmentMetadataThrift.getSegId());
            paginationSegIdList.add(segId);
          }
          if (!response.isLeftSegment()) {
            //the get segment is <= paginationNumber
            assertTrue(
                response.getVolumeMetadata().getSegmentsMetadataSwitchSize() <= paginationNumber);
            break;
          } else {
            assertTrue(response.isSetNextStartSegmentIndex());

            //check return segment size, segment = paginationNumber
            assertTrue(
                response.getVolumeMetadata().getSegmentsMetadataSwitchSize() == paginationNumber);

            request.setStartSegmentIndex(response.getNextStartSegmentIndex());
            paginationNumber = RandomUtils.nextInt(5) + 1;
            request.setPaginationNumber(paginationNumber);
          }
        } catch (Exception e) {
          logger.error("Cannot get volume!", e);
          fail();
        }
      }

      assertEquals(oriSegIdList.size(), paginationSegIdList.size());
    }
  }

  @Test
  public void testEnablePaginationGetVolume_EnableOrNot() {
    setLogLevel(Level.WARN);
    int segmentSize = 1;
    int rootVolumeSize = 20;
    final int paginationNumber = 5;

    Long rootVolumeId = RequestIdBuilder.get();
    Long childVolumeId1 = RequestIdBuilder.get();

    List<SegId> oriSegIdList = new ArrayList<>();
    VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId,
        childVolumeId1,
        "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);

    volumeStore.saveVolume(rootVolumeMetadata);
    volumeStore.saveVolumeForReport(rootVolumeMetadata);

    //add Segment and the EnablePagination NOT
    GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
        Constants.SUPERADMIN_ACCOUNT_ID, null, false);
    request.setContainDeadVolume(false);
    request.setEnablePagination(false);
    request.setStartSegmentIndex(0);
    request.setPaginationNumber(paginationNumber);

    try {
      GetVolumeResponse response = informationCenter.getVolume(request);
      List<SegmentMetadataSwitchThrift> segmentMetadataThriftList = response.getVolumeMetadata()
          .getSegmentsMetadataSwitch();
      assertTrue(segmentMetadataThriftList.size() == rootVolumeSize);
      assertTrue(response.getNextStartSegmentIndex() == 0);
      assertTrue(!response.isLeftSegment());

    } catch (Exception e) {
      logger.error("Cannot get volume!", e);
      fail();
    }

    //not add Segment
    request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
        Constants.SUPERADMIN_ACCOUNT_ID, null, true);
    request.setContainDeadVolume(false);
    request.setEnablePagination(true);
    request.setStartSegmentIndex(0);
    request.setPaginationNumber(paginationNumber);

    try {
      GetVolumeResponse response = informationCenter.getVolume(request);
      List<SegmentMetadataSwitchThrift> segmentMetadataThriftList = response.getVolumeMetadata()
          .getSegmentsMetadataSwitch();
      assertTrue(segmentMetadataThriftList.isEmpty());
      assertTrue(response.getNextStartSegmentIndex() == 0);
      assertTrue(response.isLeftSegment());

    } catch (Exception e) {
      logger.error("Cannot get volume!", e);
      fail();
    }
  }

  @Test
  public void testEnablePaginationGetVolume_Different_StartSegmentIndex() {
    setLogLevel(Level.WARN);
    int segmentSize = 1;
    int rootVolumeSize = 20;
    final int paginationNumber = 5;
    final int startSegmentIndexLarge = 21;
    final int startSegmentIndex = 16;

    Long rootVolumeId = RequestIdBuilder.get();
    Long childVolumeId1 = RequestIdBuilder.get();

    List<SegId> oriSegIdList = new ArrayList<>();
    VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId,
        childVolumeId1,
        "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);

    volumeStore.saveVolume(rootVolumeMetadata);
    volumeStore.saveVolumeForReport(rootVolumeMetadata);

    //add Segment and the StartSegmentIndex is large > segment count
    GetVolumeRequest request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
        Constants.SUPERADMIN_ACCOUNT_ID, null, false);
    request.setContainDeadVolume(false);
    request.setEnablePagination(true);
    request.setStartSegmentIndex(startSegmentIndexLarge);
    request.setPaginationNumber(paginationNumber);

    try {
      GetVolumeResponse response = informationCenter.getVolume(request);
      List<SegmentMetadataSwitchThrift> segmentMetadataThriftList = response.getVolumeMetadata()
          .getSegmentsMetadataSwitch();
      assertTrue(segmentMetadataThriftList.isEmpty());
      assertTrue(response.getNextStartSegmentIndex() == 0);
      assertTrue(response.isLeftSegment());

    } catch (Exception e) {
      logger.error("Cannot get volume!", e);
      fail();
    }

    //add Segment and startSegmentIndex + paginationNumber >= totalSegmentCount
    request = new GetVolumeRequest(RequestIdBuilder.get(), rootVolumeId,
        Constants.SUPERADMIN_ACCOUNT_ID, null, false);
    request.setContainDeadVolume(false);
    request.setEnablePagination(true);
    request.setStartSegmentIndex(startSegmentIndex);
    request.setPaginationNumber(paginationNumber);

    try {
      GetVolumeResponse response = informationCenter.getVolume(request);
      List<SegmentMetadataSwitchThrift> segmentMetadataThriftList = response.getVolumeMetadata()
          .getSegmentsMetadataSwitch();
      assertTrue(segmentMetadataThriftList.size() == rootVolumeSize - startSegmentIndex);
      assertTrue(response.getNextStartSegmentIndex() == rootVolumeSize);
      assertTrue(!response.isLeftSegment());

    } catch (Exception e) {
      logger.error("Cannot get volume!", e);
      fail();
    }
  }

  @Test
  public void testEnablePaginationGetVolume_ForDebug() {
    setLogLevel(Level.WARN);

    int segmentSize = 1;
    int rootVolumeSize = RandomUtils.nextInt(20) + 1;
    int paginationNumberSet = 5;

    Long rootVolumeId = RequestIdBuilder.get();
    Long childVolumeId1 = RequestIdBuilder.get();

    List<SegId> oriSegIdList = new ArrayList<>();

    VolumeMetadata rootVolumeMetadata = generateVolumeMetadata2(rootVolumeId, rootVolumeId,
        childVolumeId1,
        "rootVolume", rootVolumeSize, segmentSize, 1, oriSegIdList);

    int startSegmentIndex = 0;
    int paginationNumber = paginationNumberSet;

    while (true) {
      try {
        VolumeMetadata volumeMetadata = informationCenter.processVolumeForPagination(
            rootVolumeMetadata, true, true, startSegmentIndex, paginationNumber);

        if (!volumeMetadata.isLeftSegment()) {
          break;
        } else {

          startSegmentIndex = volumeMetadata.getNextStartSegmentIndex();
        }
      } catch (Exception e) {
        logger.error("Cannot get volume!", e);
        fail();
      }
    }
  }

  /**
   * Test list volumes with filter: if listVolumeRequest is set filter(volumesCanBeList), then only
   * the volumes in the list will be return to caller; if filter is not set, then listVolumes will
   * return all without filter.
   */
  @Test
  public void testListVolumesWithFilter() throws AccountNotFoundExceptionThrift {
    //make volume
    VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
    volumeMetadata.setAccountId(123456);
    volumeMetadata.setInAction(NULL);

    VolumeMetadata volumeMetadata1 = generateVolumeMetadata(2, null, 6, 3, 1);
    volumeMetadata1.setAccountId(123456);
    volumeMetadata1.setInAction(NULL);

    //save volume
    volumeStore.saveVolume(volumeMetadata);
    volumeStore.saveVolume(volumeMetadata1);

    Set<Long> accessibleResource = new HashSet<>();
    accessibleResource.add(1L);
    accessibleResource.add(2L);

    when(
        securityManager.getAccessibleResourcesByType(anyLong(), any(PyResource.ResourceType.class)))

        .thenReturn(accessibleResource);

    Set<Long> volumeCanBeList = new HashSet<>();
    ListVolumesRequest request = new ListVolumesRequest(RequestIdBuilder.get(), 123456);
    request.setVolumesCanBeList(volumeCanBeList);

    // filter list, volumeCanBeList is empty
    List<VolumeMetadataThrift> volumeMetadataThrifts;

    // first with filter which has 0 volume can be list
    try {
      ListVolumesResponse response = informationCenter.listVolumes(request);
      volumeMetadataThrifts = response.getVolumes();
      assertEquals(0, volumeMetadataThrifts.size());
    } catch (Exception e) {
      logger.error("Cannot list volumes!", e);
      fail();
    }

    // then filter with 3 volumes which two exist and one doesn't in AccessibleResources
    volumeCanBeList.add(1L);
    volumeCanBeList.add(2L);
    volumeCanBeList.add(3L);
    request.setVolumesCanBeList(volumeCanBeList);

    try {
      ListVolumesResponse response = informationCenter.listVolumes(request);
      volumeMetadataThrifts = response.getVolumes();
      assertEquals(2, volumeMetadataThrifts.size());
    } catch (Exception e) {
      logger.error("Cannot list volumes!", e);
      fail();
    }

    // last close filter function, list form AccessibleResources
    request.setVolumesCanBeList(null);
    try {
      ListVolumesResponse response = informationCenter.listVolumes(request);
      volumeMetadataThrifts = response.getVolumes();
      assertEquals(2, volumeMetadataThrifts.size());
    } catch (Exception e) {
      logger.error("Cannot list volumes!", e);
      fail();
    }
  }

  @Test
  public void testListVolumesWithMigrationSpeed() throws Exception {
    //make volume
    long volumeId = 1L;
    VolumeMetadata volumeMetadata = generateVolumeMetadata(volumeId, null, 6, 3, 1);
    volumeMetadata.setAccountId(123456);
    volumeMetadata.setInAction(NULL);

    //make accessibleResource
    Set<Long> accessibleResource = new HashSet<>();
    accessibleResource.add(1L);

    when(
        securityManager.getAccessibleResourcesByType(anyLong(), any(PyResource.ResourceType.class)))

        .thenReturn(accessibleResource);

    //make SegmentUnitMetadata and segmentMetadata
    final SegmentUnitMetadata segUnit1 = new SegmentUnitMetadata(new SegId(volumeId, 0), 0);
    final SegmentUnitMetadata segUnit2 = new SegmentUnitMetadata(new SegId(volumeId, 0), 1);
    final SegmentUnitMetadata segUnit3 = new SegmentUnitMetadata(new SegId(volumeId, 0), 2);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);

    SegmentMembership highestMembershipInSegment = mock(SegmentMembership.class);
    volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);

    when(segmentMetadata.getIndex()).thenReturn(0);
    when(segmentMetadata.getSegmentUnitCount()).thenReturn(3);
    when(segmentMetadata.getSegId()).thenReturn(new SegId(volumeId, 0));
    Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = new HashMap<>();
    segmentUnitMetadataMap.put(new InstanceId(1), segUnit1);
    segmentUnitMetadataMap.put(new InstanceId(2), segUnit2);
    segmentUnitMetadataMap.put(new InstanceId(3), segUnit3);
    List<SegmentUnitMetadata> segmentUnitMetadataArrayList = new ArrayList<>();
    segmentUnitMetadataArrayList.add(segUnit1);
    segmentUnitMetadataArrayList.add(segUnit2);
    segmentUnitMetadataArrayList.add(segUnit3);
    when(segmentMetadata.getSegmentUnitMetadataTable()).thenReturn(segmentUnitMetadataMap);
    when(segmentMetadata.getSegmentUnits()).thenReturn(segmentUnitMetadataArrayList);
    when(segmentMetadata.getLatestMembership()).thenReturn(highestMembershipInSegment);

    segUnit1.setMembership(highestMembershipInSegment);
    segUnit1.setInstanceId(new InstanceId(1));
    segUnit1.setTotalPageToMigrate(100);
    segUnit1.setAlreadyMigratedPage(50);
    segUnit1.setMigrationSpeed(10);
    segUnit1.setStatus(SegmentUnitStatus.Primary);

    segUnit2.setMembership(highestMembershipInSegment);
    segUnit2.setInstanceId(new InstanceId(2));
    segUnit2.setTotalPageToMigrate(100);
    segUnit2.setAlreadyMigratedPage(50);
    segUnit2.setMigrationSpeed(10);
    segUnit2.setStatus(SegmentUnitStatus.Secondary);

    segUnit3.setMembership(highestMembershipInSegment);
    segUnit3.setInstanceId(new InstanceId(3));
    segUnit3.setTotalPageToMigrate(100);
    segUnit3.setAlreadyMigratedPage(50);
    segUnit3.setMigrationSpeed(10);
    segUnit3.setStatus(SegmentUnitStatus.Secondary);

    SegmentVersion segmentVersion = new SegmentVersion(0, 0);

    when(highestMembershipInSegment.getSegmentVersion()).thenReturn(segmentVersion);
    when(highestMembershipInSegment.getPrimary()).thenReturn(new InstanceId(1));
    Set<InstanceId> secondaries = new HashSet<>();
    secondaries.add(new InstanceId(2));
    secondaries.add(new InstanceId(3));
    when(highestMembershipInSegment.getSecondaries()).thenReturn(secondaries);
    when(highestMembershipInSegment.getArbiters()).thenReturn(new HashSet<>());
    when(highestMembershipInSegment.getInactiveSecondaries()).thenReturn(new HashSet<>());
    when(highestMembershipInSegment.getJoiningSecondaries()).thenReturn(new HashSet<>());
    when(highestMembershipInSegment.getMemberIoStatusMap()).thenReturn(new HashMap<>());

    Pair<VolumeMetadata, Long> volumeMetadataLongPair = new Pair<>();
    //merge and report volume
    volumeMetadataLongPair = volumeInformationManger.mergeVolumeInfo(volumeMetadata);
    volumeStore.saveVolume(volumeMetadataLongPair.getFirst());

    ListVolumesRequest listVolumesRequest = new ListVolumesRequest(RequestIdBuilder.get(), 123456);
    ListVolumesResponse listVolumesResponse = informationCenter.listVolumes(listVolumesRequest);
    logger.warn("{}", listVolumesResponse);
    assertEquals(1, listVolumesResponse.getVolumesSize());

    VolumeMetadataThrift volumeGet = listVolumesResponse.getVolumes().get(0);
    assertEquals(30, volumeGet.getMigrationSpeed());
    assertTrue(50.0 == volumeGet.getMigrationRatio());
  }

  @Test
  public void testListVolumeAccessRulesByVolumeIds() {
    VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);

    VolumeMetadata volume2 = generateVolumeMetadata(2, null, 3, 3, 1);
    volumeStore.saveVolume(volume2);
    volumeStore.saveVolumeForReport(volume2);

    VolumeRuleRelationshipInformation volumeRuleRelationshipInformation =
        new VolumeRuleRelationshipInformation(
            1,
            1, 1);
    // relationship 1 volume 1 rule 1
    volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
    volumeRuleRelationshipInformation.setRelationshipId(2);
    volumeRuleRelationshipInformation.setRuleId(2);
    // relationship 2 volume 1 rule 2
    volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
    volumeRuleRelationshipInformation.setRelationshipId(3);
    volumeRuleRelationshipInformation.setRuleId(3);
    // relationship 3 volume 1 rule 3
    volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
    volumeRuleRelationshipInformation.setVolumeId(2);
    volumeRuleRelationshipInformation.setRelationshipId(4);
    volumeRuleRelationshipInformation.setRuleId(1);
    // relationship 4 volume 2 rule 1
    volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
    volumeRuleRelationshipInformation.setRelationshipId(5);
    volumeRuleRelationshipInformation.setRuleId(2);
    // relationship 5 volume 2 rule 2
    volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
    volumeRuleRelationshipInformation.setRelationshipId(6);
    volumeRuleRelationshipInformation.setRuleId(3);
    // relationship 6 volume 2 rule 3
    volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);

    AccessRuleInformation accessRuleInformation = new AccessRuleInformation(1, "", 1);
    accessRuleInformation.setStatus(AccessRuleStatus.APPLIED.APPLIED.name());
    accessRuleStore.save(accessRuleInformation);
    accessRuleInformation.setRuleId(2);
    accessRuleInformation.setStatus(AccessRuleStatus.APPLIED.APPLIED.name());
    accessRuleStore.save(accessRuleInformation);
    accessRuleInformation.setRuleId(3);
    accessRuleInformation.setStatus(AccessRuleStatus.APPLIED.APPLIED.name());
    accessRuleStore.save(accessRuleInformation);

    Set<Long> volumeIds = new ArraySet<>(Long.class);

    try {
      volumeIds.add(1L);
      ListVolumeAccessRulesByVolumeIdsRequest request = new ListVolumeAccessRulesByVolumeIdsRequest(
          1, volumeIds);
      ListVolumeAccessRulesByVolumeIdsResponse response = informationCenter
          .listVolumeAccessRulesByVolumeIds(request);
      assertEquals(1, response.getAccessRulesTableSize());
      List<VolumeAccessRuleThrift> volumeAccessRuleThrifts = response.getAccessRulesTable()
          .get(1L);
      assertEquals(3, volumeAccessRuleThrifts.size());

      volumeIds.add(2L);
      request.setVolumeIds(volumeIds);
      response = informationCenter.listVolumeAccessRulesByVolumeIds(request);
      assertEquals(2, response.getAccessRulesTableSize());
      volumeAccessRuleThrifts = response.getAccessRulesTable().get(1L);
      assertEquals(3, volumeAccessRuleThrifts.size());
      volumeAccessRuleThrifts = response.getAccessRulesTable().get(2L);
      assertEquals(3, volumeAccessRuleThrifts.size());

      volumeIds.clear();
      request.setVolumeIds(volumeIds);
      response = informationCenter.listVolumeAccessRulesByVolumeIds(request);
      assertEquals(0, response.getAccessRulesTableSize());
    } catch (Exception e) {
      logger.error("Cannot list volume access rule by volumeIds!", e);
      fail();
    }
  }

  /**
   * Test mark volume readOnly and then check volume is readOnly. Volume is not connected by any
   * client and is not applied any access rule. Expected no IsNotReadOnlyException.
   */
  @Test
  public void markVolumeReadOnlyAndCheckVolumeIsReadOnlyTest() {
    try {
      VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
      volumeStore.saveVolume(volume);
      volumeStore.saveVolumeForReport(volume);

      //set READONLY
      informationCenter.markVolumeReadWrite(volume.getVolumeId(), ReadWriteTypeThrift.READONLY);
      VolumeMetadata volumeAfterMarkAsReadOnly = volumeStore.getVolume(volume.getVolumeId());
      assertEquals(VolumeMetadata.ReadWriteType.READONLY,
          volumeAfterMarkAsReadOnly.getReadWrite());

      //check
      CheckVolumeIsReadOnlyRequest checkVolumeIsReadOnlyRequest =
          new CheckVolumeIsReadOnlyRequest();
      checkVolumeIsReadOnlyRequest.setRequestId(RequestIdBuilder.get());
      checkVolumeIsReadOnlyRequest.setVolumeId(volume.getVolumeId());
      informationCenter.checkVolumeIsReadOnly(checkVolumeIsReadOnlyRequest);
    } catch (Exception e) {
      logger.error("Caught an exception:", e);
      fail("Caught an exception.");
    }
  }

  /**
   * Test mark volume readOnly and then check volume is readOnly. Volume is not connected by any
   * client but is applied an access rule with ReadWrite permission. Expected
   * VolumeIsMarkReadOnlyExceptionThrift.
   */
  @Test(expected = VolumeIsAppliedWriteAccessRuleExceptionThrift.class)
  public void volumeIsReadOnlyWhenVolumeIsAppliedWriteAccessRuleTest() throws Exception {
    VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
    volumeStore.saveVolume(volume);
    try {
      //set READONLY
      informationCenter.markVolumeReadWrite(volume.getVolumeId(), ReadWriteTypeThrift.READONLY);
    } catch (TException fail) {
      fail();
    }

    try {
      VolumeRuleRelationshipInformation volumeRuleRelationshipInformation =
          new VolumeRuleRelationshipInformation(
              1, 1, 1);
      volumeRuleRelationshipStore.save(volumeRuleRelationshipInformation);
      AccessRuleInformation accessRuleInformation = new AccessRuleInformation(1, "0.0.0.1", 3);
      accessRuleStore.save(accessRuleInformation);

      //check
      CheckVolumeIsReadOnlyRequest checkVolumeIsReadOnlyRequest =
          new CheckVolumeIsReadOnlyRequest();
      checkVolumeIsReadOnlyRequest.setRequestId(RequestIdBuilder.get());
      checkVolumeIsReadOnlyRequest.setVolumeId(volume.getVolumeId());

      informationCenter.checkVolumeIsReadOnly(checkVolumeIsReadOnlyRequest);
    } catch (VolumeIsAppliedWriteAccessRuleExceptionThrift e) {
      throw e;
    }
  }

  /**
   * Test mark volume readOnly and then mark back to readWrite. Volume is not connected by any
   * client and is not applied any access rule. Expected VolumeIsMarkWriteExceptionThrift.
   */
  @Test(expected = VolumeIsMarkWriteExceptionThrift.class)
  public void markVolumeReadOnlyThenBackToReadWrite() throws Exception {
    try {
      VolumeMetadata volume = generateVolumeMetadata(1, null, 3, 3, 1);
      volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
      volumeStore.saveVolume(volume);
      volumeStore.saveVolumeForReport(volume);

      //set READONLY
      informationCenter.markVolumeReadWrite(volume.getVolumeId(), ReadWriteTypeThrift.READONLY);
      VolumeMetadata volumeAfterMarkAsReadOnly = volumeStore.getVolume(volume.getVolumeId());
      assertEquals(VolumeMetadata.ReadWriteType.READONLY,
          volumeAfterMarkAsReadOnly.getReadWrite());

      //set READ_WRITE
      informationCenter.markVolumeReadWrite(volume.getVolumeId(), ReadWriteTypeThrift.READWRITE);

      CheckVolumeIsReadOnlyRequest checkVolumeIsReadOnlyRequest =
          new CheckVolumeIsReadOnlyRequest();
      checkVolumeIsReadOnlyRequest.setRequestId(RequestIdBuilder.get());
      checkVolumeIsReadOnlyRequest.setVolumeId(volume.getVolumeId());

      informationCenter.checkVolumeIsReadOnly(checkVolumeIsReadOnlyRequest);
    } catch (VolumeIsMarkWriteExceptionThrift e) {
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception:", e);
      fail("Caught an exception.");
    }

  }

  @Test
  public void testnewathriftresponseobjectlistisnull() {
    GetVolumeResponse response = new GetVolumeResponse();
    assertNull(response.getVolumeMetadata());
    assertNull(response.getDriverMetadatas());
  }

  @Test
  public void testListArchives_SlotNo() {
    ListArchivesRequestThrift requestThrift = new ListArchivesRequestThrift();
    requestThrift.setRequestId(RequestIdBuilder.get());

    Map<Long, Integer> integerHashMap = new HashMap<>();
    List<InstanceMetadata> insMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata ins = new InstanceMetadata(instanceId);

      integerHashMap.put(instanceId.getId(), i);
      ins.setEndpoint("10.0.2.79:2232");
      ins.setCapacity(15000 + i * 2000);
      ins.setFreeSpace(10000 + i * 2000);
      ins.setDatanodeStatus(InstanceMetadata.DatanodeStatus.OK);

      List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
      for (int j = 0; j < 4; j++) {
        RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
        rawArchiveMetadata.setArchiveId((long) j);
        char diskNo = (char) ((long) 'a' + j);
        rawArchiveMetadata.setSerialNumber("/dev/sd" + diskNo);
        rawArchiveMetadata.setSlotNo(String.valueOf(j));
        rawArchiveMetadata.setArchiveType(ArchiveType.RAW_DISK);
        rawArchiveMetadata.setStorageType(StorageType.SATA);

        rawArchiveMetadataList.add(rawArchiveMetadata);
      }
      ins.setArchives(rawArchiveMetadataList);
      ins.setDatanodeType(NORMAL);

      insMetadataList.add(ins);
    }

    when(storageStore.list()).thenReturn(insMetadataList);

    ListArchivesResponseThrift responseThrift = null;
    try {
      responseThrift = informationCenter.listArchives(requestThrift);
    } catch (TException e) {
      e.printStackTrace();
      assert (false);
    }

    List<InstanceMetadataThrift> instanceMetadataThriftList = responseThrift
        .getInstanceMetadata();

    assertEquals(3, instanceMetadataThriftList.size());

    for (InstanceMetadataThrift instanceMetadataThrift : instanceMetadataThriftList) {
      Long insId = instanceMetadataThrift.getInstanceId();
      assertTrue(integerHashMap.containsKey(insId));
      int i = integerHashMap.get(insId);

      assertEquals(15000 + i * 2000, instanceMetadataThrift.getCapacity());
      assertEquals(10000 + i * 2000, instanceMetadataThrift.getFreeSpace());

      List<ArchiveMetadataThrift> archiveMetadataThriftList = instanceMetadataThrift
          .getArchiveMetadata();
      assertEquals(4, archiveMetadataThriftList.size());

      for (ArchiveMetadataThrift archiveMetadataThrift : archiveMetadataThriftList) {
        int j = (int) archiveMetadataThrift.getArchiveId();
        char diskNo = (char) ((long) 'a' + j);
        assertEquals("/dev/sd" + diskNo, archiveMetadataThrift.getSerialNumber());
        assertEquals(String.valueOf(j), archiveMetadataThrift.getSlotNo());
      }
    }
  }

  /**
   * pool level will changes to HIGH 1 domain, 1 pool in domain1, 3 instance, 1 simple datanode with
   * 1 archive in domain1, 2 normal datanode with 1 archives in pool, 1 PSA volume in pool;.
   */
  @Test
  public void testPoolLevelPsaIsGroupEnough1Simple2Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      volumeStore.saveVolume(volume1);

      storagePool1.addVolumeId(volume1.getVolumeId());
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes to LOW 2 domain, 1 pool in domain1, 3 instance, 1 simple datanode with
   * no archive in domain2, 2 normal datanode with 1 archives in pool, 1 PSA volume in pool;.
   */
  @Test
  public void testPoolLevelPsaIsGroupEnough1SimpleInAnotherDomain2Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);
      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
        instance.setDomainId(2L);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      volumeStore.saveVolume(volume1);

      storagePool1.addVolumeId(volume1.getVolumeId());
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes to HIGH 1 domain, 1 pool in domain1, 3 instance, 1 simple datanode with
   * 1 archive in domain1, 2 normal datanode with 1 archives in pool, 1 PSA volume in pool;.
   */
  @Test
  public void testpoollevelPsaIsgroupenough1Simplewitharchive2Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);
      }

      List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
      for (int j = 0; j < archiveCount; j++) {
        RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
        rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
        rawArchiveMetadata.setInstanceId(instance.getInstanceId());
        rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
        rawArchiveMetadata.setLogicalSpace(archiveSpace);
        rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
        rawArchiveMetadataList.add(rawArchiveMetadata);
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
      }
      instance.setArchives(rawArchiveMetadataList);

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      volumeStore.saveVolume(volume1);

      storagePool1.addVolumeId(volume1.getVolumeId());
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes to MIDDLE, when detach normalDatanode disk 1 domain, 1 pool in domain1,
   * 3 instance, 1 simple datanode with no archive in domain1, 2 normal datanode with 1 archives in
   * pool, 1 PSA volume in pool;.
   */
  @Test
  public void testpoollevelPsaDetachnormaldatanodearchive1Simplewitharchive2Normal()
      throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);
      }

      List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
      for (int j = 0; j < archiveCount; j++) {
        RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
        rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
        rawArchiveMetadata.setInstanceId(instance.getInstanceId());
        rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
        rawArchiveMetadata.setLogicalSpace(archiveSpace);
        rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
        if (i == simpleDatanodeCount && j == 0) {
          rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
        }
        rawArchiveMetadataList.add(rawArchiveMetadata);
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

      }
      instance.setArchives(rawArchiveMetadataList);

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      volumeStore.saveVolume(volume1);

      storagePool1.addVolumeId(volume1.getVolumeId());
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes to LOW, when detach simpleDatanode disk 1 domain, 1 pool in domain1, 3
   * instance, 1 simple datanode with no archive in domain1, 2 normal datanode with 1 archives in
   * pool, 1 PSA volume in pool;.
   */
  @Test
  public void testpoollevelPsaDetachsimpledatanodearchive1Simplewitharchive2Normal()
      throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    appContext.setStatus(InstanceStatus.HEALTHY);

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);
      }

      List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
      for (int j = 0; j < archiveCount; j++) {
        RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
        rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
        rawArchiveMetadata.setInstanceId(instance.getInstanceId());
        rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
        rawArchiveMetadata.setLogicalSpace(archiveSpace);
        rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
        if (i == 0 && j == 0) {
          rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
        }
        rawArchiveMetadataList.add(rawArchiveMetadata);
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

      }
      instance.setArchives(rawArchiveMetadataList);

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      volumeStore.saveVolume(volume1);

      storagePool1.addVolumeId(volume1.getVolumeId());
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.MIDDLE.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes to HIGH 1 domain, 1 pool in domain1, 3 instance, 0 simple datanode with
   * no archive in domain1, 3 normal datanode with 1 archives in pool, 1 PSS volume in pool;..
   */
  @Test
  public void testPoolLevelPss_IsGroupEnough_0Simple_3Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    final long domainId = 1;

    appContext.setStatus(InstanceStatus.HEALTHY);

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 0;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(1L);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.REGULAR);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);
      storagePool1.addVolumeId(volume1.getVolumeId());

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changed to LOW; 1 domain, 1 pool in domain1, 3 instance, 1 simple datanode with
   * no archive in domain1, 2 normal datanode with 1 archives in pool, 1 PSS volume in pool;.
   */
  @Test
  public void testPoolLevelPss_IsGroupEnough_1Simple_2Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    appContext.setStatus(InstanceStatus.HEALTHY);

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.REGULAR);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);
      storagePool1.addVolumeId(volume1.getVolumeId());

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes, when a pool disk detached; 1 domain, 1 pool in domain1, 3 instance, 0
   * simple datanode with no archive in domain1, 3 normal datanode with 1 archives in pool, 1 PSS
   * volume in pool;.
   */
  @Test
  public void testPoolLevelPss_DiskDetach_3Instance_3Archives() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    appContext.setStatus(InstanceStatus.HEALTHY);

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 0;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

          if (i == 0 && j == 0) {
            rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
          }
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.REGULAR);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);
      storagePool1.addVolumeId(volume1.getVolumeId());

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes, when a pool disk detached; 2 domain, 1 pool in domain1, 4 instance, 0
   * simple datanode with no archive in domain1, 4 normal datanode with 1 archives in pool, 1 PSS
   * volume in pool;.
   */
  @Test
  public void testPoolLevelPss_DiskDetach_4Instance_4Archives() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 4;
    int simpleDatanodeCount = 0;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());

          if (i == 0 && j == 0) {
            rawArchiveMetadata.setStatus(ArchiveStatus.OFFLINED);
          }
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);
    storagePool1.setDomainId(domainId);

    //add volume
    for (int i = 0; i < 1; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.REGULAR);
      volume1.setVolumeId(1);
      volume1.setRootVolumeId(1);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);
      storagePool1.addVolumeId(volume1.getVolumeId());

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);

    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.MIDDLE.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will be LOW; 1 domain, 1 pool in domain1, 3 instance, 1 simple datanode with no
   * archive in domain1, 2 normal datanode with 1 archives in pool, 1 PSA volume, 1 PSS volume in
   * pool;.
   */
  @Test
  public void testpoollevelPsapssIsgroupenough1Simple2Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 1;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 0) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 1) {
        volume1.setVolumeType(VolumeType.REGULAR);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will be HIGH; 1 domain, 1 pool in domain1, 3 instance, 0 simple datanode with no
   * archive in domain1, 3 normal datanode with 1 archives in pool, 1 PSA volume, 1 PSS volume in
   * pool;.
   */
  @Test
  public void testpoollevelPsapssIsgroupenough0Simple3Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 3;
    int simpleDatanodeCount = 0;
    int archiveCount = 1;
    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < simpleDatanodeCount) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
          instanceId2ArchiveIdInPool1
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 0) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 1) {
        volume1.setVolumeType(VolumeType.REGULAR);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will be HIGH; 1 domain, 1 pool in domain1, 5 instance, 1 simple datanode with no
   * archive in domain1, 4 normal datanode with 2 archives in pool, 1 PSA volume, 1 PSSAA volume in
   * pool;.
   */
  @Test
  public void testpoollevelPsapssaaIsgroupenough1Simple4Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 5;
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i == 2) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        int archiveCount = 2;
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
    for (InstanceMetadata instance : instanceMetadataList) {
      for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        if (instance.getInstanceId().getId() < 3) {
          instanceId2ArchiveIdInPool2
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
      }
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 1) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 0) {
        volume1.setVolumeType(VolumeType.LARGE);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
    //        assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool2.getStoragePoolLevel()));
  }

  /**
   * pool level will changes, when group not enough; 1 domain, 1 pool in domain1, 4 instance, 1
   * simple datanode with no archive in domain1, 3 normal datanode with 2 archives in pool, 1 PSA
   * volume, 1 PSSAA volume in pool;.
   */
  @Test
  public void testpoollevelPsapssaaIsgroupenough1Simple3Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 4;
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i == 2) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        int archiveCount = 2;
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
    for (InstanceMetadata instance : instanceMetadataList) {
      for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        if (instance.getInstanceId().getId() < 3) {
          instanceId2ArchiveIdInPool2
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
      }
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 0) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 1) {
        volume1.setVolumeType(VolumeType.LARGE);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes, when group not enough; 1 domain, 1 pool in domain1, 5 instance, 2
   * simple datanode with no archive in domain1, 3 normal datanode with 2 archives in pool, 1 PSA
   * volume, 1 PSSAA volume in pool;.
   */
  @Test
  public void testpoollevelPsapssaaIsgroupenough2Simple3Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 5;
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i == 2 || i == 3) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        int archiveCount = 2;
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
    for (InstanceMetadata instance : instanceMetadataList) {
      for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        if (instance.getInstanceId().getId() < 3) {
          instanceId2ArchiveIdInPool2
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
      }
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 1) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 0) {
        volume1.setVolumeType(VolumeType.LARGE);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes, when group not enough; 1 domain, 1 pool in domain1, 5 instance, 3
   * simple datanode with no archive in domain1, 2 normal datanode with 2 archives in pool, 1 PSA
   * volume, 1 PSSAA volume in pool;.
   */
  @Test
  public void testpoollevelPsapssaaIsgroupenough3Simple2Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 5;
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < 3) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        int archiveCount = 2;
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
    for (InstanceMetadata instance : instanceMetadataList) {
      for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        if (instance.getInstanceId().getId() < 3) {
          instanceId2ArchiveIdInPool2
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
      }
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 1) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 0) {
        volume1.setVolumeType(VolumeType.LARGE);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.LOW.name().equals(storagePool1.getStoragePoolLevel()));
  }

  /**
   * pool level will changes, when group not enough; 1 domain, 1 pool in domain1, 5 instance, 0
   * simple datanode with no archive in domain1, 5 normal datanode with 2 archives in pool, 1 PSA
   * volume, 1 PSSAA volume in pool;.
   */
  @Test
  public void testpoollevelPsapssaaIsgroupenough0Simple5Normal() throws Exception {
    long childVolumeId = 1L;
    long volumeSize = 6L;
    long archiveSpace = volumeSize;
    long archiveFreeSpace = 4;
    long domainId = 1;

    final List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = new ArrayList<>();

    //add instance
    int instanceCount = 5;
    List<InstanceMetadata> instanceMetadataList = new ArrayList<>();
    for (int i = 0; i < instanceCount; i++) {
      InstanceId instanceId = new InstanceId(i);
      InstanceMetadata instance = new InstanceMetadata(instanceId);
      instance.setGroup(new Group(i * 10));
      instance.setLastUpdated(System.currentTimeMillis() + 80000);
      instance.setDatanodeStatus(OK);

      instance.setDomainId(domainId);

      if (i < 0) {
        instance.setDatanodeType(SIMPLE);
      } else {
        instance.setDatanodeType(NORMAL);

        List<RawArchiveMetadata> rawArchiveMetadataList = new ArrayList<>();
        int archiveCount = 2;
        for (int j = 0; j < archiveCount; j++) {
          RawArchiveMetadata rawArchiveMetadata = new RawArchiveMetadata();
          rawArchiveMetadata.setArchiveId(Long.valueOf(i * 10 + j));
          rawArchiveMetadata.setInstanceId(instance.getInstanceId());
          rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);
          rawArchiveMetadata.setLogicalSpace(archiveSpace);
          rawArchiveMetadata.setLogicalFreeSpace(archiveFreeSpace);
          rawArchiveMetadataList.add(rawArchiveMetadata);
        }
        instance.setArchives(rawArchiveMetadataList);
      }

      instanceMetadataList.add(instance);

      when(instanceMaintenanceDbStore.getById(instanceId.getId()))
          .thenReturn(new InstanceMaintenanceInformation());
    }

    Multimap<Long, Long> instanceId2ArchiveIdInPool1 = HashMultimap.create();
    Multimap<Long, Long> instanceId2ArchiveIdInPool2 = HashMultimap.create();
    for (InstanceMetadata instance : instanceMetadataList) {
      for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
        instanceId2ArchiveIdInPool1
            .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        if (instance.getInstanceId().getId() < 3) {
          instanceId2ArchiveIdInPool2
              .put(instance.getInstanceId().getId(), rawArchiveMetadata.getArchiveId());
        }
      }
    }

    StoragePool storagePool1 = new StoragePool();
    storagePool1.setPoolId(1L);
    storagePool1.setDomainId(domainId);
    storagePool1.setName("pool1");
    storagePool1.setArchivesInDataNode(instanceId2ArchiveIdInPool1);

    //add volume
    for (int i = 0; i < 2; i++) {
      VolumeMetadata volume1 = new VolumeMetadata();
      volume1.setVolumeType(VolumeType.SMALL);
      volume1.setVolumeId(i);
      volume1.setRootVolumeId(i);
      volume1.setStoragePoolId(storagePool1.getPoolId());
      volume1.setVolumeSize(volumeSize);
      volume1.setVolumeStatus(VolumeStatus.Available);
      volume1.setChildVolumeId(childVolumeId);
      volume1.setVolumeSource(CREATE_VOLUME);
      volume1.setInAction(NULL);

      if (i == 1) {
        volume1.setVolumeType(VolumeType.SMALL);
        storagePool1.addVolumeId(volume1.getVolumeId());
      } else if (i == 0) {
        volume1.setVolumeType(VolumeType.LARGE);
        storagePool1.addVolumeId(volume1.getVolumeId());
      }

      volumeStore.saveVolume(volume1);
    }

    List<StoragePool> storagePoolList = new ArrayList<>();
    storagePoolList.add(storagePool1);

    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    when(instanceMaintenanceDbStore.listAll()).thenReturn(instanceMaintenanceDbStoreList);
    when(storageStore.list()).thenReturn(instanceMetadataList);

    Map<Long, EventDataWorker> poolId2EventDataWorker = new HashMap<>();
    EventDataWorker eventDataWorker = new EventDataWorker("InfoCenter", "aaa", null);
    poolId2EventDataWorker.put(storagePool1.getPoolId(), eventDataWorker);
    storageStoreSweeper.setPoolIdMapEventDataWorker(poolId2EventDataWorker);
    storageStoreSweeper.setSegmentSize(1);

    storageStoreSweeper.doWork();

    assertTrue(StoragePoolLevel.HIGH.name().equals(storagePool1.getStoragePoolLevel()));
  }

  @Test
  public void testCheckAndPutSwitchValue() {
    Long uuid = RequestIdBuilder.get() / 1000000;
    Map<Long, Short> switchValue = new HashMap<>();
    Set<Short> checkSet = new HashSet<>();

    short seedShort = 12345;

    RequestResponseHelper.checkAndPutSwitchValue(uuid, switchValue, checkSet);
    assertEquals(1, switchValue.size());
    assertEquals(1, checkSet.size());
    switchValue.clear();
    checkSet.clear();

    int loopCount = RequestResponseHelper.SHORT_MAX_VALUE;
    for (int i = 0; i < loopCount; i++) {
      Long newuuid = (long) seedShort;
      newuuid += (i * 65536);
      assertEquals(seedShort, newuuid.shortValue());
      RequestResponseHelper.checkAndPutSwitchValue(newuuid, switchValue, checkSet);
    }

    assertEquals(loopCount, switchValue.size());
    assertEquals(loopCount, checkSet.size());
    switchValue.clear();
    checkSet.clear();

    loopCount++;
    try {
      for (int i = 0; i < loopCount; i++) {
        Long newuuid = (long) seedShort;
        newuuid += (i * 65536);
        assertEquals(seedShort, newuuid.shortValue());
        RequestResponseHelper.checkAndPutSwitchValue(newuuid, switchValue, checkSet);
      }
      fail();
    } catch (Exception e) {
      assertEquals(RequestResponseHelper.SHORT_MAX_VALUE, switchValue.size());
      assertEquals(RequestResponseHelper.SHORT_MAX_VALUE, checkSet.size());
    }
  }

  @After
  public void clean() {
    List<VolumeRuleRelationshipInformation> volumeRuleRelationshipInformationList =
        volumeRuleRelationshipStore
            .list();
    for (VolumeRuleRelationshipInformation volumeRuleRelationshipInformation :
        volumeRuleRelationshipInformationList) {
      volumeRuleRelationshipStore.deleteByRuleId(volumeRuleRelationshipInformation.getRuleId());
    }

    volumeStore.clearData();
    statusStore.clear();
  }

  public void clear() {
    logger.warn("after to clear info");
    driverClientStore.clearMemoryData();
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    for (DriverClientInformation driverClientInformation : driverClientInformationList) {
      driverClientStore.deleteValue(driverClientInformation);
    }
  }
  
  private VolumeMetadata createavolumeswitch(long size, VolumeType volumeType, long volumeId,
      long poolId,
      long domainId,
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> segmentIndex2SegmentMap) {
    ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, size, segmentSize,
        volumeType, domainId, poolId);
    for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceId>>> indexMapEntry :
        segmentIndex2SegmentMap
            .entrySet()) {
      int segIndex = indexMapEntry.getKey();
      final SegId segId = new SegId(volumeMetadata.getVolumeId(), segIndex);
      Map<SegmentUnitTypeThrift, List<InstanceId>> map = indexMapEntry.getValue();
      List<InstanceId> arbiterUnits = map.get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceId> segUnits = map.get(SegmentUnitTypeThrift.Normal);

      // build membership
      SegmentMembership membership;
      InstanceId primary = new InstanceId(segUnits.get(0));
      List<InstanceId> secondaries = new ArrayList<>();
      for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
        secondaries.add(new InstanceId(segUnits.get(i)));
      }
      List<InstanceId> arbiters = new ArrayList<>();
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiters.add(new InstanceId(arbiterUnits.get(i)));
      }
      if (arbiters.isEmpty()) {
        membership = new SegmentMembership(primary, secondaries);
      } else {
        membership = new SegmentMembership(primary, secondaries, arbiters);
      }

      // build segment meta data
      SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

      // build segment units
      for (InstanceId arbiter : membership.getArbiters()) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter);
        InstanceId instanceId = new InstanceId(arbiter);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
      normalUnits.add(primary);
      for (InstanceId normalUnit : normalUnits) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
            volumeType, SegmentUnitType.Normal);
        InstanceId instanceId = new InstanceId(normalUnit);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
        segmentUnitMetadata.setArchiveId(archiveId);
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      volumeMetadata.addSegmentMetadata(segment, membership);
    }
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setInAction(NULL);
    //        volumeStore.saveVolume(volumeMetadata);
    return volumeMetadata;
  }

  private void compareTwoSegment(SegmentMetadata beforeSegment, SegmentMetadata afterSegment) {
    assertEquals(beforeSegment.getSegId().getVolumeId().getId(),
        afterSegment.getSegId().getVolumeId().getId());
    for (InstanceId beforeInstanceId : beforeSegment.getSegmentUnitMetadataTable().keySet()) {
      assertTrue(afterSegment.getSegmentUnitMetadataTable().containsKey(beforeInstanceId));
    }

    List<SegmentMembership> beforeMemberships = new ArrayList<>();
    List<SegmentMembership> afterMemberships = new ArrayList<>();

    for (SegmentUnitMetadata beforeUnit : beforeSegment.getSegmentUnitMetadataTable().values()) {
      beforeMemberships.add(beforeUnit.getMembership());
    }
    for (SegmentUnitMetadata afterUnit : afterSegment.getSegmentUnitMetadataTable().values()) {
      afterMemberships.add(afterUnit.getMembership());
    }

    for (SegmentMembership before : beforeMemberships) {
      boolean found = false;
      for (SegmentMembership after : afterMemberships) {
        if (after.equals(before)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }

  private ReportSegmentUnitsMetadataRequest generateSegUnitsMetadataRequest(
      VolumeMetadata volumeMetadata,
      SegmentUnitStatus status) throws JsonProcessingException {
    ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest();
    // report units from instance 1 which is a secondary
    request.setInstanceId(1);
    List<SegmentUnitMetadataThrift> segUnitsMetadata = new ArrayList<>();
    SegmentUnitMetadata segUnitMetadata = TestUtils
        .generateSegmentUnitMetadata(new SegId(volumeMetadata.getVolumeId(), 0), status);

    segUnitMetadata
        .setVolumeMetadataJson(volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
    SegmentUnitMetadataThrift segUnitMetadataThrift = RequestResponseHelper
        .buildThriftSegUnitMetadataFrom(segUnitMetadata);
    segUnitsMetadata.add(segUnitMetadataThrift);
    request.setSegUnitsMetadata(segUnitsMetadata);
    return request;
  }

  private ReportSegmentUnitsMetadataRequest generateSegUnitsMetadataRequest(
      VolumeMetadata volumeMetadata,
      SegmentUnitStatus status, long instanceId, int segmentIndex)
      throws JsonProcessingException {
    ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest();
    request.setInstanceId(instanceId);
    List<SegmentUnitMetadataThrift> segUnitsMetadata = new ArrayList<>();
    SegmentUnitMetadata segUnitMetadata = TestUtils
        .generateSegmentUnitMetadata(new SegId(volumeMetadata.getVolumeId(), segmentIndex), status);
    segUnitMetadata
        .setVolumeMetadataJson(volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
    SegmentUnitMetadataThrift segUnitMetadataThrift = RequestResponseHelper
        .buildThriftSegUnitMetadataFrom(segUnitMetadata);
    segUnitsMetadata.add(segUnitMetadataThrift);
    request.setSegUnitsMetadata(segUnitsMetadata);
    return request;
  }

  private ReportSegmentUnitsMetadataRequest generateSegUnitsMetadataRequestChangMembership(
      VolumeMetadata volumeMetadata, SegmentUnitStatus status, long instanceId, int segmentIndex,
      SegmentMembership segmentMembership) throws JsonProcessingException {
    ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest();
    request.setInstanceId(instanceId);

    SegmentUnitMetadata segUnitMetadata = TestUtils
        .generateSegmentUnitMetadata(new SegId(volumeMetadata.getVolumeId(), segmentIndex), status);

    //reset membership
    if (segmentMembership != null) {
      segUnitMetadata.setMembership(segmentMembership);
    }

    segUnitMetadata
        .setVolumeMetadataJson(volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
    SegmentUnitMetadataThrift segUnitMetadataThrift = RequestResponseHelper
        .buildThriftSegUnitMetadataFrom(segUnitMetadata);
    if (status == SegmentUnitStatus.Arbiter) {
      segUnitMetadataThrift.setSegmentUnitType(SegmentUnitTypeThrift.Arbiter);
    }
    List<SegmentUnitMetadataThrift> segUnitsMetadata = new ArrayList<>();
    segUnitsMetadata.add(segUnitMetadataThrift);
    request.setSegUnitsMetadata(segUnitsMetadata);
    return request;
  }

  public SegmentMembership mockSegmentMembership(SegmentForm segmentForm, int epoch,
      int generation) {
    long primaryId = 1L;
    Long startInstanceId = primaryId;
    InstanceId primary = null;
    Set<InstanceId> secondaries = new HashSet<>();
    Set<InstanceId> joiningSecondaries = new HashSet<>();
    Set<InstanceId> arbiters = new HashSet<>();
    Set<InstanceId> iarbiters = new HashSet<>();
    String name = segmentForm.name();
    for (int i = 0; i < name.length(); i++) {
      switch (name.charAt(i)) {
        case 'P':
          primary = new InstanceId(startInstanceId++);
          break;
        case 'S':
          InstanceId secondary = new InstanceId(startInstanceId++);
          secondaries.add(secondary);
          break;
        case 'J':
          InstanceId joiningSecondary = new InstanceId(startInstanceId++);
          joiningSecondaries.add(joiningSecondary);
          break;
        case 'A':
          InstanceId arbiter = new InstanceId(startInstanceId++);
          arbiters.add(arbiter);
          break;
        case 'I':
          InstanceId iarbiter = new InstanceId(startInstanceId++);
          iarbiters.add(iarbiter);
          break;
        default:
          break;
      }
    }

    return new SegmentMembership(new SegmentVersion(epoch, generation), primary, primary,
        secondaries, arbiters,
        iarbiters, joiningSecondaries, null, null);
  }

  private VolumeMetadata generateVolumeMetadata(long volumeId, Long childVolumeId, long volumeSize,
      long segmentSize,
      int version) {
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, volumeSize, segmentSize,
        VolumeType.REGULAR, 0L, 0L);

    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setChildVolumeId(childVolumeId);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    volumeMetadata.setName("testvolume" + InfoCenterConstants.name);
    volumeMetadata.setVolumeSource(CREATE_VOLUME);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setLastExtendedTime(new Date(System.currentTimeMillis()));
    volumeMetadata.setSegmentTable(new HashMap<>());
    while (version-- > 0) {
      volumeMetadata.incVersion();
    }
    return volumeMetadata;
  }

  private VolumeMetadata generateVolumeMetadata2(long rootVolumeId, Long volumeId,
      Long childVolumeId,
      String volumeName, long volumeSize, long segmentSize,
      int version, List<SegId> oriSegIdList) {
    VolumeMetadata volumeMetadata = new VolumeMetadata(rootVolumeId, volumeId, volumeSize,
        segmentSize,
        VolumeType.REGULAR, 0L, 0L);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setChildVolumeId(childVolumeId);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    volumeMetadata.setName(volumeName);
    volumeMetadata.setVolumeSource(CREATE_VOLUME);
    for (int i = 0; i < volumeSize / segmentSize; i++) {
      SegId segId = new SegId(volumeId, i);
      oriSegIdList.add(segId);
      SegmentMetadata segmentMetadata = TestUtils.generateSegmentMetadata(segId);
      volumeMetadata
          .addSegmentMetadata(segmentMetadata, TestUtils.generateMembership(VolumeType.REGULAR));
    }
    while (version-- > 0) {
      volumeMetadata.incVersion();
    }

    return volumeMetadata;
  }
}