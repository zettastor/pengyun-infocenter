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

package py.infocenter.volumemanager;

import static org.mockito.Mockito.when;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.icshare.DomainStore;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.HaInstanceMangerWithZk;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.instance.manger.VolumesForEquilibrium;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.reportvolume.ReportVolumeManager;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.DriverStore;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.OrphanVolumeStore;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStatusTransitionStoreImpl;
import py.infocenter.test.utils.TestBeans;
import py.infocenter.worker.HaInstanceEquilibriumSweeper;
import py.infocenter.worker.StorageStoreSweeper;
import py.infocenter.worker.VolumeActionSweeper;
import py.infocenter.worker.VolumeSweeper;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.infocenter.service.ReportArchivesRequest;
import py.thrift.infocenter.service.ReportArchivesResponse;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataRequest;
import py.thrift.infocenter.service.ReportVolumeInfoRequest;
import py.thrift.infocenter.service.ReportVolumeInfoResponse;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class EquilibriumVolumeTest extends TestBase {

  long deadVolumeToRemove = 15552000;
  VolumeActionSweeper volumeActionSweeper = null;
  @Autowired
  private SessionFactory sessionFactory;
  private MyInformationCenterImpl myInformationCenter;
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
  private OrphanVolumeStore orphanVolumeStore;
  @Mock
  private DomainStore domainStore;
  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private InfoCenterAppContext appContext;
  private DriverStore driverStore;
  private AccessRuleStore accessRuleStore;
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
  private long segmentSize = 100;
  private long volumeSize = 100;
  private StorageStoreSweeper storageStoreSweeper;

  private VolumeInformationManger volumeInformationManger;

  private MyInstanceIncludeVolumeInfoManger myInstanceIncludeVolumeInfoManger;

  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  private VolumeSweeper volumeSweeper;

  private HaInstanceManger haInstanceManger;

  private InstanceVolumesInformationStore instanceVolumesInformationStore;

  private Map<Long, VolumeMetadata> volumeTableFollower1;
  private Map<Long, VolumeMetadata> volumeTableFollower2;
  private Map<Long, VolumeMetadata> volumeTableFollower3;

  private HaInstanceEquilibriumSweeper haInstanceMoveVolumeSweeper;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  private ReportVolumeManager reportVolumeManager;


  @Before
  public void setup() {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);
    instanceVolumesInformationStore = (InstanceVolumesInformationStore) ctx
        .getBean("instanceVolumesInformationStoreTest");

    myInformationCenter = new MyInformationCenterImpl();
    myInformationCenter.setInstanceStore(instanceStore);
    volumeStore = new TwoLevelVolumeStoreImpl(new MemoryVolumeStoreImpl(), dbVolumeStore);
    myInformationCenter.setVolumeStore(volumeStore);
    driverStore = ctx.getBean(DriverStore.class);
    accessRuleStore = ctx.getBean(AccessRuleStore.class);
    volumeRuleRelationshipStore = ctx.getBean(VolumeRuleRelationshipStore.class);

    myInformationCenter.setDriverStore(driverStore);
    myInformationCenter.setSegmentSize(100);
    myInformationCenter.setDeadVolumeToRemove(deadVolumeToRemove);
    myInformationCenter.setSegmentUnitTimeoutStore(timeoutStore);
    myInformationCenter.setVolumeStatusStore(statusStore);
    myInformationCenter.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    myInformationCenter.setAppContext(appContext);
    myInformationCenter.setOrphanVolumes(orphanVolumeStore);
    myInformationCenter.setDomainStore(domainStore);
    myInformationCenter.setStoragePoolStore(storagePoolStore);
    myInformationCenter.setAccessRuleStore(accessRuleStore);
    myInformationCenter.setStorageStore(storageStore);
    myInformationCenter.setPageWrappCount(128);
    myInformationCenter.setSegmentWrappCount(10);

    volumeActionSweeper = new VolumeActionSweeper();
    volumeActionSweeper.setAppContext(appContext);
    volumeActionSweeper.setVolumeStore(volumeStore);

    volumeSweeper = new VolumeSweeper();
    volumeSweeper.setVolumeStatusTransitionStore(statusStore);
    volumeSweeper.setAppContext(appContext);
    volumeSweeper.setVolumeStore(volumeStore);

    RebalanceRuleStore rebalanceRuleStore = new RebalanceRuleStoreImpl();

    SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
        new SegmentUnitsDistributionManagerImpl(
            segmentSize, volumeStore, storageStore, storagePoolStore, rebalanceRuleStore,
            domainStore);

    storageStoreSweeper = new StorageStoreSweeper();
    storageStoreSweeper.setAppContext(appContext);
    storageStoreSweeper.setStoragePoolStore(storagePoolStore);
    storageStoreSweeper.setInstanceMetadataStore(storageStore);
    storageStoreSweeper.setDomainStore(domainStore);
    storageStoreSweeper.setVolumeStore(volumeStore);

    DriverContainerSelectionStrategy balancedDriverContainerSelectionStrategy =
        new BalancedDriverContainerSelectionStrategy();
    myInformationCenter
        .setDriverContainerSelectionStrategy(balancedDriverContainerSelectionStrategy);

    //init InstanceIncludeVolumeInfoManger
    myInstanceIncludeVolumeInfoManger = new MyInstanceIncludeVolumeInfoManger();
    myInstanceIncludeVolumeInfoManger
        .setInstanceVolumesInformationStore(instanceVolumesInformationStore);

    //init HaInstanceMangerWithZK
    haInstanceManger = new HaInstanceMangerWithZk(instanceStore);

    //init VolumeInformationManger
    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(myInstanceIncludeVolumeInfoManger);

    //init InstanceVolumeInEquilibriumManger
    instanceVolumeInEquilibriumManger = new InstanceVolumeInEquilibriumManger();
    instanceVolumeInEquilibriumManger
        .setInstanceIncludeVolumeInfoManger(myInstanceIncludeVolumeInfoManger);

    myInformationCenter.setVolumeInformationManger(volumeInformationManger);
    myInformationCenter.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    myInformationCenter.setInstanceIncludeVolumeInfoManger(myInstanceIncludeVolumeInfoManger);

    //init haInstanceMoveVolumeSweeper
    haInstanceMoveVolumeSweeper = new HaInstanceEquilibriumSweeper();
    haInstanceMoveVolumeSweeper.setEnableInstanceEquilibriumVolume(true);
    haInstanceMoveVolumeSweeper.setAppContext(appContext);
    haInstanceMoveVolumeSweeper
        .setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);

    //init the follower volume table
    volumeTableFollower1 = new ConcurrentHashMap<>();
    volumeTableFollower2 = new ConcurrentHashMap<>();
    volumeTableFollower3 = new ConcurrentHashMap<>();

    //lockForSaveVolumeInfo
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();
    myInformationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    //reportVolumeManager
    reportVolumeManager = new ReportVolumeManager();
    reportVolumeManager.setVolumeStore(volumeStore);
    reportVolumeManager.setStoragePoolStore(storagePoolStore);
    reportVolumeManager.setInstanceIncludeVolumeInfoManger(myInstanceIncludeVolumeInfoManger);
    reportVolumeManager.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    reportVolumeManager.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    myInformationCenter.setReportVolumeManager(reportVolumeManager);


  }

  @Test
  public void testEquilibriumVolumeReportVolumeInfoNewInstanceEquilibriumOk() throws Exception {
    /* there is three instance
     *  master  followerFrom (volumeIdFlowerA)  followerTo ()
     *  volumeIdFlowerA - > follower2
     * */

    long volumeIdA = 1111;
    final long instanceFlowerFrom = 1;
    final long instanceFlowerTo = 2;
    final long instanceMasterId = 3;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeIdA, null, 6, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeA = new VolumeMetadata();
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeA.deepCopy(volumeInMaster);
    volumeMetadata.deepCopy(volumeInMaster);

    //follower1 report A
    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeA);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    /* init instance master and follower **/
    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(instanceFlowerFrom, new Instance(new InstanceId(String.valueOf(instanceFlowerFrom)),
            new Group(1), "test2", InstanceStatus.SUSPEND));
    flowerInstanceMap
        .put(instanceFlowerTo, new Instance(new InstanceId(String.valueOf(instanceFlowerTo)),
            new Group(1), "test3", InstanceStatus.SUSPEND));

    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceMasterId)),
        new Group(1), "test1", InstanceStatus.HEALTHY);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    /* 1. the followerFrom report A to master ***/
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceFlowerFrom,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    myInformationCenter.reportVolumeInfo(request);

    //check the followerFrom report volume good
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = myInstanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      Assert.assertTrue(entry.getKey() == instanceFlowerFrom);
      Assert.assertTrue(entry.getValue().containsValue(volumeIdA));
    }

    /* 2. master init the Equilibrium table **/
    //for  {1={1111=2}}
    initEquilibriumTable(volumeIdA, instanceFlowerFrom, instanceFlowerTo);

    /* 3. begin Equilibrium volume, the data node report volume A to followerFrom and followerTo
     *  same time ***/

    /* 4. the followerFrom and followerTo report volume A to master ***/

    //4.1 followerTo report A to master
    List<VolumeMetadata> volumeMetadataListFollowerTo = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteListFollowerTo = new ArrayList<>();
    volumeMetadataListFollowerTo.add(volumeMetadata);

    request = generateReportVolumeInfoRequest(instanceFlowerTo, volumeMetadataListFollowerTo,
        volumeMetadataToDeleteListFollowerTo);

    myInformationCenter.reportVolumeInfo(request);

    //check, the A not Equilibrium ok. the volume still in instanceFlowerFrom
    InstanceToVolumeInfo instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap().get(instanceFlowerFrom);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    //4.2 followerFrom report A to master
    request = generateReportVolumeInfoRequest(instanceFlowerFrom, volumeMetadataList,
        volumeMetadataToDeleteList);
    myInformationCenter.reportVolumeInfo(request);

    //check Equilibriume is still
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    Assert.assertTrue(instanceVolumeInEquilibriumManger.checkVolumeEquilibriumOk(volumeIdA) == 0);
    AtomicLong version = instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion();
    Assert.assertTrue(version.get() == 0); // the version not change

    //the Equilibrium table is not empty
    Assert.assertTrue(
        !instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId()
            .isEmpty());

    //4.3 followerTo report A to master, the volumeMetadata status is ok
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    request = generateReportVolumeInfoRequest(instanceFlowerTo, volumeMetadataListFollowerTo,
        volumeMetadataToDeleteListFollowerTo);

    myInformationCenter.reportVolumeInfo(request);

    //check, the volumeA Equilibrium ok
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(
        !instanceToVolumeInfo.containsValue(volumeIdA)); //volume A not in instanceFlowerFrom

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA)); //volume A in instanceFlowerTo

    version = instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion();
    Assert.assertTrue(version.get() == 1); // the version update

    long instanceIdOld = instanceVolumeInEquilibriumManger.checkVolumeEquilibriumOk(volumeIdA);
    Assert.assertTrue(instanceIdOld == instanceFlowerFrom); // the Equilibrium Ok

    //the Equilibrium table is empty
    Assert.assertTrue(
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId()
            .isEmpty());
    Assert.assertTrue(instanceVolumeInEquilibriumManger.checkVolumeEquilibriumOk(volumeIdA)
        == instanceFlowerFrom);

    // check the update datanode table
    Map<Long, Map<Long, Long>> updateTheDatanodeReportTable = instanceVolumeInEquilibriumManger
        .getUpdateTheDatanodeReportTable();
    for (Map.Entry<Long, Map<Long, Long>> entry : updateTheDatanodeReportTable.entrySet()) {
      long volumeId = entry.getKey();
      Map<Long, Long> value = entry.getValue();

      Assert.assertTrue(volumeId == volumeIdA);
      for (Map.Entry<Long, Long> entry1 : value.entrySet()) {
        Assert.assertTrue(entry1.getKey() == instanceFlowerFrom);
        Assert.assertTrue(entry1.getValue() == instanceFlowerTo);
      }
    }

    //4.4 followerFrom report A to master, and the A Equilibrium ok, so delete it in 
    // instanceFlowerFrom
    request = generateReportVolumeInfoRequest(instanceFlowerFrom, volumeMetadataList,
        volumeMetadataToDeleteList);
    ReportVolumeInfoResponse response = myInformationCenter.reportVolumeInfo(request);

    //check, the response
    Set<Long> notReportThisVolume = response.getNotReportThisVolume();
    Assert.assertTrue(notReportThisVolume.contains(volumeIdA));
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    //4.5 followerTo report A to master, the volumeMetadata status is ok
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    request = generateReportVolumeInfoRequest(instanceFlowerTo, volumeMetadataListFollowerTo,
        volumeMetadataToDeleteListFollowerTo);

    response = myInformationCenter.reportVolumeInfo(request);

    //check, the response
    notReportThisVolume = response.getNotReportThisVolume();
    Assert.assertTrue(!notReportThisVolume.contains(volumeIdA)); //not notify to instanceFlowerFrom
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    /* 5.the datanode update the report volume table ok ***/
    //5.1 wait datanode report

    Assert.assertTrue(!instanceVolumeInEquilibriumManger.equilibriumOk());

    //5.1 wait datanode report, datanode update ok
    Set<Long> volumeIds = new HashSet<>();
    volumeIds.add(volumeIdA);
    instanceVolumeInEquilibriumManger.removeVolumeWhenDatanodeUpdateOk(volumeIds);

    Assert.assertTrue(!instanceVolumeInEquilibriumManger.equilibriumOk());
  }

  @Test
  public void testEquilibriumVolume_ReportVolumeInfo_NewInstanceEquilibrium_Failed()
      throws Exception {
    /* there is three instance
     *  master  followerFrom (volumeIdFlowerA)  followerTo ()
     *  volumeIdFlowerA - > follower2
     * */

    long volumeIdA = 1111;
    final long instanceFlowerFrom = 1;
    final long instanceFlowerTo = 2;
    final long instanceMasterId = 3;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeIdA, null, 6, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeA = new VolumeMetadata();
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeA.deepCopy(volumeInMaster);
    volumeMetadata.deepCopy(volumeInMaster);

    //follower1 report A
    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeA);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    /* init instance master and follower **/
    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(instanceFlowerFrom, new Instance(new InstanceId(String.valueOf(instanceFlowerFrom)),
            new Group(1), "test2", InstanceStatus.SUSPEND));
    flowerInstanceMap
        .put(instanceFlowerTo, new Instance(new InstanceId(String.valueOf(instanceFlowerTo)),
            new Group(1), "test3", InstanceStatus.SUSPEND));

    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceMasterId)),
        new Group(1), "test1", InstanceStatus.HEALTHY);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    /* 1. the followerFrom report A to master ***/
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceFlowerFrom,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    myInformationCenter.reportVolumeInfo(request);

    //check the followerFrom report volume good
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = myInstanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      Assert.assertTrue(entry.getKey() == instanceFlowerFrom);
      Assert.assertTrue(entry.getValue().containsValue(volumeIdA));
    }

    /* 2. master init the Equilibrium table **/
    //for  {1={1111=2}}
    initEquilibriumTable(volumeIdA, instanceFlowerFrom, instanceFlowerTo);

    /* 3. begin Equilibrium volume, the data node report volume A to followerFrom and followerTo
     *  same time ***/

    /* 4. the followerFrom and followerTo report volume A to master ***/

    //4.1 followerTo report A to master
    List<VolumeMetadata> volumeMetadataListFollowerTo = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteListFollowerTo = new ArrayList<>();
    volumeMetadataListFollowerTo.add(volumeMetadata);

    request = generateReportVolumeInfoRequest(instanceFlowerTo, volumeMetadataListFollowerTo,
        volumeMetadataToDeleteListFollowerTo);

    myInformationCenter.reportVolumeInfo(request);

    //check, the A not Equilibrium ok. the volume still in instanceFlowerFrom
    InstanceToVolumeInfo instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap().get(instanceFlowerFrom);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    //4.2 followerFrom report A to master
    request = generateReportVolumeInfoRequest(instanceFlowerFrom, volumeMetadataList,
        volumeMetadataToDeleteList);
    myInformationCenter.reportVolumeInfo(request);

    //check Equilibriume is still
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    Assert.assertTrue(instanceVolumeInEquilibriumManger.checkVolumeEquilibriumOk(volumeIdA) == 0);
    AtomicLong version = instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion();
    Assert.assertTrue(version.get() == 0); // the version not change

    //the Equilibrium table is not empty
    Assert.assertTrue(
        !instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId()
            .isEmpty());

    //4.3 followerTo report A to master, the volumeMetadata status is ok
    volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
    request = generateReportVolumeInfoRequest(instanceFlowerTo, volumeMetadataListFollowerTo,
        volumeMetadataToDeleteListFollowerTo);

    myInformationCenter.reportVolumeInfo(request);

    //check, the volumeA Equilibrium ok
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(
        instanceToVolumeInfo.containsValue(volumeIdA)); //volume A  in instanceFlowerFrom

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(
        !instanceToVolumeInfo.containsValue(volumeIdA)); //volume A not in instanceFlowerTo

    version = instanceVolumeInEquilibriumManger.getUpdateReportToInstancesVersion();
    Assert.assertTrue(version.get() == 0); // the version update

    // check the update datanode table
    Map<Long, Map<Long, Long>> updateTheDatanodeReportTable = instanceVolumeInEquilibriumManger
        .getUpdateTheDatanodeReportTable();
    Assert.assertTrue(updateTheDatanodeReportTable.isEmpty());

    /* 5.wait a long time ,the Equilibrium again, but last time the Equilibrium not ok
     *   clear the old Equilibrium table information
     * ***/
    haInstanceMoveVolumeSweeper.doWork();

    //check
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(
        instanceToVolumeInfo.containsValue(volumeIdA)); //volume A  in instanceFlowerFrom

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(
        !instanceToVolumeInfo.containsValue(volumeIdA)); //volume A not in instanceFlowerTo

    //the Equilibrium table is not empty
    Assert.assertTrue(
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibrium().isEmpty());
    Assert.assertTrue(
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId()
            .isEmpty());

    /* 6.wait a long time ,the Equilibrium again, but last time the Equilibrium not ok ***/
    //6.1 followerFrom report A to master
    request = generateReportVolumeInfoRequest(instanceFlowerFrom, volumeMetadataList,
        volumeMetadataToDeleteList);

    myInformationCenter.reportVolumeInfo(request);

    //check A in VolumeInfoManger instanceFlowerFrom
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    //6.2 followerTo report A to master
    volumeMetadataListFollowerTo = new ArrayList<>();
    volumeMetadataToDeleteListFollowerTo = new ArrayList<>();
    volumeMetadataListFollowerTo.add(volumeMetadata);

    request = generateReportVolumeInfoRequest(instanceFlowerTo, volumeMetadataListFollowerTo,
        volumeMetadataToDeleteListFollowerTo);

    final ReportVolumeInfoResponse response = myInformationCenter.reportVolumeInfo(request);

    //check
    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerFrom);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdA));

    instanceToVolumeInfo = myInstanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap()
        .get(instanceFlowerTo);
    Assert.assertTrue(!instanceToVolumeInfo.containsValue(volumeIdA));

    //check response, remove A in instanceFlowerTo,
    Set<Long> notReportThisVolume = response.getNotReportThisVolume();
    Assert.assertTrue(notReportThisVolume.contains(volumeIdA));

  }


  /**
   * test the all EquilibriumVolume logic.
   ****/
  @Test
  @Ignore
  public void testEquilibriumVolume_AllLogic() throws Exception {
    /* there is three instance
     *  master  follower1 (volumeA)  follower2 (volumeB)  follower3 ()
     *  volumeIdFlowerA - > follower2
     * */

    Map<Long, Map<Long, Long>> volumeSizeInfo = new HashMap<>();

    /* init 1 ***/
    /* init the instance 111111 **/
    Map<Long, Long> theInstanceVolume = new HashMap<>();
    theInstanceVolume.put(123451L, 1L);
    theInstanceVolume.put(123452L, 2L);
    volumeSizeInfo.put(111111L, theInstanceVolume);

    /* init the instance 111112 **/
    Map<Long, Long> theInstanceVolume2 = new HashMap<>();
    theInstanceVolume2.put(223451L, 3L);
    theInstanceVolume2.put(223452L, 2L);
    volumeSizeInfo.put(111112L, theInstanceVolume2);

    /* init the instance 111113 **/
    Map<Long, Long> theInstanceVolume3 = new HashMap<>();
    theInstanceVolume3.put(323451L, 2L);
    volumeSizeInfo.put(111113L, theInstanceVolume3);

    logger.warn("the begin map :{}", volumeSizeInfo);

    /* begin the Equilibrium work ***/

    haInstanceMoveVolumeSweeper.doWork();

    Map<Long, Map<Long, Long>> result = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibrium();
    logger.warn("the result value is :{}", result);

    long volumeIdA = 1111;
    long instanceFlower1 = 1;
    long instanceFlower2 = 2;
    long instanceFlower3 = 3;
    long instanceMasterId = 4;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeIdA, null, 6, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeA = new VolumeMetadata();
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeA.deepCopy(volumeInMaster);
    volumeMetadata.deepCopy(volumeInMaster);

    //follower1 report A
    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeA);


  }


  public VolumeMetadata generateVolumeMetadata(long volumeId, Long childVolumeId, long volumeSize,
      long segmentSize,
      int version) {
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, volumeSize, segmentSize,
        VolumeType.REGULAR, 0L, 0L);

    volumeMetadata.setVolumeId(volumeId);
    volumeMetadata.setRootVolumeId(volumeId);
    volumeMetadata.setChildVolumeId(null);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setExtendingSize(0);
    volumeMetadata.setName("test");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Creating);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(volumeSize);
    volumeMetadata.setDeadTime(0L);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setPageWrappCount(128);
    volumeMetadata.setSegmentWrappCount(10);
    volumeMetadata.setVersion(version);
    volumeMetadata.setVolumeLayout("test");

    return volumeMetadata;
  }

  private void initEquilibriumTable(long volumeId, long instanceIdFrom, long instanceIdTo) {
    //init the Equilibrium table
    Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibrium = new ConcurrentHashMap<>();
    Map<Long, Long> value1 = new HashMap<>();
    value1.put(volumeId, instanceIdTo);
    volumeReportToInstanceEquilibrium.put(instanceIdFrom, value1);

    //init the Equilibrium table with volume
    Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibriumBuildWithVolumeId =
        new ConcurrentHashMap<>();
    Map<Long, Long> value2 = new HashMap<>();
    value2.put(instanceIdFrom, instanceIdTo);
    volumeReportToInstanceEquilibriumBuildWithVolumeId.put(volumeId, value2);

    instanceVolumeInEquilibriumManger
        .setVolumeReportToInstanceEquilibrium(volumeReportToInstanceEquilibrium);
    instanceVolumeInEquilibriumManger.setVolumeReportToInstanceEquilibriumBuildWithVolumeId(
        volumeReportToInstanceEquilibriumBuildWithVolumeId);

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
      SegmentUnitStatus status, long instanceId, int segmentIndex) throws JsonProcessingException {
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

  private ReportVolumeInfoRequest generateReportVolumeInfoRequest(long instanceId,
      List<VolumeMetadata> volumeMetadataList,
      List<VolumeMetadata> volumeMetadataToDeleteList) {
    List<VolumeMetadataThrift> volumeMetadataThriftList = new ArrayList<>(
        volumeMetadataList.size());
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>(volumeMetadataList.size());
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      volumeMetadataThriftList
          .add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
      Map<Integer, SegmentMetadata> segmentTable = volumeMetadata.getSegmentTable();
      long number = 0;
      for (Map.Entry<Integer, SegmentMetadata> entry : segmentTable.entrySet()) {
        number += entry.getValue().getSegmentUnitCount();
      }
      totalSegmentUnitMetadataNumberMap.put(volumeMetadata.getVolumeId(), number);
    }

    List<VolumeMetadataThrift> volumeMetadataThriftListToDelete = new ArrayList<>(
        volumeMetadataToDeleteList.size());
    for (VolumeMetadata volumeMetadata : volumeMetadataToDeleteList) {
      volumeMetadataThriftListToDelete
          .add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
    }

    ReportVolumeInfoRequest request = new ReportVolumeInfoRequest(RequestIdBuilder.get(),
        instanceId, "test", volumeMetadataThriftList, volumeMetadataThriftListToDelete,
        totalSegmentUnitMetadataNumberMap);
    return request;
  }


  public class MyInformationCenterImpl extends InformationCenterImpl {


    @Override
    public ReportArchivesResponse reportArchives(ReportArchivesRequest request)
        throws
        TException {
      return super.reportArchives(request);
    }


    @Override
    public ReportVolumeInfoResponse reportVolumeInfo(ReportVolumeInfoRequest request)
        throws TException {
      return super.reportVolumeInfo(request);


    }
  }

  class MyInstanceIncludeVolumeInfoManger extends InstanceIncludeVolumeInfoManger {

    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    @Override
    public MultiValueMap<Long, VolumesForEquilibrium> getEachHaVolumeSizeInfo() {
      return volumeSizeInfo;
    }

    public void setVolumeSizeInfo(MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo) {
      this.volumeSizeInfo = volumeSizeInfo;
    }
  }


}