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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.volume.VolumeInAction.EXTENDING;
import static py.volume.VolumeMetadata.VolumeSourceType.CREATE_VOLUME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.icshare.DomainStore;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.HaInstanceMangerWithZk;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.reportvolume.ReportVolumeManager;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.DriverStore;
import py.infocenter.store.InstanceVolumesInformation;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.OrphanVolumeStore;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.StorageStore;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStatusTransitionStoreImpl;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreImpl;
import py.infocenter.test.utils.TestBeans;
import py.infocenter.worker.HaInstanceCheckSweeperFactory;
import py.infocenter.worker.StorageStoreSweeper;
import py.infocenter.worker.VolumeActionSweeper;
import py.infocenter.worker.VolumeSweeper;
import py.informationcenter.StoragePoolStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.test.TestBase;
import py.test.TestUtils;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataRequest;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataResponse;
import py.thrift.infocenter.service.ReportVolumeInfoRequest;
import py.thrift.infocenter.service.ReportVolumeInfoResponse;
import py.thrift.infocenter.service.SegUnitConflictThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitStatusConflictCauseThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class InformationCenterImplAddTest extends TestBase {

  long deadVolumeToRemove = 15552000;
  VolumeActionSweeper volumeActionSweeper = null;
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
  private OrphanVolumeStore orphanVolumeStore;
  @Mock
  private DomainStore domainStore;
  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private OperationStore operationStore;
  private DriverStore driverStore;
  private AccessRuleStore accessRuleStore;
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
  private long segmentSize = 100;
  private long volumeSize = 100;
  private long extendSize = 100;
  private StorageStoreSweeper storageStoreSweeper;

  private VolumeInformationManger volumeInformationManger;

  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;

  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  private VolumeSweeper volumeSweeper;

  private HaInstanceManger haInstanceManger;

  private InstanceVolumesInformationStore instanceVolumesInformationStore;

  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  @Mock
  private VolumeJobStoreImpl volumeJobStore;

  private ReportVolumeManager reportVolumeManager;


  @Before
  public void setup() throws SQLException, IOException {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);
    instanceVolumesInformationStore = (InstanceVolumesInformationStore) ctx
        .getBean("instanceVolumesInformationStoreTest");

    /* clean the info ***/
    List<InstanceVolumesInformation> instanceVolumesInformationList =
        instanceVolumesInformationStore
            .reloadAllInstanceVolumesInformationFromDb();
    for (InstanceVolumesInformation instanceVolumesInformation : instanceVolumesInformationList) {
      instanceVolumesInformationStore
          .deleteInstanceVolumesInformationFromDb(instanceVolumesInformation.getInstanceId());
    }

    informationCenter = new InformationCenterImpl();
    informationCenter.setInstanceStore(instanceStore);
    volumeStore = new TwoLevelVolumeStoreImpl(new MemoryVolumeStoreImpl(), dbVolumeStore);
    informationCenter.setVolumeStore(volumeStore);
    driverStore = ctx.getBean(DriverStore.class);
    accessRuleStore = ctx.getBean(AccessRuleStore.class);
    volumeRuleRelationshipStore = ctx.getBean(VolumeRuleRelationshipStore.class);

    informationCenter.setDriverStore(driverStore);
    informationCenter.setSegmentSize(100);
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

    RebalanceRuleStore rebalanceRuleStore = new RebalanceRuleStoreImpl();
    //SegmentUnitsDistributionManagerImpl
    final SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
        new SegmentUnitsDistributionManagerImpl(
            segmentSize, volumeStore, storageStore, storagePoolStore, rebalanceRuleStore,
            domainStore);

    volumeActionSweeper = new VolumeActionSweeper();
    volumeActionSweeper.setAppContext(appContext);
    volumeActionSweeper.setVolumeStore(volumeStore);
    volumeActionSweeper.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager);

    volumeSweeper = new VolumeSweeper();
    volumeSweeper.setVolumeStatusTransitionStore(statusStore);
    volumeSweeper.setAppContext(appContext);
    volumeSweeper.setVolumeStore(volumeStore);
    volumeSweeper.setVolumeJobStoreDb(volumeJobStore);
    volumeSweeper.setOperationStore(operationStore);

    storageStoreSweeper = new StorageStoreSweeper();
    storageStoreSweeper.setAppContext(appContext);
    storageStoreSweeper.setStoragePoolStore(storagePoolStore);
    storageStoreSweeper.setInstanceMetadataStore(storageStore);
    storageStoreSweeper.setDomainStore(domainStore);
    storageStoreSweeper.setVolumeStore(volumeStore);

    DriverContainerSelectionStrategy balancedDriverContainerSelectionStrategy =
        new BalancedDriverContainerSelectionStrategy();
    informationCenter.setDriverContainerSelectionStrategy(balancedDriverContainerSelectionStrategy);

    //init InstanceIncludeVolumeInfoManger
    instanceIncludeVolumeInfoManger = new InstanceIncludeVolumeInfoManger();
    instanceIncludeVolumeInfoManger
        .setInstanceVolumesInformationStore(instanceVolumesInformationStore);
    instanceIncludeVolumeInfoManger.clearVolumeInfoMap();

    //init InstanceVolumeInEquilibriumManger
    instanceVolumeInEquilibriumManger = new InstanceVolumeInEquilibriumManger();
    instanceVolumeInEquilibriumManger
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    //init HaInstanceMangerWithZK
    haInstanceManger = new HaInstanceMangerWithZk(instanceStore);

    //init VolumeInformationManger
    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    volumeInformationManger.setAppContext(appContext);

    informationCenter.setVolumeInformationManger(volumeInformationManger);
    informationCenter.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    informationCenter.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    //lockForSaveVolumeInfo
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();
    informationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    //reportVolumeManager
    reportVolumeManager = new ReportVolumeManager();
    reportVolumeManager.setVolumeStore(volumeStore);
    reportVolumeManager.setStoragePoolStore(storagePoolStore);
    reportVolumeManager.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    reportVolumeManager.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    reportVolumeManager.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    informationCenter.setReportVolumeManager(reportVolumeManager);
  }

  @Test
  public void testReportVolumeInfoOneFlowerReportVolumeStatus() throws Exception {

    final long instanceId = 1L;
    long volumeId = 1;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* 1 .VolumeStatus.Creating **/
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /* 2. VolumeStatus.Available **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Available);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /* 3 .VolumeStatus.Unavailable **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Unavailable);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /* 4 .VolumeStatus.Available **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Available);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /*5 .user delete volume, VolumeStatus.Deleting) **/
    volumeInMaster.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(volumeInMaster);

    //follower report Available also, but master not change the status
    volumeFromFlower.setVolumeStatus(VolumeStatus.Available);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    volumeFromFlower = changeToThirf(response);
    Assert.assertTrue(volumeInMaster.getVolumeStatus().equals(VolumeStatus.Deleting));
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(VolumeStatus.Deleting));

    /*6 .VolumeStatus.Deleting **/
    volumeMetadataList.clear();
    volumeMetadataList.add(volumeFromFlower);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /*7 .VolumeStatus.Deleted **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Deleted);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /*8 .VolumeStatus.Dead **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Dead);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeFromFlower.getVolumeStatus().equals(volumeInMaster.getVolumeStatus()));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

  }

  @Test
  public void testReportVolumeInfoOneFlowerReportVolumeStatusForRecycling() throws Exception {

    final long instanceId = 1L;
    long volumeId = 1;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeInMaster.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* 1. follower report deleting, the user user Recycling volume **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Deleting);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeInMaster.getVolumeStatus().equals(volumeFromFlower.getVolumeStatus()));

    /*2 .user Recycling volume, VolumeStatus.Recycling) **/
    volumeInMaster.setVolumeStatus(VolumeStatus.Recycling);
    volumeStore.saveVolume(volumeInMaster);

    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);
    VolumeMetadata volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getVolumeStatus().equals(VolumeStatus.Recycling));

    /*3 . when deleted, follower report VolumeStatus.Deleted **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Deleted);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getVolumeStatus().equals(VolumeStatus.Recycling));
    Assert.assertTrue(volumeInMaster.getVolumeStatus().equals(VolumeStatus.Recycling));

    /*4 .Recycling ok, follower report VolumeStatus.Available **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Available);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeInMaster.getVolumeStatus().equals(VolumeStatus.Available));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    /*5 .Recycling fail, follower report VolumeStatus.Available **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Deleted);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeInMaster.getVolumeStatus().equals(VolumeStatus.Deleted));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

    //Recycling
    volumeInMaster.setVolumeStatus(VolumeStatus.Recycling);
    volumeStore.saveVolume(volumeInMaster);

    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);
    volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getVolumeStatus().equals(VolumeStatus.Recycling));

    //fail
    volumeFromFlower.setVolumeStatus(VolumeStatus.Unavailable);
    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeInMaster = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeInMaster.getVolumeStatus().equals(VolumeStatus.Unavailable));
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());

  }

  @Test
  public void testReportVolumeInfoForChangeVolumeName() throws Exception {

    final long instanceId = 1L;
    long volumeId = 1;
    final String newVolumeName = "wen";
    final String newVolumeName2 = "wen2";
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* 1. master change the volume name  **/
    volumeInMaster.setName(newVolumeName);
    volumeStore.saveVolume(volumeInMaster);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);

    VolumeMetadata volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getName().equals(newVolumeName));

    /* change name again **/
    volumeInMaster.setName(newVolumeName2);
    volumeFromFlower.setName(newVolumeName);
    volumeStore.saveVolume(volumeInMaster);

    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);

    volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getName().equals(newVolumeName2));

  }

  @Ignore
  @Test
  public void testReportVolumeInfoForChangeVolumeLayout() throws Exception {

    final long instanceId = 1L;
    long volumeId = 1;
    String newVolumeLayout = "wen";
    final String newVolumeLayout2 = "wen2";
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeInMaster.setVolumeLayout(newVolumeLayout);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* 1. master change the VolumeLayout  **/
    volumeInMaster.setName(newVolumeLayout2);
    volumeStore.saveVolume(volumeInMaster);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);

    VolumeMetadata volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getName().equals(newVolumeLayout2));
  }

  @Test
  public void testReportVolumeInfoForChangeVolumeReadWriteType() throws Exception {

    final long instanceId = 1L;
    long volumeId = 1;
    final VolumeMetadata.ReadWriteType newReadWriteType = VolumeMetadata.ReadWriteType.READONLY;
    final VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* 1. master change the volume ReadWriteType **/
    volumeInMaster.setReadWrite(newReadWriteType);
    volumeStore.saveVolume(volumeInMaster);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);

    VolumeMetadata volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getReadWrite().equals(newReadWriteType));
  }

  @Test
  public void testReportVolumeInfoForChangeVolumeExtendSize_ForExtendVolume() throws Exception {
    final long instanceId = 1L;
    long volumeId = 1;
    final long extendSize = 1;

    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* 1. master change the volume extend size, begin the extend volume **/
    volumeInMaster.setExtendingSize(extendSize);
    volumeStore.saveVolume(volumeInMaster);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);

    VolumeMetadata volumeMasterReportToFlower = changeToThirf(response);
    Assert.assertTrue(volumeMasterReportToFlower.getExtendingSize() == extendSize);

    /* 2. the extend ok **/
    volumeFromFlower.setExtendingSize(0);
    volumeFromFlower.setVolumeSize(3 + extendSize);

    request = generateReportVolumeInfoRequest(instanceId, volumeMetadataList,
        volumeMetadataToDeleteList);

    response = informationCenter.reportVolumeInfo(request);
    VolumeMetadata volumeMetadataGet = volumeStore.getVolume(volumeId);

    Assert.assertTrue(volumeMetadataGet.getExtendingSize() == 0);
    Assert.assertTrue(volumeMetadataGet.getVolumeSize() == 3 + extendSize);
    Assert.assertTrue(response.getVolumeMetadatasChangeInMaster().isEmpty());
  }


  @Test
  public void testReportVolumeInfoForDeadVolumeToDelete() throws Exception {
    final long instanceId = 1L;
    long volumeId = 1;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    final List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataToDeleteList.add(volumeFromFlower);

    /* 1. set the Dead volume which to delete to master  **/
    Assert.assertTrue(volumeStore.getVolume(volumeId) != null);
    volumeFromFlower.setVolumeStatus(VolumeStatus.Dead);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
    Assert.assertTrue(volumeMetadata == null);

  }

  @Test
  public void testReportSegmentUnitsMetadataForChooseHaToReport() throws Exception {
    // report the first volume
    final long instanceMasterId = 1;
    VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    volumeStore.saveVolume(volumeMetadata);

    InstanceId instanceId = new InstanceId("1");
    when(appContext.getInstanceId()).thenReturn(instanceId);

    Set<Instance> instanceFlower = new HashSet<>();
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    Instance flowerInstance = new Instance(new InstanceId("2"), "test2", InstanceStatus.SUSPEND,
        new EndPoint("10.0.0.82", 8082));
    instanceFlower.add(flowerInstance);
    flowerInstanceMap.put(2L, flowerInstance);
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.SUSPEND))
        .thenReturn(instanceFlower);

    Set<Instance> instanceMasters = new HashSet<>();
    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceMasterId)),
        "test1", InstanceStatus.HEALTHY, new EndPoint("10.0.0.81", 8082));
    instanceMasters.add(instanceMaster);

    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(instanceMasters);

    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(
        volumeMetadata,
        SegmentUnitStatus.Secondary);
    //choose master to report
    ReportSegmentUnitsMetadataResponse response = informationCenter
        .reportSegmentUnitsMetadata(request);
    assertTrue(response.getWhichHaThisVolumeToReport().isEmpty());

    // now the volume store has the first volume, update volume
    volumeMetadata = generateVolumeMetadata(2, null, 6, 3, 2);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    volumeStore.saveVolume(volumeMetadata);

    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);

    response = informationCenter.reportSegmentUnitsMetadata(request);

    assertTrue(!response.getWhichHaThisVolumeToReport().isEmpty());

    // choose instance 2 to report
    for (Map.Entry<Long, Set<Long>> entry : response.getWhichHaThisVolumeToReport().entrySet()) {
      assertTrue(entry.getKey() == 2);
      Set<Long> volumeList = entry.getValue();

      assertTrue(volumeList.iterator().next() == 2);
    }
  }

  @Test
  public void testReportSegmentUnitsMetadataForChooseHaToReportMasterChange() throws Exception {
    // report the first volume
    final long instanceMasterId = 1;
    VolumeMetadata volumeMetadata = generateVolumeMetadata(1, null, 6, 3, 1);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    volumeStore.saveVolume(volumeMetadata);

    InstanceId instanceId = new InstanceId("1");
    when(appContext.getInstanceId()).thenReturn(instanceId);

    /* init instance master and follower **/
    Set<Instance> instanceFlower = new HashSet<>();
    Map<Long, Instance> followerInstanceMap = new HashMap<>();
    Instance followerInstance1 = new Instance(new InstanceId("2"), "test2", InstanceStatus.SUSPEND,
        new EndPoint("10.0.0.82", 8082));
    instanceFlower.add(followerInstance1);
    followerInstanceMap.put(2L, followerInstance1);
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.SUSPEND))
        .thenReturn(instanceFlower);

    Set<Instance> instanceMasters = new HashSet<>();
    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceMasterId)),
        "test1", InstanceStatus.HEALTHY, new EndPoint("10.0.0.81", 8082));
    instanceMasters.add(instanceMaster);
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(instanceMasters);

    //choose master to report
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(followerInstanceMap);

    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(
        volumeMetadata,
        SegmentUnitStatus.Secondary);
    ReportSegmentUnitsMetadataResponse response = informationCenter
        .reportSegmentUnitsMetadata(request);

    assertTrue(response.getWhichHaThisVolumeToReport().isEmpty());

    // now the volume store has the first volume, update volume
    volumeMetadata = generateVolumeMetadata(2, null, 6, 3, 2);
    volumeStore.saveVolume(volumeMetadata);

    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary);
    response = informationCenter.reportSegmentUnitsMetadata(request);

    assertTrue(!response.getWhichHaThisVolumeToReport().isEmpty());

    // choose instance 2 to report
    for (Map.Entry<Long, Set<Long>> entry : response.getWhichHaThisVolumeToReport().entrySet()) {
      assertTrue(entry.getKey() == 2);
      Set<Long> volumeList = entry.getValue();
      assertTrue(volumeList.iterator().next() == 2);
    }
  }

  @Test
  public void testReportSegmentUnitForExtendVolumeOk() throws Exception {
    VolumeMetadata volumeToExtend = generateVolumeMetadata(1, null, volumeSize, segmentSize, 1);
    volumeToExtend.setVolumeSource(CREATE_VOLUME);
    volumeToExtend.setVolumeStatus(VolumeStatus.Unavailable);
    volumeStore.saveVolume(volumeToExtend);
    volumeStore.saveVolumeForReport(volumeToExtend);

    //set the report HA instance
    InstanceId instanceIdMaster = new InstanceId("1");
    when(appContext.getInstanceId()).thenReturn(instanceIdMaster);

    Set<Instance> instanceFlower = new HashSet<>();
    instanceFlower.add(new Instance(new InstanceId("2"), "test2", InstanceStatus.SUSPEND,
        new EndPoint("10.0.0.82", 8082)));
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.SUSPEND))
        .thenReturn(instanceFlower);

    Set<Instance> instanceMaster = new HashSet<>();
    instanceMaster.add(new Instance(new InstanceId("1"), "test1", InstanceStatus.HEALTHY,
        new EndPoint("10.0.0.82", 8082)));
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(instanceMaster);

    //choose master to report
    when(dbVolumeStore.getVolume(anyLong())).thenReturn(volumeToExtend);

    /*1. report a segment unit, the segmentIndex is 0, the un extend SegmentUnits **/
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volumeToExtend,
        SegmentUnitStatus.Primary,
        1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    volumeSweeper.doWork();

    /* begin to extend the volume **/
    volumeToExtend.setExtendingSize(extendSize);
    volumeToExtend.setLastExtendedTime(new Date());
    volumeToExtend.setInAction(EXTENDING);
    volumeToExtend.incVersion();
    volumeStore.saveVolume(volumeToExtend);
    volumeStore.saveVolumeForReport(volumeToExtend);

    VolumeMetadata volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(!volumeInFlower.getSegmentTable().isEmpty());

    /*2. report a segment unit, the segmentIndex is 1, the extend SegmentUnits **/
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Primary,
        1, 1);
    informationCenter.reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();
    volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(volumeInFlower.getExtendSegmentTable().size() == 1);

    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 2, 1);
    informationCenter.reportSegmentUnitsMetadata(request);

    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 3, 1);
    informationCenter.reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();

    //check extend ok
    volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(volumeInFlower.getSegmentTable().size() == 2);

  }

  @Test
  public void testReportSegmentUnit_ForExtendVolumeFailed_CreateTimeOut() throws Exception {
    long volumeId = 1;
    VolumeMetadata volumeToExtend = generateVolumeMetadata(volumeId, null, volumeSize, segmentSize,
        1);
    volumeToExtend.setVolumeSource(CREATE_VOLUME);
    volumeToExtend.setVolumeStatus(VolumeStatus.Unavailable);
    volumeStore.saveVolume(volumeToExtend);
    volumeStore.saveVolumeForReport(volumeToExtend);

    //set the report HA instance
    InstanceId instanceId = new InstanceId("1");
    when(appContext.getInstanceId()).thenReturn(instanceId);

    Set<Instance> instanceFlower = new HashSet<>();
    instanceFlower.add(new Instance(new InstanceId("2"), "test2", InstanceStatus.SUSPEND,
        new EndPoint("10.0.0.82", 8082)));
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.SUSPEND))
        .thenReturn(instanceFlower);

    Set<Instance> instanceMaster = new HashSet<>();
    instanceMaster.add(new Instance(new InstanceId("1"), "test1", InstanceStatus.HEALTHY,
        new EndPoint("10.0.0.81", 8082)));
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(instanceMaster);

    //choose master to report
    when(dbVolumeStore.getVolume(anyLong())).thenReturn(volumeToExtend);

    /*1. report a segment unit, the segmentIndex is 0, the un extend SegmentUnits **/
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volumeToExtend,
        SegmentUnitStatus.Primary,
        1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    volumeSweeper.doWork();

    /* begin to extend the volume **/
    volumeToExtend.setExtendingSize(extendSize);
    volumeToExtend.setLastExtendedTime(new Date());
    volumeToExtend.setInAction(EXTENDING);
    volumeToExtend.incVersion();
    volumeStore.saveVolume(volumeToExtend);
    volumeStore.saveVolumeForReport(volumeToExtend);

    VolumeMetadata volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(!volumeInFlower.getSegmentTable().isEmpty());

    /* the data node create unit too later
     *  set the extend time out,set 3s
     * **/
    InfoCenterConstants.setVolumeBeCreatingTimeout(3);
    Thread.sleep(4000); //make extend volume time out

    /*2. report a segment unit, the segmentIndex is 1, the extend SegmentUnits,
     * but the extend volume time out
     *  **/
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Primary,
        1, 1);
    informationCenter.reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();
    volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(volumeInFlower.getExtendSegmentTable().isEmpty());

    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 2, 1);
    informationCenter.reportSegmentUnitsMetadata(request);

    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 3, 1);
    final ReportSegmentUnitsMetadataResponse response = informationCenter
        .reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();

    //check extend ok
    volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(volumeInFlower.getSegmentTable().size() == 1); //not extend ok

    //set exception to date node to delete the segment unit
    List<SegUnitConflictThrift> segUnitConflictThrifts = response.getConflicts();
    Assert.assertTrue(segUnitConflictThrifts.size() == 1); //

    SegUnitConflictThrift value = segUnitConflictThrifts.get(0);

    Assert.assertTrue(
        value.getCause().equals(SegmentUnitStatusConflictCauseThrift.VolumeExtendFailed));
    Assert.assertTrue(value.getSegIndex() == 1);
    Assert.assertTrue(value.getVolumeId() == volumeId);

  }

  @Test
  public void testReportSegmentUnit_ForExtendVolumeFailed_CreatingTimeOut() throws Exception {
    long volumeId = 1;
    VolumeMetadata volumeToExtend = generateVolumeMetadata(volumeId, null, volumeSize, segmentSize,
        1);
    volumeToExtend.setVolumeSource(CREATE_VOLUME);
    volumeToExtend.setVolumeStatus(VolumeStatus.Unavailable);
    volumeStore.saveVolume(volumeToExtend);
    volumeStore.saveVolumeForReport(volumeToExtend);

    //set the report HA instance
    InstanceId instanceId = new InstanceId("1");
    when(appContext.getInstanceId()).thenReturn(instanceId);

    Set<Instance> instanceFlower = new HashSet<>();
    instanceFlower.add(new Instance(new InstanceId("2"), "test2", InstanceStatus.SUSPEND,
        new EndPoint("10.0.0.82", 8082)));
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.SUSPEND))
        .thenReturn(instanceFlower);

    Set<Instance> instanceMaster = new HashSet<>();
    instanceMaster.add(new Instance(new InstanceId("1"), "test1", InstanceStatus.HEALTHY,
        new EndPoint("10.0.0.81", 8082)));
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(instanceMaster);

    //choose master to report
    when(dbVolumeStore.getVolume(anyLong())).thenReturn(volumeToExtend);

    /*1. report a segment unit, the segmentIndex is 0, the un extend SegmentUnits **/
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volumeToExtend,
        SegmentUnitStatus.Primary,
        1, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    volumeSweeper.doWork();

    /* begin to extend the volume **/
    volumeToExtend.setExtendingSize(extendSize * 2);
    volumeToExtend.setLastExtendedTime(new Date());
    volumeToExtend.setInAction(EXTENDING);
    volumeToExtend.incVersion();
    volumeStore.saveVolume(volumeToExtend);
    volumeStore.saveVolumeForReport(volumeToExtend);

    VolumeMetadata volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(!volumeInFlower.getSegmentTable().isEmpty());

    /*2. report a segment unit, the segmentIndex is 1, the extend SegmentUnits,
     * but the extend volume time out
     *  **/
    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Primary,
        1, 1);
    informationCenter.reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();
    volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(!volumeInFlower.getExtendSegmentTable().isEmpty());

    /* the data node create unit too later
     *  set the extend time out,set 3s
     * **/
    InfoCenterConstants.setVolumeBeCreatingTimeout(3);
    Thread.sleep(4000); //make extend volume time out

    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 2, 1);
    informationCenter.reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();

    request = generateSegUnitsMetadataRequest(volumeToExtend, SegmentUnitStatus.Secondary, 3, 1);
    final ReportSegmentUnitsMetadataResponse response = informationCenter
        .reportSegmentUnitsMetadata(request);
    volumeSweeper.doWork();

    //check extend ok
    volumeInFlower = volumeStore.getVolumeForReport(volumeToExtend.getVolumeId());
    Assert.assertTrue(volumeInFlower.getSegmentTable().size() == 1); //not extend ok

    //set exception to date node to delete the segment unit
    List<SegUnitConflictThrift> segUnitConflictThrifts = response.getConflicts();
    Assert.assertTrue(segUnitConflictThrifts.size() == 1); //

    SegUnitConflictThrift value = segUnitConflictThrifts.get(0);

    Assert.assertTrue(
        value.getCause().equals(SegmentUnitStatusConflictCauseThrift.VolumeExtendFailed));
    Assert.assertTrue(value.getSegIndex() == 1);
    Assert.assertTrue(value.getVolumeId() == volumeId);
    logger.warn("get the result :{}", value);

  }

  @Test
  public void testHaInstanceManagerTimeOut_reportSegmentUnitsMetadata() throws Exception {
    // report the first volume
    long volumeIdFlower = 1;
    final long volumeIdFlowerEquilibrium = 3;
    long volumeIdMaster = 2;
    final long instanceFlowerId = 1; //Equilibrium TO instance
    final long instanceMasterId = 2;
    final long instanceFlowerId2ForEquilibriumForm = 3; //Form
    VolumeMetadata volumeMetadataInFlower = generateVolumeMetadata(volumeIdFlower, null, 6, 3, 1);
    VolumeMetadata volumeMetadataInMaster = generateVolumeMetadata(volumeIdMaster, null, 6, 3, 1);

    //save to db
    volumeStore.saveVolume(volumeMetadataInFlower);

    volumeStore.saveVolume(volumeMetadataInMaster);

    /* start the HaInstanceCheckSweeperFactory to check HA instance **/
    PeriodicWorkExecutorImpl periodicWorkExecutor = new PeriodicWorkExecutorImpl(
        haInstanceCheckSweeperExecutionOptionsReader(), haInstanceCheckSweeperFactory(),
        "HaInstanceCheck-Sweeper");
    periodicWorkExecutor.start();
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    /* init instance master and follower **/
    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(instanceFlowerId, new Instance(new InstanceId(String.valueOf(instanceFlowerId)),
            "test2", InstanceStatus.SUSPEND, new EndPoint("10.0.0.82", 8082)));

    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceMasterId)),
        "test1", InstanceStatus.HEALTHY, new EndPoint("10.0.0.81", 8082));
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    /* set the follower to report **/
    final InstanceId instanceIdMaster = new InstanceId(String.valueOf(instanceMasterId));
    instanceIncludeVolumeInfoManger.saveVolumeInfo(instanceMasterId, volumeIdMaster);

    /* set the Equilibrium value  ***/
    Map<Long, Long> volumesReportToNewInstance = new ConcurrentHashMap<>();
    Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibrium = new ConcurrentHashMap<>();

    volumesReportToNewInstance.put(volumeIdFlowerEquilibrium, instanceFlowerId);
    volumeReportToInstanceEquilibrium
        .put(instanceFlowerId2ForEquilibriumForm, volumesReportToNewInstance);
    instanceVolumeInEquilibriumManger
        .setVolumeReportToInstanceEquilibrium(volumeReportToInstanceEquilibrium);

    Map<Long, Long> instanceIds = new ConcurrentHashMap<>();
    Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibriumBuildWithVolumeId = new HashMap<>();

    instanceIds.put(instanceFlowerId2ForEquilibriumForm, instanceFlowerId);
    volumeReportToInstanceEquilibriumBuildWithVolumeId.put(volumeIdFlowerEquilibrium, instanceIds);
    instanceVolumeInEquilibriumManger.setVolumeReportToInstanceEquilibriumBuildWithVolumeId(
        volumeReportToInstanceEquilibriumBuildWithVolumeId);

    //check
    Assert.assertTrue(
        !instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibrium().isEmpty());
    Assert.assertTrue(
        !instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId()
            .isEmpty());

    //set master id
    when(appContext.getInstanceId()).thenReturn(instanceIdMaster);

    /* the unit report, the volume distribute to Flower **/
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(
        volumeMetadataInFlower,
        SegmentUnitStatus.Secondary);
    informationCenter.reportSegmentUnitsMetadata(request);

    //check, choose the follower to report
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    InstanceToVolumeInfo instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo.getLastReportedTime() > 0);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdFlower));
    long reportTime = 0;
    reportTime = instanceToVolumeInfo.getLastReportedTime();

    /* because the follower is down, the datanode report to follower, but can not
     * report ok, so report to master again
     * the master choose HA
     **/

    Thread.sleep(6000);
    //report again, the report time still,  the follower down, so not report to master, the 
    // lastTime not change
    informationCenter.reportSegmentUnitsMetadata(request);
    instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap();
    instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo.getLastReportedTime() == reportTime);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdFlower));

    Thread.sleep(6000);
    //check,  find the follower down, to remove
    instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap();
    instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo == null);

    //check, find the follower down, so move the instance in Equilibrium table
    Assert.assertTrue(
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibrium().isEmpty());
    Assert.assertTrue(
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId()
            .isEmpty());
    logger.warn("get the ---- :{}",
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibrium());
    logger.warn("get the ---- :{}",
        instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibriumBuildWithVolumeId());
  }

  @Test
  public void testHaInstanceManagerTimeOut_ReportVolumeInfo() throws Exception {
    // report the first volume
    long volumeIdFlower = 1;
    final long instanceFlowerId = 1;
    final long instanceMasterId = 2;
    VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeIdFlower, null, 6, 3, 1);
    volumeStore.saveVolume(volumeInMaster);

    VolumeMetadata volumeFromFlower = new VolumeMetadata();
    volumeFromFlower.deepCopy(volumeInMaster);

    final List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    final List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    /* start the HaInstanceCheckSweeperFactory to check HA instance **/
    PeriodicWorkExecutorImpl haInstanceCheckSweeperExecutor = new PeriodicWorkExecutorImpl(
        haInstanceCheckSweeperExecutionOptionsReader(), haInstanceCheckSweeperFactory(),
        "HaInstanceCheck-Sweeper");
    haInstanceCheckSweeperExecutor.start();
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    /* init instance master and follower **/
    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(instanceFlowerId, new Instance(new InstanceId(String.valueOf(instanceFlowerId)),
            "test2", InstanceStatus.SUSPEND, new EndPoint("10.0.0.82", 8082)));

    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceMasterId)),
        "test1", InstanceStatus.HEALTHY, new EndPoint("10.0.0.81", 8082));
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    /* 1. the follower report volume  **/
    volumeFromFlower.setVolumeStatus(VolumeStatus.Available);
    ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceFlowerId,
        volumeMetadataList,
        volumeMetadataToDeleteList);

    informationCenter.reportVolumeInfo(request);

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeIdFlower);
    Assert.assertTrue(volumeMetadata.getVolumeStatus().equals(VolumeStatus.Available));

    //check, get the last report time
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    InstanceToVolumeInfo instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo.getLastReportedTime() > 0);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdFlower));
    long reportTime = 0;
    reportTime = instanceToVolumeInfo.getLastReportedTime();

    /*2. report again, the last report time change  **/
    informationCenter.reportVolumeInfo(request);

    //check, get the last report time
    instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap();
    instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo.getLastReportedTime() > reportTime);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdFlower));
    reportTime = instanceToVolumeInfo.getLastReportedTime();

    /* because the follower is down, the follower can not report to master **/

    Thread.sleep(6000);
    //report again, the report time still,  the follower down, so not report to master, the 
    // lastTime not change
    instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap();
    instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo.getLastReportedTime() == reportTime);
    Assert.assertTrue(instanceToVolumeInfo.containsValue(volumeIdFlower));

    Thread.sleep(6000);
    //check, time out, find the follower down, to remove
    instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap();
    instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceFlowerId);
    Assert.assertTrue(instanceToVolumeInfo == null);
  }

  @Test
  public void testInstanceIncludeVolumeInfoManger_InstanceFromDb_TimeOut() throws Exception {

    /* instance start, load all instance volumes info form db ***/
    long instanceId = 1;
    long volumeId = 11111L;

    InstanceVolumesInformation instanceVolumesInformation = new InstanceVolumesInformation();
    Set<Long> testSet = new HashSet<>();
    testSet.add(volumeId);
    instanceVolumesInformation.setInstanceId(instanceId);

    /* there is no value in db ****/
    //master start
    instanceIncludeVolumeInfoManger.initVolumeInfo();
    Assert.assertTrue(instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap().isEmpty());

    //save value
    instanceVolumesInformation.setVolumeIds(testSet);
    instanceVolumesInformationStore.saveInstanceVolumesInformationToDb(instanceVolumesInformation);

    //master start
    instanceIncludeVolumeInfoManger.initVolumeInfo();

    //check, have instance in table
    Assert.assertTrue(!instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap().isEmpty());

    /* start the HaInstanceCheckSweeperFactory to check HA instance **/
    PeriodicWorkExecutorImpl haInstanceCheckSweeperExecutor = new PeriodicWorkExecutorImpl(
        haInstanceCheckSweeperExecutionOptionsReader(), haInstanceCheckSweeperFactory(),
        "HaInstanceCheck-Sweeper");
    haInstanceCheckSweeperExecutor.start();
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    /* sleep 10s, the thread check current instance is down or not ***/
    Thread.sleep(6 * 1000);
    Assert.assertTrue(!instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap().isEmpty());
    Thread.sleep(6 * 1000);

    /* the instance 1 not report info to master, so i think the follower down ***/
    Assert.assertTrue(instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap().isEmpty());
  }

  @Test
  public void testCreateVolumeForWait() throws Exception {
    long volumeCreateWaitId = 3;
    VolumeMetadata volumeCreateWait = generateVolumeMetadata(volumeCreateWaitId, null, volumeSize,
        segmentSize, 1);

    //save
    volumeCreateWait.setVolumeSource(CREATE_VOLUME);
    volumeCreateWait.setVolumeStatus(VolumeStatus.ToBeCreated);
    volumeCreateWait.setVolumeCreatedTime(new Date(System.currentTimeMillis()));
    volumeStore.saveVolume(volumeCreateWait);
    volumeStore.saveVolumeForReport(volumeCreateWait);

    //set the ToBeCreatedTime out,
    InfoCenterConstants.setVolumeToBeCreatedTimeout(6);
    List<Long> volumeIds = new ArrayList<>();
    volumeIds.add(volumeCreateWaitId);
    when(volumeJobStore.getAllCreateOrExtendVolumeId()).thenReturn(volumeIds);

    //all ok
    volumeSweeper.doWork();

    /* 1. begin to update status **/
    Thread.sleep(4000);
    volumeSweeper.setUpdateVolumesStatusTrigger(60);
    volumeSweeper.doWork();

    /*2 . check each volume ***/
    //the create time is same volumeCreatTimeOutId, but not create unit, so not time out
    VolumeStatus volumeStatus = volumeStore.getVolume(volumeCreateWaitId).getVolumeStatus();
    Assert.assertTrue(volumeStatus == VolumeStatus.ToBeCreated);

    /*3.volumeCreateWaitId begin to create **/
    volumeIds.clear();
    Thread.sleep(4000);
    volumeSweeper.setUpdateVolumesStatusTrigger(60);
    volumeSweeper.doWork();

    //still not time out
    volumeStatus = volumeStore.getVolume(volumeCreateWaitId).getVolumeStatus();
    Assert.assertTrue(volumeStatus == VolumeStatus.ToBeCreated);

    /* 4. the volumeCreateWaitId time out ****/
    Thread.sleep(3000);
    volumeSweeper.setUpdateVolumesStatusTrigger(60);
    volumeSweeper.doWork();

    // time out
    volumeStatus = volumeStore.getVolume(volumeCreateWaitId).getVolumeStatus();
    Assert.assertTrue(volumeStatus == VolumeStatus.Dead);

  }


  @After
  public void clear() throws SQLException, IOException {
    /* clean the info ***/
    List<InstanceVolumesInformation> instanceVolumesInformationList =
        instanceVolumesInformationStore
            .reloadAllInstanceVolumesInformationFromDb();
    for (InstanceVolumesInformation instanceVolumesInformation : instanceVolumesInformationList) {
      instanceVolumesInformationStore
          .deleteInstanceVolumesInformationFromDb(instanceVolumesInformation.getInstanceId());
    }
  }

  @Test
  public void justTestReportVolumeInfoForTestTime() throws Exception {
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);

    long instanceIdNumber = 50;
    long eachInstanceVolumeNumber = 100;
    List<ReportVolumeInfoRequest> requests = new ArrayList<>();

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    List<VolumeMetadata> volumeMetadataToDeleteList = new ArrayList<>();
    for (int i = 0; i < instanceIdNumber; i++) {
      final long instanceId = i + 1;

      volumeMetadataList.clear();
      volumeMetadataToDeleteList.clear();

      for (int j = 0; j < eachInstanceVolumeNumber; j++) {
        long volumeId = RequestIdBuilder.get();
        VolumeMetadata volumeInMaster = generateVolumeMetadata(volumeId, null, 3, 3, 1);
        volumeStore.saveVolume(volumeInMaster);

        VolumeMetadata volumeFromFlower = new VolumeMetadata();
        volumeFromFlower.deepCopy(volumeInMaster);
        volumeMetadataList.add(volumeFromFlower);
      }

      ReportVolumeInfoRequest request = generateReportVolumeInfoRequest(instanceId,
          volumeMetadataList,
          volumeMetadataToDeleteList);
      requests.add(request);
    }

    long beginTime = System.currentTimeMillis();
    for (ReportVolumeInfoRequest request : requests) {
      ReportVolumeInfoResponse response = informationCenter.reportVolumeInfo(request);
    }

    logger
        .warn("get the time is for reportVolumeInfo :{} ", System.currentTimeMillis() - beginTime);

    /* clean the info ***/
    List<InstanceVolumesInformation> instanceVolumesInformationList =
        instanceVolumesInformationStore
            .reloadAllInstanceVolumesInformationFromDb();
    for (InstanceVolumesInformation instanceVolumesInformation : instanceVolumesInformationList) {
      instanceVolumesInformationStore
          .deleteInstanceVolumesInformationFromDb(instanceVolumesInformation.getInstanceId());
    }
  }


  @Test //just for test the VolumeMetadata, for @JsonIgnore
  public void testJson() throws JsonProcessingException, TException {
    long volumeId = 1;
    VolumeMetadata volumeMetadata = generateVolumeMetadata(volumeId, null, volumeSize, segmentSize,
        1);
    volumeMetadata.setVolumeSource(CREATE_VOLUME);
    volumeMetadata.setVolumeStatus(VolumeStatus.Unavailable);
    volumeStore.saveVolume(volumeMetadata);
    volumeStore.saveVolumeForReport(volumeMetadata);

    //set the report HA instance
    InstanceId instanceIdMaster = new InstanceId("1");
    when(appContext.getInstanceId()).thenReturn(instanceIdMaster);

    Set<Instance> instanceFlowerList = new HashSet<>();
    Instance instanceFlower1 = new Instance(new InstanceId("2"),
        "test2", InstanceStatus.SUSPEND, new EndPoint("10.0.0.80", 8082));
    instanceFlowerList.add(instanceFlower1);
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.SUSPEND))
        .thenReturn(instanceFlowerList);

    Set<Instance> instanceMasterList = new HashSet<>();
    Instance instanceMaster = new Instance(instanceIdMaster,
        "test1", InstanceStatus.HEALTHY, new EndPoint("10.0.0.81", 8082));

    instanceMasterList.add(instanceMaster);
    when(instanceStore.getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY))
        .thenReturn(instanceMasterList);

    //choose master to report
    when(dbVolumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    /*1. report a segment unit, the segmentIndex is 0, the un extend SegmentUnits **/
    ReportSegmentUnitsMetadataRequest request = generateSegUnitsMetadataRequest(volumeMetadata,
        SegmentUnitStatus.Primary,
        1, 0);

    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary, 2, 0);
    informationCenter.reportSegmentUnitsMetadata(request);
    request = generateSegUnitsMetadataRequest(volumeMetadata, SegmentUnitStatus.Secondary, 3, 0);
    informationCenter.reportSegmentUnitsMetadata(request);

    ObjectMapper mapper = new ObjectMapper();
    String volumeString = null;

    volumeMetadata = volumeStore.getVolumeForReport(volumeId);
    try {
      volumeString = mapper.writeValueAsString(volumeMetadata);
    } catch (JsonProcessingException e) {
      logger.error("failed to build volumemetadata string ", e);
    }
    logger.warn("get the string :{}", volumeString);
  }


  public VolumeMetadata changeToThirf(ReportVolumeInfoResponse response) {
    VolumeMetadata volumeMetadata = RequestResponseHelper
        .buildVolumeFrom(response.getVolumeMetadatasChangeInMaster().get(0));
    return volumeMetadata;
  }

  public ExecutionOptionsReader haInstanceCheckSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, 1000, null);
  }


  public HaInstanceCheckSweeperFactory haInstanceCheckSweeperFactory() throws Exception {
    HaInstanceCheckSweeperFactory haInstanceCheckSweeperFactory =
        new HaInstanceCheckSweeperFactory();
    haInstanceCheckSweeperFactory.setVolumeInformationManger(volumeInformationManger);
    haInstanceCheckSweeperFactory.setInstanceTimeOutCheck(10);
    haInstanceCheckSweeperFactory.setAppContext(appContext);
    haInstanceCheckSweeperFactory
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    haInstanceCheckSweeperFactory
        .setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    return haInstanceCheckSweeperFactory;
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
    volumeMetadata.setLastExtendedTime(new Date(0));
    return volumeMetadata;
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


  @Test
  public void testList() {
    long volumeId = 1;
    VolumeMetadata volumeFromFlower = generateVolumeMetadata(volumeId, null, 3, 3, 1);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    volumeMetadataList.add(volumeFromFlower);

    Assert.assertTrue(volumeMetadataList.get(0).getVolumeStatus().equals(VolumeStatus.Creating));

    volumeFromFlower.setVolumeStatus(VolumeStatus.Available);

    Assert.assertTrue(volumeMetadataList.get(0).getVolumeStatus().equals(VolumeStatus.Available));

    volumeFromFlower.setVolumeStatus(VolumeStatus.Unavailable);

    Assert.assertTrue(volumeMetadataList.get(0).getVolumeStatus().equals(VolumeStatus.Unavailable));
  }

}