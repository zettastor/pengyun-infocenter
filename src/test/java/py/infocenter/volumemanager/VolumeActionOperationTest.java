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

import java.util.Date;
import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.icshare.DomainStore;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.HaInstanceMangerWithZk;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.service.ExceptionForOperation;
import py.infocenter.service.InformationCenterImpl;
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
import py.infocenter.store.control.OperationStore;
import py.infocenter.test.utils.TestBeans;
import py.infocenter.worker.StorageStoreSweeper;
import py.infocenter.worker.VolumeActionSweeper;
import py.infocenter.worker.VolumeSweeper;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.test.TestBase;
import py.thrift.icshare.DeleteVolumeRequest;
import py.thrift.icshare.RecycleVolumeRequest;
import py.thrift.infocenter.service.ExtendVolumeRequest;
import py.thrift.share.AccessDeniedExceptionThrift;
import py.thrift.share.AccountNotFoundExceptionThrift;
import py.thrift.share.BadLicenseTokenExceptionThrift;
import py.thrift.share.DomainIsDeletingExceptionThrift;
import py.thrift.share.DomainNotExistedExceptionThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.LicenseExceptionThrift;
import py.thrift.share.NotEnoughLicenseTokenExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.PermissionNotGrantExceptionThrift;
import py.thrift.share.RootVolumeBeingDeletedExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.StoragePoolIsDeletingExceptionThrift;
import py.thrift.share.StoragePoolNotExistInDoaminExceptionThrift;
import py.thrift.share.StoragePoolNotExistedExceptionThrift;
import py.thrift.share.UmountDriverRequestThrift;
import py.thrift.share.UselessLicenseExceptionThrift;
import py.thrift.share.VolumeCyclingExceptionThrift;
import py.thrift.share.VolumeDeletingExceptionThrift;
import py.thrift.share.VolumeExistingExceptionThrift;
import py.thrift.share.VolumeInExtendingExceptionThrift;
import py.thrift.share.VolumeInMoveOnlineDoNotHaveOperationExceptionThrift;
import py.thrift.share.VolumeIsBeginMovedExceptionThrift;
import py.thrift.share.VolumeIsCloningExceptionThrift;
import py.thrift.share.VolumeIsCopingExceptionThrift;
import py.thrift.share.VolumeIsMovingExceptionThrift;
import py.thrift.share.VolumeNameExistedExceptionThrift;
import py.thrift.share.VolumeNotAvailableExceptionThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.thrift.share.VolumeSizeNotMultipleOfSegmentSizeThrift;
import py.thrift.share.VolumeWasRollbackingExceptionThrift;
import py.volume.CacheType;
import py.volume.OperationFunctionType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class VolumeActionOperationTest extends TestBase {

  long deadVolumeToRemove = 15552000;
  VolumeActionSweeper volumeActionSweeper = null;
  @Autowired
  private SessionFactory sessionFactory;
  private MyInformationCenterImpl informationCenter;
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
  private StorageStoreSweeper storageStoreSweeper;

  private VolumeInformationManger volumeInformationManger;

  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;

  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  private VolumeSweeper volumeSweeper;

  private HaInstanceManger haInstanceManger;

  private InstanceVolumesInformationStore instanceVolumesInformationStore;
  private ExceptionForOperation exceptionForOperation;

  @Mock
  private OperationStore operationStore;

  @Mock
  private PySecurityManager securityManager;
  private VolumeMetadata volume;


  @Before
  public void setup() {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);
    instanceVolumesInformationStore = (InstanceVolumesInformationStore) ctx
        .getBean("instanceVolumesInformationStoreTest");

    informationCenter = new MyInformationCenterImpl();
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
    informationCenter.setSegmentSize(segmentSize);
    informationCenter.setSecurityManager(securityManager);

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
    informationCenter.setDriverContainerSelectionStrategy(balancedDriverContainerSelectionStrategy);

    //init InstanceIncludeVolumeInfoManger
    instanceIncludeVolumeInfoManger = new InstanceIncludeVolumeInfoManger();
    instanceIncludeVolumeInfoManger
        .setInstanceVolumesInformationStore(instanceVolumesInformationStore);

    //init HaInstanceMangerWithZK
    haInstanceManger = new HaInstanceMangerWithZk(instanceStore);

    //init VolumeInformationManger
    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);
    volumeInformationManger.setDriverStore(driverStore);
    volumeInformationManger.setAppContext(appContext);

    //init InstanceVolumeInEquilibriumManger
    instanceVolumeInEquilibriumManger = new InstanceVolumeInEquilibriumManger();

    informationCenter.setVolumeInformationManger(volumeInformationManger);

    //init InstanceVolumeInEquilibriumManger
    instanceVolumeInEquilibriumManger = new InstanceVolumeInEquilibriumManger();
    instanceVolumeInEquilibriumManger
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    informationCenter.setVolumeInformationManger(volumeInformationManger);
    informationCenter.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    informationCenter.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    //exceptionForOperation
    exceptionForOperation = new ExceptionForOperation();
    informationCenter.setExceptionForOperation(exceptionForOperation);

    //operation
    informationCenter.setOperationStore(operationStore);

    //
    long volumeId = 1;
    volume = generateVolumeMetadata(volumeId, null, 3, 3, 1);
    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);

    //

  }

  @Test
  public void testVolumeAction_extendVolume() {

    ExtendVolumeRequest extendVolumeRequest = generateExtendVolumeRequest();

    int exceptionCount = 0;
    for (int i = 1; i < 15; i++) {
      if (i == 5) {
        //FIXING is not
        continue;
      }
      VolumeInAction currentAction = VolumeInAction.findByValue(i);
      setVolumeAction(currentAction);
      try {
        informationCenter.extendVolume(extendVolumeRequest);
      } catch (VolumeIsMovingExceptionThrift volumeIsMovingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsCopingExceptionThrift volumeIsCopingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (AccessDeniedExceptionThrift accessDeniedExceptionThrift) {
        logger.error("", accessDeniedExceptionThrift);
      } catch (VolumeIsBeginMovedExceptionThrift volumeIsBeginMovedExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeExistingExceptionThrift volumeExistingExceptionThrift) {
        logger.error("", volumeExistingExceptionThrift);
      } catch (StoragePoolIsDeletingExceptionThrift storagePoolIsDeletingExceptionThrift) {
        logger.error("", storagePoolIsDeletingExceptionThrift);
      } catch (VolumeIsCloningExceptionThrift volumeIsCloningExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (StoragePoolNotExistedExceptionThrift storagePoolNotExistedExceptionThrift) {
        logger.error("", storagePoolNotExistedExceptionThrift);
      } catch (VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (NotEnoughSpaceExceptionThrift notEnoughSpaceExceptionThrift) {
        logger.error("", notEnoughSpaceExceptionThrift);
      } catch (ServiceHavingBeenShutdownThrift serviceHavingBeenShutdownThrift) {
        logger.error("", serviceHavingBeenShutdownThrift);
      } catch (VolumeWasRollbackingExceptionThrift volumeWasRollbackingExceptionThrift) {
        logger.error("", volumeWasRollbackingExceptionThrift);
      } catch (VolumeNotFoundExceptionThrift volumeNotFoundExceptionThrift) {
        logger.error("", volumeNotFoundExceptionThrift);
      } catch (VolumeNameExistedExceptionThrift volumeNameExistedExceptionThrift) {
        logger.error("", volumeNameExistedExceptionThrift);
      } catch (RootVolumeBeingDeletedExceptionThrift rootVolumeBeingDeletedExceptionThrift) {
        logger.error("", rootVolumeBeingDeletedExceptionThrift);
      } catch (VolumeSizeNotMultipleOfSegmentSizeThrift volumeSizeNotMultipleOfSegmentSizeThrift) {
        logger.error("", volumeSizeNotMultipleOfSegmentSizeThrift);
      } catch (VolumeCyclingExceptionThrift volumeCyclingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (DomainIsDeletingExceptionThrift domainIsDeletingExceptionThrift) {
        logger.error("", domainIsDeletingExceptionThrift);
      } catch (InvalidInputExceptionThrift invalidInputExceptionThrift) {
        logger.error("", invalidInputExceptionThrift);
      } catch (VolumeDeletingExceptionThrift volumeDeletingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (DomainNotExistedExceptionThrift domainNotExistedExceptionThrift) {
        logger.error("", domainNotExistedExceptionThrift);
      } catch (PermissionNotGrantExceptionThrift permissionNotGrantExceptionThrift) {
        logger.error("", permissionNotGrantExceptionThrift);
      } catch (VolumeInMoveOnlineDoNotHaveOperationExceptionThrift
          volumeInMoveOnlineDoNotHaveOperationExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (StoragePoolNotExistInDoaminExceptionThrift
          storagePoolNotExistInDoaminExceptionThrift) {
        logger.error("", storagePoolNotExistInDoaminExceptionThrift);
      } catch (AccountNotFoundExceptionThrift accountNotFoundExceptionThrift) {
        logger.error("", accountNotFoundExceptionThrift);
      } catch (ServiceIsNotAvailableThrift serviceIsNotAvailableThrift) {
        logger.error("", serviceIsNotAvailableThrift);
      } catch (TException e) {
        logger.error("", e);
      }
    }

    logger.warn("get the exceptionCount is :{}", exceptionCount);
    Assert.assertTrue(exceptionCount == 13);
  }

  @Test
  public void testVolumeAction_extendVolume_Two() {
    int exceptionCount = 0;
    int goodCount = 0;
    for (int i = 1; i < 15; i++) {
      if (i == 5) {
        //FIXING is not
        continue;
      }
      VolumeInAction currentAction = VolumeInAction.findByValue(i);
      setVolumeAction(currentAction);

      try {
        informationCenter.checkExceptionForOperation(volume, OperationFunctionType.extendVolume);
      } catch (VolumeIsCopingExceptionThrift volumeIsCopingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsCloningExceptionThrift volumeIsCloningExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsBeginMovedExceptionThrift volumeIsBeginMovedExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInMoveOnlineDoNotHaveOperationExceptionThrift
          volumeInMoveOnlineDoNotHaveOperationExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsMovingExceptionThrift volumeIsMovingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeCyclingExceptionThrift volumeCyclingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeDeletingExceptionThrift volumeDeletingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift) {
        exceptionCount++;
        continue;
      }

      goodCount++;
      logger.warn("when volume in :{}, the :{} can do it", currentAction,
          OperationFunctionType.extendVolume.toString());
    }

    logger.warn("get the exceptionCount is :{}", exceptionCount);
    Assert.assertTrue(exceptionCount == 4);

    /*
     *  NULL
     */
    Assert.assertTrue(goodCount == 9);
  }

  @Test
  public void testVolumeAction_deleteVolume() {
    int exceptionCount = 0;
    int goodCount = 0;
    for (int i = 1; i < 15; i++) {
      if (i == 5) {
        //FIXING is not
        continue;
      }
      VolumeInAction currentAction = VolumeInAction.findByValue(i);
      setVolumeAction(currentAction);

      try {
        informationCenter.checkExceptionForOperation(volume, OperationFunctionType.deleteVolume);
      } catch (VolumeIsCopingExceptionThrift volumeIsCopingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsCloningExceptionThrift volumeIsCloningExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsBeginMovedExceptionThrift volumeIsBeginMovedExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInMoveOnlineDoNotHaveOperationExceptionThrift
          volumeInMoveOnlineDoNotHaveOperationExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsMovingExceptionThrift volumeIsMovingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeCyclingExceptionThrift volumeCyclingExceptionThrift) {
        logger.error("", volumeCyclingExceptionThrift);
      } catch (VolumeDeletingExceptionThrift volumeDeletingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift) {
        exceptionCount++;
        continue;
      }

      goodCount++;
      logger.warn("when volume in :{}, the :{} can do it", currentAction,
          OperationFunctionType.deleteVolume.toString());
    }

    logger.warn("get the exceptionCount is :{}", exceptionCount);
    Assert.assertTrue(exceptionCount == 2);


    Assert.assertTrue(goodCount == 11);
  }

  @Test
  public void testVolumeAction_recycleVolume() {
    int exceptionCount = 0;
    int goodCount = 0;
    for (int i = 1; i < 15; i++) {
      if (i == 5) {
        //FIXING is not
        continue;
      }
      VolumeInAction currentAction = VolumeInAction.findByValue(i);
      setVolumeAction(currentAction);

      try {
        informationCenter.checkExceptionForOperation(volume, OperationFunctionType.recycleVolume);
      } catch (VolumeIsCopingExceptionThrift volumeIsCopingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsCloningExceptionThrift volumeIsCloningExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsBeginMovedExceptionThrift volumeIsBeginMovedExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInMoveOnlineDoNotHaveOperationExceptionThrift
          volumeInMoveOnlineDoNotHaveOperationExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsMovingExceptionThrift volumeIsMovingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeCyclingExceptionThrift volumeCyclingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeDeletingExceptionThrift volumeDeletingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift) {
        exceptionCount++;
        continue;
      }

      goodCount++;
      logger.warn("when volume in :{}, the :{} can do it", currentAction,
          OperationFunctionType.recycleVolume.toString());
    }

    logger.warn("get the exceptionCount is :{}", exceptionCount);
    Assert.assertTrue(exceptionCount == 3);

    /*
     * DELETING NULL
     */
    Assert.assertTrue(goodCount == 10);
  }

  @Test
  public void testVolumeAction_launchDriver() {
    int exceptionCount = 0;
    int goodCount = 0;
    for (int i = 1; i < 15; i++) {
      if (i == 5) {
        //FIXING is not
        continue;
      }
      VolumeInAction currentAction = VolumeInAction.findByValue(i);
      setVolumeAction(currentAction);

      try {
        informationCenter.checkExceptionForOperation(volume, OperationFunctionType.launchDriver);
      } catch (VolumeIsCopingExceptionThrift volumeIsCopingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsCloningExceptionThrift volumeIsCloningExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsBeginMovedExceptionThrift volumeIsBeginMovedExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInMoveOnlineDoNotHaveOperationExceptionThrift
          volumeInMoveOnlineDoNotHaveOperationExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsMovingExceptionThrift volumeIsMovingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeCyclingExceptionThrift volumeCyclingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeDeletingExceptionThrift volumeDeletingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift) {
        exceptionCount++;
        continue;
      }

      goodCount++;
      logger.warn("when volume in :{}, the :{} can do it", currentAction,
          OperationFunctionType.launchDriver.toString());
    }

    logger.warn("get the exceptionCount is :{}, good count :{}", exceptionCount, goodCount);
    Assert.assertTrue(exceptionCount == 4);


    Assert.assertTrue(goodCount == 9);
  }

  @Test
  public void testVolumeAction_umountDriver() {
    int exceptionCount = 0;
    int goodCount = 0;
    for (int i = 1; i < 15; i++) {
      if (i == 5) {
        //FIXING is not
        continue;
      }
      VolumeInAction currentAction = VolumeInAction.findByValue(i);
      setVolumeAction(currentAction);

      try {
        informationCenter.checkExceptionForOperation(volume, OperationFunctionType.umountDriver);
      } catch (VolumeIsCopingExceptionThrift volumeIsCopingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsCloningExceptionThrift volumeIsCloningExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsBeginMovedExceptionThrift volumeIsBeginMovedExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInMoveOnlineDoNotHaveOperationExceptionThrift
          volumeInMoveOnlineDoNotHaveOperationExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeInExtendingExceptionThrift volumeInExtendingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeIsMovingExceptionThrift volumeIsMovingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeCyclingExceptionThrift volumeCyclingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeDeletingExceptionThrift volumeDeletingExceptionThrift) {
        exceptionCount++;
        continue;
      } catch (VolumeNotAvailableExceptionThrift volumeNotAvailableExceptionThrift) {
        exceptionCount++;
        continue;
      }

      goodCount++;
      logger.warn("when volume in :{}, the :{} can do it", currentAction,
          OperationFunctionType.umountDriver.toString());
    }

    logger.warn("get the exceptionCount is :{}, good count :{}", exceptionCount, goodCount);
    Assert.assertTrue(exceptionCount == 0);

    Assert.assertTrue(goodCount == 13);
  }


  public void setVolumeAction(VolumeInAction volumeAction) {
    volume.setInAction(volumeAction);
    volumeStore.saveVolumeForReport(volume);
    volumeStore.saveVolume(volume);
  }

  @After
  public void clean() {
    volume.setInAction(VolumeInAction.NULL);
  }


  public ExtendVolumeRequest generateExtendVolumeRequest() {
    ExtendVolumeRequest extendVolumeRequest = new ExtendVolumeRequest();
    extendVolumeRequest.setRequestId(1);
    extendVolumeRequest.setVolumeId(1);
    extendVolumeRequest.setExtendSize(segmentSize);
    return extendVolumeRequest;
  }


  public DeleteVolumeRequest generateDeleteVolumeRequest() {
    DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest();
    deleteVolumeRequest.setRequestId(1);
    deleteVolumeRequest.setVolumeId(1);
    return deleteVolumeRequest;
  }


  public RecycleVolumeRequest generateRecycleVolumeRequest() {
    RecycleVolumeRequest recycleVolumeRequest = new RecycleVolumeRequest();
    recycleVolumeRequest.setRequestId(1);
    recycleVolumeRequest.setVolumeId(1);
    return recycleVolumeRequest;
  }


  public LaunchDriverRequestThrift generateLaunchDriverRequestThrift() {
    LaunchDriverRequestThrift launchDriverRequestThrift = new LaunchDriverRequestThrift();
    launchDriverRequestThrift.setRequestId(1);
    launchDriverRequestThrift.setVolumeId(1);
    return launchDriverRequestThrift;
  }


  public UmountDriverRequestThrift generateUmountDriverRequestThrift() {
    UmountDriverRequestThrift umountDriverRequestThrift = new UmountDriverRequestThrift();
    umountDriverRequestThrift.setRequestId(1);
    umountDriverRequestThrift.setVolumeId(1);
    return umountDriverRequestThrift;
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


  class MyInformationCenterImpl extends InformationCenterImpl {
  }

}