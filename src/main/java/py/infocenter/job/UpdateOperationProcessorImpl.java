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

package py.infocenter.job;

import static py.icshare.OperationType.MIGRATE;
import static py.icshare.TargetType.DOMAIN;
import static py.icshare.TargetType.DRIVER;
import static py.icshare.TargetType.STORAGEPOOL;
import static py.icshare.TargetType.VOLUME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import py.app.context.AppContext;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.thrift.share.ListDomainRequest;
import py.thrift.share.ListDomainResponse;
import py.thrift.share.ListStoragePoolRequestThrift;
import py.thrift.share.ListStoragePoolResponseThrift;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * update operation per 5 second.
 *
 */
public class UpdateOperationProcessorImpl implements UpdateOperationProcessor {

  private static final Logger logger = LoggerFactory.getLogger(UpdateOperationProcessorImpl.class);
  private OperationStore operationStore;
  private VolumeJobStoreDb volumeJobStoreDb;
  private InformationCenterImpl informationCenter;
  private long runTimes = 0;
  private long rollbackPassTimeSecond;
  private long operationPassTimeSecond;
  private InstanceStore instanceStore;
  private VolumeInformationManger volumeInformationManger;
  private AppContext appContext;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  public UpdateOperationProcessorImpl() {
  }

  /**
   * calculate the size of Ok segments the segment is OK if there exits at least one
   * primary-segment-unit and one second-segment-unit is OK.the segment-unit is OK ,if InstanceId
   * equals to primary.
   *
   * <p>it had been replaced by calculateOKsegmentThroughSegmentStatus class when clone
   */
  public static int calculateOkSegment(VolumeMetadata volumeMetadata) {
    logger.info(
        "begin calculate the count of ok segments about volume :{}, the volume status and "
            + "action:{} {},segment size :{}",
        volumeMetadata.getVolumeId(), volumeMetadata.getVolumeStatus(),
        volumeMetadata.getVolumeStatus(),
        volumeMetadata.getSegmentTable().size());

    int count = 0;
    List<SegmentMetadata> segmentMetadataList = volumeMetadata.getSegments();
    for (SegmentMetadata segmentMetadata : segmentMetadataList) {
      logger.info("segmentMetadata status is : {}", segmentMetadata.getSegmentStatus());
      if (segmentMetadata.getSegmentStatus().available()) {
        count++;
      }
    }

    return count;
  }

  /**
   * for check the extend segment is ok.
   */
  public static int calculateOkForExtendSegment(VolumeMetadata volumeMetadata) {
    logger.info(
        "begin calculate the count of ok segments about volume :{}, the volume status and "
            + "action:{} {},ExtendSegmentTable size :{}",
        volumeMetadata.getVolumeId(), volumeMetadata.getVolumeStatus(),
        volumeMetadata.getVolumeStatus(),
        volumeMetadata.getExtendSegmentTable().size());

    int count = 0;
    List<SegmentMetadata> segmentMetadataList = volumeMetadata.getExtendSegments();
    for (SegmentMetadata segmentMetadata : segmentMetadataList) {
      logger.debug("Extend segmentMetadata status is : {}", segmentMetadata.getSegmentStatus());
      if (segmentMetadata.getSegmentStatus().available()) {
        count++;
      }
    }

    return count;
  }

  /**
   * calculate the size of in delete segments. the segment is deleted if there exits at least one
   * primary-segment-unit and one second-segment-unit is deleting or deleted.
   */
  public static int calculateInDeleteSegment(VolumeMetadata volumeMetadata) {
    logger.debug("begin calculate the count of in delete segments");
    int count = 0;
    logger.debug("volume metadata thrift id is {}, root-volume-id is {}",
        volumeMetadata.getVolumeId(),
        volumeMetadata.getRootVolumeId());
    logger.debug("other info of volumeMetadata thrift name {}, volumesize {}, extendsize {}",
        volumeMetadata.getName(), volumeMetadata.getVolumeSize(),
        volumeMetadata.getExtendingSize());
    Collection<SegmentMetadata> segmentMetadataList = volumeMetadata.getSegmentTable().values();
    for (SegmentMetadata segmentMetadata : segmentMetadataList) {
      logger.debug("segId:{}", segmentMetadata.getSegId());
      boolean checkPrimarySegment = false;
      boolean checkSecondSegment = false;
      List<SegmentUnitMetadata> segmentUnitsList = segmentMetadata.getSegmentUnits();
      for (SegmentUnitMetadata segmentUnitMetadata : segmentUnitsList) {
        logger.debug("segment-unit-metadata: instance id is {}, status is {}, membership is {}",
            segmentUnitMetadata.getInstanceId(), segmentUnitMetadata.getStatus(),
            segmentUnitMetadata.getMembership());
        if (segmentUnitMetadata.getInstanceId().getId() == segmentUnitMetadata.getMembership()
            .getPrimary().getId()) {
          logger.debug("this is primary segmentunit");
          logger.debug("segmentunit metadata status is {}", segmentUnitMetadata.getStatus());
          if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Deleted
              || segmentUnitMetadata.getStatus() == SegmentUnitStatus.Deleting) {
            logger.debug("it is deleted");
            checkPrimarySegment = true;
          } else {
            break;
          }
        } else {
          logger.debug("segmentunit metadata status is {}", segmentUnitMetadata.getStatus());
          if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Deleted
              || segmentUnitMetadata.getStatus() == SegmentUnitStatus.Deleting) {
            logger.debug("this is secondary segmentunit,and it is deleted");
            checkSecondSegment = true;
          }
        }
      }
      if (checkPrimarySegment && checkSecondSegment) {
        count++;
      }
    }
    return count;
  }


  public static OperationStatus createVolumeStatusToOperationStatus(VolumeStatus volumeStatus) {
    switch (volumeStatus) {
      case Available:
      case Stable:
        return OperationStatus.SUCCESS;
      case ToBeCreated:
      case Creating:
        return OperationStatus.ACTIVITING;
      default:
        return OperationStatus.FAILED;
    }
  }

  /**
   * change the status of driver to operation by the operation of launch driver. operation status is
   * success, if the status of driver launched; operation status is activiting, if the status of
   * volume is start or launching; operation status is failed ,else.
   */
  public static OperationStatus launchDriverToOperationStatus(DriverStatus driverStatus) {
    logger.debug("begin change status form driver to operation by launch, the driver status is {}",
        driverStatus);
    switch (driverStatus) {
      case SLAUNCHED:
      case LAUNCHED:
        return OperationStatus.SUCCESS;
      case START:
      case LAUNCHING:
        return OperationStatus.ACTIVITING;
      default:
        return OperationStatus.FAILED;
    }
  }

  /**
   * change the status of driver to operation by the operation of launch driver. operation status is
   * success, if the status of driver remove; operation status is failed, if the status of volume is
   * unknown; operation status is failed ,else.
   */
  public static OperationStatus umountDriverToOperationStatus(DriverStatus driverStatus) {
    logger.debug("begin change status form driver to operation by umount, the driver status is :{}",
        driverStatus);
    switch (driverStatus) {
      case REMOVING:
        return OperationStatus.SUCCESS;
      case UNKNOWN:
        return OperationStatus.FAILED;
      default:
        return OperationStatus.ACTIVITING;
    }
  }

  @Override
  @Scheduled(fixedDelay = 3000)
  public void updateOperation() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("only the master can do it");
      return;
    }

    logger.info(
        "begin updateOperation.class, rollbackPassTimeSecond is : {}, operationPassTimeSecond is "
            + ": {}",
        rollbackPassTimeSecond, operationPassTimeSecond);
    List<Operation> operationList = operationStore.getActiveOperation();
    if (operationList == null || operationList.isEmpty()) {
      return;
    }
    long currentTime = System.currentTimeMillis();
    for (Operation operation : operationList) {
      logger.debug("the operation: {}", operation);
      // set operation failed, if this operation not end for setting time.
      long passTime;
      if (operation.getOperationType().equals(MIGRATE)) {
        //todo: set 10 day
        passTime = rollbackPassTimeSecond * 1000 * 10;
      } else {
        passTime = operationPassTimeSecond * 1000;
      }
      if (currentTime - operation.getStartTime() > passTime) {
        // save operation to db
        operation.setProgress(0L);
        operation.setStatus(OperationStatus.FAILED);
        operation.setEndTime(currentTime);
        logger.debug("after update the operation is {}", operation);
        operationStore.saveOperation(operation);
        continue;
      }

      // when caught an exception, we not throw, continue to scanning the next operation
      try {
        // update target volume
        if (operation.getTargetType().equals(VOLUME)) {
          updateOperationByVolume(operation);
        }

        // update target driver
        if (operation.getTargetType().equals(DRIVER)) {
          updateOperationByDriver(operation);
        }

        if (operation.getTargetType().equals(STORAGEPOOL)) {
          updateOperationByStoragePool(operation);
        }

        if (operation.getTargetType().equals(DOMAIN)) {
          updateOperationByDomain(operation);
        }
      } catch (Exception e) {
        logger.error("caught an exception when update operation : {}", e);
      }
    }
    runTimes++;
    // delete end old-operations which pass 30 days
    if (runTimes % 100 == 0) {
      operationStore.deleteOldOperations();
    }
  }

  public OperationStore getOperationStore() {
    return operationStore;
  }

  public void setOperationStore(OperationStore operationStore) {
    this.operationStore = operationStore;
  }

  public InformationCenterImpl getInformationCenter() {
    return informationCenter;
  }

  public void setInformationCenter(InformationCenterImpl informationCenter) {
    this.informationCenter = informationCenter;
  }

  public long getRollbackPassTimeSecond() {
    return rollbackPassTimeSecond;
  }

  public void setRollbackPassTimeSecond(long rollbackPassTimeSecond) {
    this.rollbackPassTimeSecond = rollbackPassTimeSecond;
  }

  public long getOperationPassTimeSecond() {
    return operationPassTimeSecond;
  }

  public void setOperationPassTimeSecond(long operationPassTimeSecond) {
    this.operationPassTimeSecond = operationPassTimeSecond;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public void updateOperationByVolume(Operation operation) throws Exception {
    try {
      VolumeMetadata volumeMetadata = null;
      try {
        if (operation.getOperationType() == OperationType.MIGRATE) {
          //not need segment unit
          volumeMetadata = volumeInformationManger.getVolume(operation.getTargetId(),
              operation.getAccountId());
        } else {
          volumeMetadata = volumeInformationManger.getVolumeNew(operation.getTargetId(),
              operation.getAccountId());
        }

      } catch (VolumeNotFoundException e) {
        logger.error("when updateOperation, catch an exception {}", e);
      }

      // if operation-type is delete, maybe the volume-id has been deleted
      // and couldn't be found
      if (volumeMetadata == null) {
        if (operation.getOperationType() == OperationType.DELETE) {
          long percentage = 100L;
          OperationStatus status = OperationStatus.SUCCESS;

          // save operation to db
          operation.setProgress(percentage);
          operation.setStatus(status);
          operation.setEndTime(System.currentTimeMillis());
          logger.debug("after update the operation is {}", operation);
          operationStore.saveOperation(operation);
          return;
        } else {
          logger.error("volume not found {}", operation.getTargetId());

          // set operation failed and save to db
          operation.setStatus(OperationStatus.FAILED);
          operation.setEndTime(System.currentTimeMillis());
          logger.warn("after update the operation is {}", operation);
          operationStore.saveOperation(operation);
          return;
        }
      } else {
        // operation induced by create volume
        if (operation.getOperationType() == OperationType.CREATE) {
          updateOperationByCreateVolume(operation, volumeMetadata);
        }

        // operation induced by extend volume
        if (operation.getOperationType() == OperationType.EXTEND) {
          updateOperationByExtendVolume(operation, volumeMetadata);
        }

        // operation induced by delete volume
        if (operation.getOperationType() == OperationType.DELETE) {
          updateOperationByDeleteVolume(operation, volumeMetadata);
        }

        // operation induced by cycle volume
        if (operation.getOperationType() == OperationType.CYCLE) {
          updateOperationByCycleVolume(operation, volumeMetadata);
        }

        // operation induced by MIGRATE
        if (operation.getOperationType().equals(MIGRATE)) {
          updateOperationByMigrateVolume(operation, volumeMetadata);
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception when update operation by volume");
      throw e;
    }
  }


  public void updateOperationByCreateVolume(Operation operation, VolumeMetadata volumeMetadata) {
    logger.info("access create volume operation update, the operation: {}", operation);
    long percentage;
    OperationStatus status;
    long segmentsPossese = volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize();
    int okSegments = calculateOkSegment(volumeMetadata);
    percentage = (long) (100 * okSegments / (segmentsPossese));
    VolumeStatus volumeStatus = volumeMetadata.getVolumeStatus();
    status = createVolumeStatusToOperationStatus(volumeStatus);

    // save operation to db
    operation.setProgress(percentage);
    operation.setStatus(status);
    operation.setEndTime(System.currentTimeMillis());
    operationStore.saveOperation(operation);
    logger.info("after update the operation is {}", operation);
    if (percentage == 100) {
      logger.warn("when updateOperationByCreateVolume, the volume :{}, Operation is ok",
          volumeMetadata.getVolumeId());
    }

  }


  public void updateOperationByMigrateVolume(Operation operation, VolumeMetadata volumeMetadata) {
    logger.info("Migrate volume :{} operation update, the operation: {}, the migrationRatio:{}",
        operation.getTargetId(), operation, volumeMetadata.getMigrationRatio());
    double migrationRatio = volumeMetadata.getMigrationRatio();
    VolumeStatus volumeStatus = volumeMetadata.getVolumeStatus();
    // save operation to db
    operation.setProgress((long) migrationRatio);
    if (migrationRatio >= 100
        || volumeStatus == VolumeStatus.Dead
        || volumeStatus == VolumeStatus.Deleted
        || volumeStatus == VolumeStatus.Deleting) {
      logger.warn(
          "when updateOperationByMigrateVolume, the volume :{}, migration finish, the "
              + "operation:{} ",
          volumeMetadata.getVolumeId(), operation);
      operation.setProgress(100L);
      operation.setStatus(OperationStatus.SUCCESS);
    }
    operation.setEndTime(System.currentTimeMillis());
    operationStore.saveOperation(operation);
    logger.info("after updateOperationByMigrateVolume the operation is {}", operation);
  }


  public void updateOperationByCycleVolume(Operation operation, VolumeMetadata volumeMetadata) {
    logger.info("access cycle volume operation update, the operation: {}", operation);
    long percentage;
    OperationStatus status = OperationStatus.ACTIVITING;
    logger.debug("when updateOperationByCycleVolume, volume status is: {}",
        volumeMetadata.getVolumeStatus());
    long segmentsPossese = volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize();
    int okSegments = calculateOkSegment(volumeMetadata);
    long segmentsExtend = volumeMetadata.getExtendingSize() / volumeMetadata.getSegmentSize();
    logger.debug("extend segment must be 0 : {}", segmentsExtend);
    percentage = (long) (100 * okSegments / segmentsPossese);
    VolumeStatus volumeStatus = volumeMetadata.getVolumeStatus();
    if (volumeStatus == VolumeStatus.Dead) {
      status = OperationStatus.FAILED;
    }
    if (percentage == 100L) {
      status = OperationStatus.SUCCESS;
    }

    // save operation to db
    operation.setProgress(percentage);
    operation.setStatus(status);
    operation.setEndTime(System.currentTimeMillis());
    operationStore.saveOperation(operation);
    logger.info("updateOperationByCycleVolume, after update the operation is {}", operation);

    if (operation.getProgress() == 100L) {
      logger.warn("when updateOperationByCycleVolume, the volume :{}, Operation is ok",
          volumeMetadata.getVolumeId());
    }
  }


  public void updateOperationByExtendVolume(Operation operation, VolumeMetadata volumeMetadata) {
    logger.debug("access extend volume operation update, the operation {}", operation);
    long percentage;
    OperationStatus status = OperationStatus.ACTIVITING;
    logger.debug("when updateOperationByExtendVolume,volume status is: {}",
        volumeMetadata.getVolumeStatus());
    long segmentsPossese = volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize();

    int okExtendSegments = calculateOkForExtendSegment(volumeMetadata);
    logger.debug("ok segments is {},segmentsPossese is {}", okExtendSegments, segmentsPossese);
    Long targetSourceSize = operation.getTargetSourceSize();
    logger.debug("target source size is {}", targetSourceSize);
    long srcSegments = 0L;
    if (targetSourceSize != null) {
      srcSegments = targetSourceSize / volumeMetadata.getSegmentSize();
    }
    logger.debug("source volume segments is {}", srcSegments);

    long segmentsExtend = volumeMetadata.getExtendingSize() / volumeMetadata.getSegmentSize();
    if (segmentsExtend == 0) { // extend ok
      percentage = 100L;
    } else {
      percentage = (long) (100 * (okExtendSegments / segmentsExtend));
    }

    logger.warn("when updateOperationByExtendVolume, the percentage is {}", percentage);
    if (percentage == 0) {
      //may be the extend begin or the extend ok
      if (segmentsPossese - srcSegments > 0) {
        percentage = 100L;
      }
    }

    if (percentage == 100L) {
      status = OperationStatus.SUCCESS;
    }

    // save operation to db
    operation.setProgress(percentage);
    operation.setStatus(status);
    operation.setEndTime(System.currentTimeMillis());
    logger.info("when updateOperationByExtendVolume, after update the operation is {}", operation);
    operationStore.saveOperation(operation);
  }


  public void updateOperationByDeleteVolume(Operation operation, VolumeMetadata volumeMetadata) {
    logger.debug("access delete volume operation update, the operation: {}", operation);
    int inDeleteSegments = calculateInDeleteSegment(volumeMetadata);
    long percentage;
    OperationStatus status = OperationStatus.ACTIVITING;
    logger.debug("volumeMetadataThrift status is: {}", volumeMetadata.getVolumeStatus());
    long segmentsPossese = volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize();
    logger.debug("in delete segments is {},all segments is {}", inDeleteSegments, segmentsPossese);
    percentage = (long) (100 * inDeleteSegments / segmentsPossese);
    VolumeStatus volumeStatus = volumeMetadata.getVolumeStatus();
    if (volumeStatus == VolumeStatus.Dead || percentage == 100L) {
      status = OperationStatus.SUCCESS;
      // if volume status is dead, set percentage to 100.
      if (percentage != 100) {
        percentage = 100L;
      }
    }

    // save operation to db
    operation.setProgress(percentage);
    operation.setStatus(status);
    operation.setEndTime(System.currentTimeMillis());
    logger.info("when updateOperationByDeleteVolume, after update, the operation is {}", operation);
    operationStore.saveOperation(operation);
  }


  public void updateOperationByDriver(Operation operation) {
    try {
      logger.warn("update operation by driver, the operation: {}", operation);
      int launchOrUmountDriverAccount = 0;
      OperationStatus status = OperationStatus.ACTIVITING;
      VolumeMetadataAndDrivers volumeAndDriverInfo;
      try {
        volumeAndDriverInfo = volumeInformationManger
            .getDriverContainVolumes(operation.getTargetId(), operation.getAccountId(), false);
      } catch (VolumeNotFoundException e) {
        logger.error("when updateOperationByDriver, can not find the volume:{}",
            operation.getTargetId());

        // set operation failed and save to db
        operation.setStatus(OperationStatus.FAILED);
        operation.setEndTime(System.currentTimeMillis());
        logger.debug("after update by driver, the operation is {}", operation);
        operationStore.saveOperation(operation);
        return;
      } catch (Exception e) {
        logger.error("catch an exception {}", e);
        return;
      }
      List<DriverMetadata> driverMetadataList = volumeAndDriverInfo.getDriverMetadatas();
      logger.warn("driverMetadataList is : {}", driverMetadataList);
      logger.debug("launch or umount driver {}", operation.getOperationType());
      List<EndPoint> endPointList = operation.getEndPointList();
      for (DriverMetadata driverMetadata : driverMetadataList) {
        logger.debug("driverMetadata hostname is {}", driverMetadata.getHostName());
      }

      logger.debug("endPointList is {}", endPointList);
      for (DriverMetadata driverMetadata : driverMetadataList) {
        DriverStatus driverStatus = driverMetadata.getDriverStatus();
        long driverContainerId = driverMetadata.getDriverContainerId();
        Instance driverContainer = instanceStore.get(new InstanceId(driverContainerId));
        String hostName = driverContainer.getEndPointByServiceName(PortType.CONTROL).getHostName();
        /*            String hostName = driverMetadata.getHostName();*/
        logger.warn("driver metadata hostName: {}", hostName);
        logger.warn("driver metadata status is {}", driverStatus);
        for (EndPoint endPoint : endPointList) {
          logger.warn("end point hostname: {}", endPoint.getHostName());
          if (endPoint.getHostName().equals(hostName)) {
            OperationStatus operationStatus = null;
            if (operation.getOperationType() == OperationType.LAUNCH) {
              operationStatus = launchDriverToOperationStatus(driverStatus);
            }
            if (operation.getOperationType() == OperationType.UMOUNT) {
              operationStatus = umountDriverToOperationStatus(driverStatus);
            }
            if (operationStatus == OperationStatus.SUCCESS) {
              launchOrUmountDriverAccount++;
            } else if (operationStatus == OperationStatus.FAILED) {
              status = OperationStatus.FAILED;
            }
          }
        }
      }
      long percentage = (long) (100 * launchOrUmountDriverAccount / endPointList.size());
      if (percentage == 100L) {
        status = OperationStatus.SUCCESS;
      }

      // save operation to db
      operation.setProgress(percentage);
      operation.setStatus(status);
      operation.setEndTime(System.currentTimeMillis());
      logger.debug("after update the operation is {}", operation);
      operationStore.saveOperation(operation);
    } catch (Exception e) {
      logger.error("caught an exception when update operation by driver : {}", e);
      throw e;
    }
  }

  private void updateOperationByStoragePool(Operation operation) throws Exception {
    ListStoragePoolRequestThrift request = new ListStoragePoolRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    List<Long> poolIds = new ArrayList<>();
    poolIds.add(operation.getTargetId());
    request.setStoragePoolIds(poolIds);
    request.setAccountId(operation.getAccountId());
    ListStoragePoolResponseThrift response = null;
    try {
      response = informationCenter.listStoragePools(request);
    } catch (Exception e) {
      logger.error("catch an exception {}", e);
    }
    if (operation.getOperationType().equals(OperationType.DELETE)) {
      if (response.getStoragePoolDisplays().isEmpty()) {
        operation.setProgress(100L);
        operation.setStatus(OperationStatus.SUCCESS);
      }
      operation.setEndTime(System.currentTimeMillis());
      logger.info("when updateOperationByStoragePool, after update the operation is {}", operation);
      operationStore.saveOperation(operation);
    }
  }

  private void updateOperationByDomain(Operation operation) throws Exception {
    ListDomainRequest request = new ListDomainRequest();
    request.setRequestId(RequestIdBuilder.get());
    List<Long> domainIds = new ArrayList<>();
    domainIds.add(operation.getTargetId());
    request.setDomainIds(domainIds);
    request.setAccountId(operation.getAccountId());
    ListDomainResponse response = null;
    try {
      response = informationCenter.listDomains(request);
    } catch (Exception e) {
      logger.error("when updateOperationByDomain, catch an exception {}", e);
    }
    if (operation.getOperationType().equals(OperationType.DELETE) && response != null) {
      if (response.getDomainDisplays().isEmpty()) {
        operation.setProgress(100L);
        operation.setStatus(OperationStatus.SUCCESS);
      }
      operation.setEndTime(System.currentTimeMillis());
      logger.info("when updateOperationByDomain, after update the operation is {}", operation);
      operationStore.saveOperation(operation);
    }
  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public VolumeJobStoreDb getVolumeJobStoreDb() {
    return volumeJobStoreDb;
  }

  public void setVolumeJobStoreDb(VolumeJobStoreDb volumeJobStoreDb) {
    this.volumeJobStoreDb = volumeJobStoreDb;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public LockForSaveVolumeInfo getLockForSaveVolumeInfo() {
    return lockForSaveVolumeInfo;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }
}
