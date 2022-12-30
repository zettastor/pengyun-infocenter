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

package py.infocenter.instance.manger;

import static py.informationcenter.Utils.MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.Constants;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.driver.DriverMetadata;
import py.exception.GenericThriftClientFactoryException;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.store.DriverStore;
import py.infocenter.store.VolumeStore;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.infocenter.service.UpdateVolumeLayoutRequest;
import py.thrift.infocenter.service.UpdateVolumeLayoutResponse;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.VolumeNameExistedExceptionThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;

public class VolumeInformationManger {

  private static final Logger logger = LoggerFactory.getLogger(VolumeInformationManger.class);
  private VolumeStore volumeStore;
  private InformationCenterClientFactory infoCenterClientFactory;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private InfoCenterAppContext appContext;

  /**
   * save the all volume which load form db,just with master for list volume.
   **/
  private DriverStore driverStore;

  private HaInstanceManger haInstanceManger;

  //when the volume Equilibrium ok or more instance report, delete in in old instance;
  private Set<Long> volumeToDeleteForEquilibrium;

  //the all segment number which master or follower can save
  private long allSegmentNumberToSave = 10000;

  //the all segment number which master can save = all.segment.number.to.save * percent.number
  // .master.segment
  private double percentNumberSegmentNumberMasterSave = 0.6;

  public VolumeInformationManger() {
    volumeToDeleteForEquilibrium = new HashSet<>();
  }

  public VolumeMetadata getVolumeNew(long volumeId, long accountId)
      throws VolumeNotFoundException {

    //get volume from master db and memory
    VolumeMetadata volume;
    VolumeMetadata volumeMetadata = null;

    //the follower get volume must form db
    if (InstanceStatus.HEALTHY == appContext.getStatus()) {
      volumeMetadata = volumeStore.getVolume(volumeId);
    } else {
      volumeMetadata = volumeStore.followerGetVolume(volumeId);
    }

    if (volumeMetadata == null) {
      logger.warn("can not find the volume :{}", volumeId);
      throw new VolumeNotFoundException();
    } else {
      //**  Dead volume not contain the segments ,the method for get volume with segments **/
      if (VolumeStatus.Dead.equals(volumeMetadata.getVolumeStatus())) {
        logger.warn("getVolumeNew find the volume :{}, {}, the status is Dead", volumeId,
            volumeMetadata.getName());
        throw new VolumeNotFoundException();
      }
    }

    int maxAttempts = 3;
    while (maxAttempts-- > 0) {
      try {
        //get volume from local memory
        volume = volumeStore.getVolumeForReport(volumeId);
        if (volume != null) {
          mergeSomeVolumeInfo(volume);
          checkTheVolumeWhenGet(volume);
          return volume;
        } else {
          //try to get from follower
          long instanceId = getInstanceFromTable(volumeId);
          if (instanceId != 0) {
            Instance flowerInstance = haInstanceManger.getFollower(instanceId);
            if (flowerInstance == null) {
              logger.warn("can not find the current follower :{}, may be down", instanceId);
              haInstanceManger.updateInstance();
              throw new VolumeNotFoundException();
            }

            EndPoint endPoint = flowerInstance.getEndPoint();

            try {
              VolumeMetadataAndDrivers volumeMetadataAndDrivers = infoCenterClientFactory
                  .build(endPoint)
                  .getVolumeByPagination(volumeId, accountId);
              if (volumeMetadataAndDrivers != null
                  && volumeMetadataAndDrivers.getVolumeMetadata() != null) {
                logger
                    .warn("find the volume :{} in follower :{} {}", volumeId, instanceId, endPoint);
                volume = volumeMetadataAndDrivers.getVolumeMetadata();
                mergeSomeVolumeInfo(volume);
                checkTheVolumeWhenGet(volume);
                return volume;
              } else {
                //** may be the volume no report by date node this time **/
                logger.error(
                    "can not find the volume :{} in follower :{} {}, may be the volume has no "
                        + "report this time",
                    volumeId, instanceId, endPoint);

              }
            } catch (VolumeNotFoundExceptionThrift volumeNotFoundExceptionThrift) {
              logger.error("when get volume :{}, can not find it:", volumeId);
              throw volumeNotFoundExceptionThrift;
            } catch (GenericThriftClientFactoryException e) {
              logger.warn("can not find the current follower :{}, may be down", endPoint);
              haInstanceManger.updateInstance();
              throw new VolumeNotFoundException();
            } catch (Exception e) {
              logger.error("when get volume :{}, find a exception:", volumeId, e);
              throw e;
            }
          } else {

            //**use the db **/
            logger.warn(
                "can not find the volume :{} in any instance, may be the unit has not report this"
                    + " time"

                    + ",and the table :{}", volumeId,
                instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap());
            throw new VolumeNotFoundException();
          }
        }
      } catch (VolumeNotFoundException e) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          logger.warn("when getVolume :{}, can not get it and can't sleep", volumeId, e1);
        }

        continue;

      } catch (Exception e) {
        logger.error("when getVolume :{}, caught a exception :", volumeId, e);
        break;
      }
    } // end while

    //**use the db **/
    if (volumeMetadata == null) {
      logger.warn("can not find the volume :{}", volumeId);
      throw new VolumeNotFoundException();
    }

    return volumeMetadata;
  }


  public String updateVolumeLayoutToInstance(UpdateVolumeLayoutRequest request) {
    //get volume from master db and memory
    long volumeId = request.getVolumeId();
    long instanceId = getInstanceFromTable(volumeId);
    if (instanceId != 0) {
      Instance flowerInstance = haInstanceManger.getFollower(instanceId);
      if (flowerInstance == null) {
        logger.warn("can not find the current follower :{}, may be down", instanceId);
        haInstanceManger.updateInstance();
        return null;
      }

      EndPoint endPoint = flowerInstance.getEndPoint();

      try {
        InformationCenter.Iface client = infoCenterClientFactory.build(endPoint).getClient();
        UpdateVolumeLayoutResponse response = client.updateVolumeLayout(request);
        logger.warn("find the volume :{} in follower :{} {}", volumeId, instanceId, endPoint);
        String volumeLayout = response.getVolumeLayout();
        return volumeLayout;

      } catch (GenericThriftClientFactoryException e) {
        logger.warn("can not find the current follower :{}, may be down", endPoint);
        haInstanceManger.updateInstance();
      } catch (Exception e) {
        logger.error("when get volume :{}, find a exception:", volumeId, e);
      }
    } else {
      logger
          .warn("when updateVolumeLayoutToInstance, can not find the volume :{} in any instance, "

                  + "may be the unit has not report this time,and the table :{}", volumeId,
              instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap());
    }
    return null;
  }


  /**
   * if the volume is change,bu not update to follower immediately, and this time to get volume, so
   * the volume value is not good, so check the volume to db.
   */
  public void checkTheVolumeWhenGet(VolumeMetadata volumeFromGet) {
    //only the master check the volume
    if (InstanceStatus.HEALTHY != appContext.getStatus()) {
      return;
    }

    long volumeId = volumeFromGet.getVolumeId();
    VolumeMetadata volumeInDb = volumeStore.getVolume(volumeId);

    /* 1. check status, follower can not set status when the master volume is deleting
     *     the user Deleting one volume
     */
    VolumeStatus masterVolumeStatus = volumeInDb.getVolumeStatus();
    VolumeStatus volumeFromGetStatus = volumeFromGet.getVolumeStatus();

    if ((masterVolumeStatus == VolumeStatus.Deleting)
        && (volumeFromGetStatus != VolumeStatus.Deleting
        && volumeFromGetStatus != VolumeStatus.Deleted
        && volumeFromGetStatus != VolumeStatus.Dead)) {

      logger.warn(
          "when get volume,find volume delete by user, the volume :{} {}, the master status is "
              + ":{}, the get status is :{},"

              + "use the master value", volumeInDb.getName(), volumeInDb.getVolumeId(),
          masterVolumeStatus, volumeFromGetStatus);
      volumeFromGet.setVolumeStatus(masterVolumeStatus);
    }

    /* 2. check status, follower can not set status when the master volume is Recycling
     *     the user Recycling one volume
     */
    if ((masterVolumeStatus == VolumeStatus.Recycling)
        && (volumeFromGetStatus == VolumeStatus.Deleting
        || volumeFromGetStatus == VolumeStatus.Deleted)) {

      logger.warn(
          "when get volume,find volume Recycling by user, the volume :{} {}, the master status is"
              + " :{}, the get status is :{}, "

              + "use the master value", volumeInDb.getName(), volumeInDb.getVolumeId(),
          masterVolumeStatus, volumeFromGetStatus);
      //if status Deleting or Recycling, use the master status
      volumeFromGet.setVolumeStatus(masterVolumeStatus);
    }

    /* 3. check action, follower can not change action, only the maseter change action
     *     if different, use the master action
     ***/
    VolumeInAction masterVolumeAction = volumeInDb.getInAction();
    VolumeInAction reportVolumeAction = volumeFromGet.getInAction();

    if (!masterVolumeAction.equals(reportVolumeAction)) {
      logger.warn(
          "when get volume,find volume action change, the volume :{} {}, the master action is "
              + ":{}, the get action is :{}, "

              + "use the master value", volumeInDb.getName(), volumeId, masterVolumeAction,
          reportVolumeAction);
      volumeFromGet.setInAction(masterVolumeAction);
    }

    /* 4 . check volume name , when move volume or other, the master change the volume name
     *      if different, use the master name
     ***/
    if (!volumeInDb.getName().equals(volumeFromGet.getName())) {
      logger.warn(
          "when get volume,find volume name change, the volume :{}, the master name is :{}, the "
              + "get name is :{}, "

              + "use the master value", volumeId, volumeInDb.getName(), volumeFromGet.getName());
      volumeFromGet.setName(volumeInDb.getName());
    }

    /* 5 . check volume readAndWrite , the console change the volume readAndWrite
     *      if different, use the master value
     ***/
    if (!volumeInDb.getReadWrite().equals(volumeFromGet.getReadWrite())) {
      logger.warn(
          "when get volume,find volume readAndWrite change, the volume :{} {}, the master "
              + "readAndWrite is :{},"

              + " the get readAndWrite is :{}, use the master value", volumeInDb.getName(),
          volumeId,
          volumeInDb.getReadWrite(), volumeFromGet.getReadWrite());
      volumeFromGet.setReadWrite(volumeInDb.getReadWrite());
    }

    /* 6 . check volume ExtendingSize , when extend the volume, the ExtendingSize change
     *      if different and the volume size is equals, the master value
     ***/
    if (volumeInDb.getExtendingSize() != volumeFromGet.getExtendingSize()
        && volumeInDb.getVolumeSize() == volumeFromGet.getVolumeSize()) {
      logger.warn(
          "when get volume,find volume ExtendingSize change, the volume:{} {}, the master "
              + "ExtendingSize is :{}, "

              + "the get ExtendingSize is :{}, use the master value", volumeInDb.getName(),
          volumeId,
          volumeInDb.getExtendingSize(), volumeFromGet.getExtendingSize());
      volumeFromGet.setExtendingSize(volumeInDb.getExtendingSize());
    }

    //change LastExtendedTime Changee for extend volume
    if (checkLastExtendedTimeChange(volumeInDb, volumeFromGet)) {
      logger.warn(
          "when get volume,find volume LastExtendedTime change, the volume :{} {}, the master "
              + "LastExtendedTime is :{}, "

              + "the get LastExtendedTime is :{}, use the master value", volumeFromGet.getName(),
          volumeId, volumeInDb.getLastExtendedTime(), volumeFromGet.getLastExtendedTime());
      volumeFromGet.setLastExtendedTime(volumeInDb.getLastExtendedTime());
    }

    //change the mark delete info
    if (!new Boolean(volumeInDb.isMarkDelete()).equals(new Boolean(volumeFromGet.isMarkDelete()))) {
      logger.warn(
          "for the report response,find volume isMarkDelete change, the volume :{} {}, the master"
              + " isMarkDelete is :{}, "

              + "i get isMarkDelete is :{}, use the master value", volumeFromGet.getName(),
          volumeId, volumeInDb.isMarkDelete(), volumeFromGet.isMarkDelete());
      volumeFromGet.setMarkDelete(volumeInDb.isMarkDelete());
    }

    //
    if (volumeInDb.getVolumeDescription() != null && !volumeInDb.getVolumeDescription()
        .equals(volumeFromGet.getVolumeDescription())) {
      logger.warn(
          "when get volume,find volume description change, the volume :{}, the master description"
              + " is :{}, the get description is :{}, "

              + "use the master value", volumeId, volumeInDb.getVolumeDescription(),
          volumeFromGet.getVolumeDescription());
      volumeFromGet.setVolumeDescription(volumeInDb.getVolumeDescription());
    }

    /* 7. rebuild the rebalance Info, the RebalanceInfo only the master do it, so used master
     *
     ***/
    volumeFromGet.setRebalanceInfo(volumeInDb.getRebalanceInfo());
    volumeFromGet.setStableTime(volumeInDb.getStableTime());
    volumeFromGet.setClientLastConnectTime(volumeInDb.getClientLastConnectTime());
  }

  public Pair mergeVolumeInfo(VolumeMetadata volume) {
    // Set the basic information of volume meta data
    VolumeMetadata virtualVolume = new VolumeMetadata();

    virtualVolume.setVolumeId(volume.getVolumeId());
    virtualVolume.setRootVolumeId(volume.getRootVolumeId());
    virtualVolume.setAccountId(volume.getAccountId());
    virtualVolume.setChildVolumeId(null);
    virtualVolume.setName(volume.getName());
    virtualVolume.setVolumeType(volume.getVolumeType());
    virtualVolume.setSegmentSize(volume.getSegmentSize());
    virtualVolume.setDeadTime(volume.getDeadTime());
    virtualVolume.setExtendingSize(volume.getExtendingSize());
    virtualVolume.setDomainId(volume.getDomainId());
    virtualVolume.setSegmentNumToCreateEachTime(volume.getSegmentNumToCreateEachTime());
    virtualVolume.setStoragePoolId(volume.getStoragePoolId());
    virtualVolume.setVolumeCreatedTime(volume.getVolumeCreatedTime());
    virtualVolume.setLastExtendedTime(volume.getLastExtendedTime());
    virtualVolume.setVolumeSource(volume.getVolumeSource());
    virtualVolume.setReadWrite(volume.getReadWrite());
    virtualVolume.setPageWrappCount(volume.getPageWrappCount());
    virtualVolume.setSegmentWrappCount(volume.getSegmentWrappCount());
    virtualVolume.setInAction(volume.getInAction());
    virtualVolume.setEnableLaunchMultiDrivers(volume.isEnableLaunchMultiDrivers());
    virtualVolume.getRebalanceInfo()
        .setRebalanceVersion(volume.getRebalanceInfo().getRebalanceVersion());
    virtualVolume.setStableTime(volume.getStableTime());
    virtualVolume.setEachTimeExtendVolumeSize(volume.getEachTimeExtendVolumeSize());
    virtualVolume.setMarkDelete(volume.isMarkDelete());
    virtualVolume.setVolumeDescription(volume.getVolumeDescription());

    // merge all volume and calculate total free space ratio
    double totalFreeSpaceRatio = 0.0;
    int logicSegIndex = 0;
    long volumeTotalPageToMigrate = 0;
    long volumeAlreadyMigratedPage = 0;
    long volumeMigrationSpeed = 0;

    long totalPhysicalSpace = 0;
    long segmentSize = volume.getSegmentSize();

    //** if there is no segment unit in one segment, volume won't get real segId ***/
    int segmentCount = volume.getSegmentCount();

    for (int i = 0; i < segmentCount; i++) {
      SegId segId = new SegId(volume.getVolumeId(), i);
      virtualVolume.recordLogicSegIndexAndSegId(logicSegIndex++, segId);
    }

    List<SegmentMetadata> segmentMetadatas = volume.getSegments();
    List<SegmentMetadata> extendSegmentMetadatas = volume.getExtendSegments();

    // calculate the total volume ratio
    double volumeFreeSpaceRatio;
    double volumeAllSegFreeSpaceRatio = 0.0;
    long segmentUnitsNumber = 0;
    for (SegmentMetadata segmentMetadata : segmentMetadatas) {
      volumeAllSegFreeSpaceRatio += segmentMetadata.getFreeRatio();
      for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
        volumeTotalPageToMigrate += segmentUnitMetadata.getTotalPageToMigrate();
        volumeAlreadyMigratedPage += segmentUnitMetadata.getAlreadyMigratedPage();
        volumeMigrationSpeed += segmentUnitMetadata.getMigrationSpeed();

        //mark the segmentUnitsNumber
        segmentUnitsNumber++;
      }

      totalPhysicalSpace += segmentMetadata.getWritableUnitNumber() * segmentSize;

    }

    //for extend volume
    for (SegmentMetadata segmentMetadata : extendSegmentMetadatas) {
      segmentUnitsNumber += segmentMetadata.getSegmentUnits().size();
    }

    volumeFreeSpaceRatio = volumeAllSegFreeSpaceRatio / segmentMetadatas.size();

    double totalFreeSpace = 0.0;
    totalFreeSpace += volume.getSegments().size() * volumeFreeSpaceRatio;
    double totalSpace = 0.0;
    totalSpace += volume.getSegments().size();

    long totalRebalanceSteps = 0;
    long remainRebalanceSteps = 0;
    boolean isRebalanceCalculating = false;

    //reblance task steps
    totalRebalanceSteps += volume.getRebalanceInfo().getRebalanceTotalTaskCount();
    remainRebalanceSteps += volume.getRebalanceInfo().getRebalanceRemainTaskCount();
    if (volume.getRebalanceInfo().isRebalanceCalculating()) {
      isRebalanceCalculating = true;
    }

    if (totalSpace != 0) {
      totalFreeSpaceRatio = totalFreeSpace / totalSpace;
    }

    virtualVolume.setTotalPhysicalSpace(totalPhysicalSpace);
    virtualVolume.setFreeSpaceRatio(totalFreeSpaceRatio);
    virtualVolume.setTotalPageToMigrate(volumeTotalPageToMigrate);
    virtualVolume.setAlreadyMigratedPage(volumeAlreadyMigratedPage);
    virtualVolume.setMigrationSpeed(volumeMigrationSpeed);
    virtualVolume.setMigrationRatio(
        0 == volumeTotalPageToMigrate ? 100
            : (volumeAlreadyMigratedPage * 100) / volumeTotalPageToMigrate);

    // this is very important variable, please pay attention on currentSegmentIndex before try to
    // modify pagination code
    AtomicInteger currentSegmentIndex = new AtomicInteger(0);

    //set size
    virtualVolume.setNextStartSegmentIndex(currentSegmentIndex.get());
    virtualVolume.setVolumeSize(volume.getVolumeSize());

    // set the volume layout
    virtualVolume.initVolumeLayout();
    //set status
    virtualVolume.setVolumeStatus(volume.getVolumeStatus());

    //about rebalance, not care
    VolumeRebalanceInfo rebalanceInfo = virtualVolume.getRebalanceInfo();
    rebalanceInfo.setRebalanceCalculating(isRebalanceCalculating);
    rebalanceInfo.setRebalanceTotalTaskCount(totalRebalanceSteps);
    rebalanceInfo.setRebalanceRemainTaskCount(remainRebalanceSteps);
    rebalanceInfo.calcRatio();

    Pair<VolumeMetadata, Long> volumeInfo = new Pair<>();
    volumeInfo.setFirst(virtualVolume);
    volumeInfo.setSecond(segmentUnitsNumber);

    logger.info("when merge, get the Migrate info :{}, {}, {}, {},",
        virtualVolume.getTotalPageToMigrate(),
        virtualVolume.getAlreadyMigratedPage(),
        virtualVolume.getMigrationSpeed(), virtualVolume.getMigrationRatio());

    if (virtualVolume.getTotalPageToMigrate() > 0 || virtualVolume.getAlreadyMigratedPage() > 0
        || virtualVolume.getMigrationSpeed() > 0 || virtualVolume.getMigrationRatio() > 0) {
      logger.info("when merge for test, get the Migrate info :{}, {}, {}, {},",
          virtualVolume.getTotalPageToMigrate(),
          virtualVolume.getAlreadyMigratedPage(),
          virtualVolume.getMigrationSpeed(), virtualVolume.getMigrationRatio());

    }

    logger.info("when merge, the volume is :{}", volumeInfo);
    return volumeInfo;
  }

  //TODO: can easy do it, change next time

  public void mergeSomeVolumeInfo(VolumeMetadata volume) {
    // merge all volume and calculate total free space ratio
    double totalFreeSpaceRatio = 0.0;
    int logicSegIndex = 0;
    long volumeTotalPageToMigrate = 0;
    long volumeAlreadyMigratedPage = 0;
    long volumeMigrationSpeed = 0;

    long totalPhysicalSpace = 0;
    long segmentSize = volume.getSegmentSize();

    //** if there is no segment unit in one segment, volume won't get real segId ***/
    int segmentCount = volume.getSegmentCount();

    for (int i = 0; i < segmentCount; i++) {
      SegId segId = new SegId(volume.getVolumeId(), i);
      volume.recordLogicSegIndexAndSegId(logicSegIndex++, segId);
    }

    List<SegmentMetadata> segmentMetadatas = volume.getSegments();

    // calculate the total volume ratio
    double volumeFreeSpaceRatio;
    double volumeAllSegFreeSpaceRatio = 0.0;
    for (SegmentMetadata segmentMetadata : segmentMetadatas) {
      volumeAllSegFreeSpaceRatio += segmentMetadata.getFreeRatio();
      for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
        volumeTotalPageToMigrate += segmentUnitMetadata.getTotalPageToMigrate();
        volumeAlreadyMigratedPage += segmentUnitMetadata.getAlreadyMigratedPage();
        volumeMigrationSpeed += segmentUnitMetadata.getMigrationSpeed();
      }

      totalPhysicalSpace += segmentMetadata.getWritableUnitNumber() * segmentSize;

    }

    volumeFreeSpaceRatio = volumeAllSegFreeSpaceRatio / segmentMetadatas.size();
    double totalFreeSpace = 0.0;
    double totalSpace = 0.0;

    totalFreeSpace += volume.getSegments().size() * volumeFreeSpaceRatio;
    totalSpace += volume.getSegments().size();

    if (totalSpace != 0) {
      totalFreeSpaceRatio = totalFreeSpace / totalSpace;
    }

    volume.setTotalPhysicalSpace(totalPhysicalSpace);
    volume.setFreeSpaceRatio(totalFreeSpaceRatio);
    volume.setTotalPageToMigrate(volumeTotalPageToMigrate);
    volume.setAlreadyMigratedPage(volumeAlreadyMigratedPage);
    volume.setMigrationSpeed(volumeMigrationSpeed);
    volume.setMigrationRatio(
        0 == volumeTotalPageToMigrate ? 100
            : (volumeAlreadyMigratedPage * 100) / volumeTotalPageToMigrate);

    logger.info("when merge for get, get the Migrate info :{}, {}, {}, {},",
        volume.getTotalPageToMigrate(), volume.getAlreadyMigratedPage(),
        volume.getMigrationSpeed(), volume.getMigrationRatio());
    logger.info("when merge, the volume is :{}", volume);
  }

  public VolumeMetadata getVolume(long volumeId, long accountId) throws VolumeNotFoundException {
    //get volume from master db and memory
    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
    if (volumeMetadata == null) {
      throw new VolumeNotFoundException();
    } else {
      return volumeMetadata;
    }
  }

  public List<VolumeMetadata> listVolumes() {
    //get volume from master db and memory
    List<VolumeMetadata> result = new ArrayList<>();
    List<VolumeMetadata> volumeMetadatas = volumeStore.listVolumes();

    for (VolumeMetadata volumeMetadata : volumeMetadatas) {
      try {
        if (!VolumeStatus.Dead.equals(volumeMetadata.getVolumeStatus())) {
          long instanceId = getInstanceFromTable(volumeMetadata.getVolumeId());
          if (instanceId == 0) {
            logger.warn(
                "when list volume, can not find the volume :{} in any instance, try it next time",
                volumeMetadata.getVolumeId(), volumeMetadata.getName());
            continue;
          }

          VolumeMetadata volume = getVolumeNew(volumeMetadata.getVolumeId(),
              Constants.SUPERADMIN_ACCOUNT_ID);
          result.add(volume);
        }
      } catch (VolumeNotFoundException e) {
        logger.warn("when list volume, can not find the volume :{}", volumeMetadata.getVolumeId());
      }
    }

    return result;
  }

  public VolumeMetadataAndDrivers getDriverContainVolumes(long volumeId, long accountId,
      boolean withSegment)
      throws VolumeNotFoundException {
    VolumeMetadataAndDrivers volumeMetadataAndDrivers = new VolumeMetadataAndDrivers();

    //get volume and Drivers
    VolumeMetadata volumeMetadata;
    if (withSegment) {
      volumeMetadata = getVolumeNew(volumeId, accountId);
    } else {
      volumeMetadata = getVolume(volumeId, accountId);
    }
    List<DriverMetadata> volumeBindingDrivers = driverStore.get(volumeId);

    volumeMetadataAndDrivers.setVolumeMetadata(volumeMetadata);
    volumeMetadataAndDrivers.setDriverMetadatas(volumeBindingDrivers);

    return volumeMetadataAndDrivers;
  }

  /**
   * for test.
   ***/
  public void saveVolume(VolumeMetadata volumeMetadata) {
    volumeStore.saveVolume(volumeMetadata);
    volumeStore.saveVolumeForReport(volumeMetadata);

    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    if (instanceToVolumeInfoMap.isEmpty()) {
      InstanceToVolumeInfo instanceToVolumeInfo = new InstanceToVolumeInfo();
      instanceToVolumeInfo.addVolume(volumeMetadata.getVolumeId(), 1L);
      instanceToVolumeInfoMap.put(appContext.getInstanceId().getId(), instanceToVolumeInfo);

    } else {
      for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
        InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();
        instanceToVolumeInfo.addVolume(volumeMetadata.getVolumeId(), 1L);
      }
    }
  }

  public void deleteVolumeForReport(long volumeId) {
    VolumeMetadata volumeMetadata = volumeStore.getVolumeForReport(volumeId);
    if (volumeMetadata != null) {
      volumeStore.deleteVolumeForReport(volumeMetadata);
    } else {
      logger.warn("for Equilibrium to delete this volume :{}, but it is not have", volumeId);
    }
  }

  public void updateVolume(long volumeId, String changeVolumeName, VolumeInAction action)
      throws VolumeNotFoundExceptionThrift, InvalidInputExceptionThrift,
      VolumeNameExistedExceptionThrift {
    logger.warn("updateVolume, the volume id : {}, the name :{}, the action :{}",
        volumeId, changeVolumeName, action);

    VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
    if (volumeMetadata == null) {
      throw new VolumeNotFoundExceptionThrift();
    }

    //todo:for move volume
    if (changeVolumeName != null && !changeVolumeName.isEmpty()) {
      if (!changeVolumeName.contains(MOVE_VOLUME_APPEND_STRING_FOR_ORIGINAL_VOLUME)
          && changeVolumeName.length() > 100) {
        logger.error("volume new name:{} is too long", changeVolumeName);
        throw new InvalidInputExceptionThrift();
      }

      // check new volume name valid
      if (volumeStore.getVolumeNotDeadByName(changeVolumeName) != null) {
        logger.error("some volume has been exist, name is {}", changeVolumeName);
        throw new VolumeNameExistedExceptionThrift();
      }
    }

    if (changeVolumeName != null && !changeVolumeName.isEmpty()) {
      volumeMetadata.setName(changeVolumeName);
      logger.warn("volume:{} change new name:{}", volumeId, changeVolumeName);
    }

    if (action != null) {
      volumeMetadata.setInAction(action);
    }

    volumeStore.saveVolume(volumeMetadata);
  }

  private long getInstanceFromTable(long volumeId) {
    long instanceIdGet = 0;
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      long instanceId = entry.getKey();
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();

      if (instanceToVolumeInfo.containsValue(volumeId)) {
        instanceIdGet = instanceId;
        logger.info("when getInstanceFromTable, the volume :{}, in instance :{}", volumeId,
            instanceId);
        break;
      }
    }

    return instanceIdGet;
  }

  /**
   * for distribute Volume to each instance.
   ***/
  public long distributeVolumeToReportUnit(long volumeId) {
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    //** check all HA have change ***/
    Instance master = haInstanceManger.getMaster();
    //the master change, so need update
    if (master == null || (master.getId().getId() != appContext.getInstanceId().getId())) {
      logger.warn(
          "when distributeVolumeToReportUnit find master change, so need to update, the master "
              + "value :{}, the current info :{}, {}",
          master, appContext.getInstanceId(), appContext.getInstanceName());
      haInstanceManger.updateInstance();
      return 0;
    }

    long masterId = master.getId().getId();
    //check report to master, first choose the master, in master or master no volumes
    if (/*volumeStore.getVolumeForReport(volumeId) != null */
        instanceToVolumeInfoMap.isEmpty()
      /* || instanceToVolumeInfoMap.get(masterId).getVolumeInfo().isEmpty() */) {
      return masterId;

    } else {

      //check which HA have the volume id
      long instanceIdFlower = getInstanceFromTable(volumeId);
      if (instanceIdFlower != 0) {
        return instanceIdFlower;
      } else {

        //choose one HA to report, first choose master
        return chooseOneHaToReport(volumeId);
      }
    }
  }


  public boolean checkCurrentVolumeInMaster(long instanceId, long volumeId) {
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = instanceIncludeVolumeInfoManger
        .getInstanceToVolumeInfoMap();
    InstanceToVolumeInfo instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceId);
    if (instanceToVolumeInfo != null && instanceToVolumeInfo.containsValue(volumeId)) {
      return true;
    }

    return false;
  }

  /**
   * get the instance which the all volume size is smallest return the HA instance id.
   ****/
  private long chooseOneHaToReport(long volumeId) {
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap = new HashMap<>(
        instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap());
    List<Instance> instanceAll = haInstanceManger.getAllInstance();

    //* 1, choose the HA which no any volume to report **/
    for (Instance instanceHa : instanceAll) {
      long instanceIdHa = instanceHa.getId().getId();
      if (!instanceToVolumeInfoMap.containsKey(instanceIdHa)) {
        return instanceIdHa;
      }
    }

    //** check, if only one instance, not choose instance  ***/
    if (instanceToVolumeInfoMap.size() == 1) {
      long returnInstanceId = instanceToVolumeInfoMap.keySet().iterator().next();
      logger.warn("there is only one instance :{}, not need to choose with volume :{}",
          returnInstanceId, volumeId);
      return returnInstanceId;
    }



    if (volumeStore.getVolume(volumeId) == null) {
      List<Long> instanceIdInfo = new LinkedList<>(instanceToVolumeInfoMap.keySet());
      Collections.shuffle(instanceIdInfo);
      long returnInstanceId = instanceIdInfo.get(0);
      logger.warn(
          "when in chooseOneHaToReport, find the volume :{}, may be lost in db, so choose by "
              + "random, the choose :{}",
          volumeId, returnInstanceId);
      return returnInstanceId;
    }

    long currentVolumeSegmentNumber = volumeStore.getVolume(volumeId).getSegmentCount();
    long masterInstanceId = appContext.getInstanceId().getId();

    InstanceToVolumeInfo masterInstanceToVolumeInfo = instanceToVolumeInfoMap.get(masterInstanceId);
    masterInstanceToVolumeInfo.setVolumeStore(volumeStore);
    long masterAllSegmentSize = masterInstanceToVolumeInfo.getTotalVolumeSize().getSecond();

    //check the master segment is large or not,
    long masterCanSaveSegmentNumber = (long) (allSegmentNumberToSave
        * percentNumberSegmentNumberMasterSave);
    if (masterAllSegmentSize > masterCanSaveSegmentNumber
        || masterAllSegmentSize + currentVolumeSegmentNumber > masterCanSaveSegmentNumber) {
      logger.warn(
          "when distribute the volume :{} for report, the master save the segment :{}, the limit "
              + ":{}",
          volumeId, masterAllSegmentSize, masterCanSaveSegmentNumber);
      //remove master in choose
      instanceToVolumeInfoMap.remove(masterInstanceId);
    }

    //check the follower segment number
    Iterator<Long> iterator = instanceToVolumeInfoMap.keySet().iterator();
    while (iterator.hasNext()) {
      long key = iterator.next();
      InstanceToVolumeInfo instanceToVolumeInfo = instanceToVolumeInfoMap.get(key);
      instanceToVolumeInfo.setVolumeStore(volumeStore);
      long allSegmentSize = instanceToVolumeInfo.getTotalVolumeSize().getSecond();

      if (allSegmentSize > allSegmentNumberToSave
          || allSegmentSize + currentVolumeSegmentNumber > allSegmentNumberToSave) {

        if (instanceToVolumeInfoMap.size() > 1) {
          iterator.remove();
          logger.warn(
              "when distribute the volume :{} for report, the follower current segment number :{}"
                  + " the want save number :{}, the limit :{}",
              volumeId, allSegmentSize, currentVolumeSegmentNumber, allSegmentNumberToSave);
        } else {
          logger.warn(
              "when distribute the volume :{} for report, the follower current segment number :{}"
                  + " the want save number :{}, "

                  + "the limit :{}, but must have one instance to save volume, so choose the last "
                  + "one",
              volumeId, allSegmentSize, currentVolumeSegmentNumber, allSegmentNumberToSave);
          break;
        }
      }
    }


    long chooseInstanceId = 0;
    long smallSize = 0;
    boolean check = true; //just for first time
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      long chooseInstanceIdTemp = entry.getKey();
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();
      instanceToVolumeInfo.setVolumeStore(volumeStore);
      long smallTotalVolumeSizeTemp = instanceToVolumeInfo.getTotalVolumeSize().getFirst();

      logger.info(
          "when chooseOneHaToReport, the chooseInstanceIdTemp :{}, the smallTotalVolumeSizeTemp "
              + ":{}",
          chooseInstanceIdTemp, smallTotalVolumeSizeTemp);

      if (check) {
        smallSize = smallTotalVolumeSizeTemp;
        chooseInstanceId = chooseInstanceIdTemp;
        check = false;
      } else {

        if (smallTotalVolumeSizeTemp < smallSize) {
          chooseInstanceId = chooseInstanceIdTemp;
        }
      }
    }

    return chooseInstanceId;
  }


  public boolean checkLastExtendedTimeChange(VolumeMetadata volumeInDb,
      VolumeMetadata volumeFromGet) {
    if (volumeInDb.getLastExtendedTime() != null
        && !volumeInDb.getLastExtendedTime().equals(volumeFromGet.getLastExtendedTime())) {
      return true;
    }

    return false;
  }

  public void addVolumeToDeleteForEquilibrium(Set<Long> volumeToDeleteForEquilibrium) {
    this.volumeToDeleteForEquilibrium.addAll(volumeToDeleteForEquilibrium);
  }

  public void updateToHaInstanceManger() {
    haInstanceManger.updateInstance();
  }

  public InformationCenterClientFactory getInfoCenterClientFactory() {
    return infoCenterClientFactory;
  }

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  public HaInstanceManger getHaInstanceManger() {
    return haInstanceManger;
  }

  public void setHaInstanceManger(HaInstanceManger haInstanceManger) {
    this.haInstanceManger = haInstanceManger;
  }

  public InstanceIncludeVolumeInfoManger getInstanceIncludeVolumeInfoManger() {
    return instanceIncludeVolumeInfoManger;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public Set<Long> getVolumeToDeleteForEquilibrium() {
    return volumeToDeleteForEquilibrium;
  }

  public void setVolumeToDeleteForEquilibrium(Set<Long> volumeToDeleteForEquilibrium) {
    this.volumeToDeleteForEquilibrium = volumeToDeleteForEquilibrium;
  }

  public void setAppContext(InfoCenterAppContext appContext) {
    this.appContext = appContext;
  }

  public long getAllSegmentNumberToSave() {
    return allSegmentNumberToSave;
  }

  public void setAllSegmentNumberToSave(long allSegmentNumberToSave) {
    this.allSegmentNumberToSave = allSegmentNumberToSave;
  }

  public double getPercentNumberSegmentNumberMasterSave() {
    return percentNumberSegmentNumberMasterSave;
  }

  public void setPercentNumberSegmentNumberMasterSave(double percentNumberSegmentNumberMasterSave) {
    this.percentNumberSegmentNumberMasterSave = percentNumberSegmentNumberMasterSave;
  }
}
