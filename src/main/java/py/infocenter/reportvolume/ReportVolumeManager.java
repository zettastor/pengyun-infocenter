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

package py.infocenter.reportvolume;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.struct.Pair;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePoolStore;
import py.thrift.infocenter.service.ReportVolumeInfoRequest;
import py.thrift.infocenter.service.ReportVolumeInfoResponse;
import py.thrift.share.ReadWriteTypeThrift;
import py.thrift.share.VolumeInActionThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeStatusThrift;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

public class ReportVolumeManager {

  private static final Logger logger = LoggerFactory.getLogger(ReportVolumeManager.class);
  private VolumeStore volumeStore;
  private StoragePoolStore storagePoolStore;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;
  private VolumeRecycleStore volumeRecycleStore;

  public ReportVolumeManager() {
  }


  /**
   * master save the volume info which report from the follower HA current instance must master.
   ***/
  public ReportVolumeInfoResponse reportVolumeInfo(ReportVolumeInfoRequest request) {

    try {
      String instanceName = request.getInstanceName();
      long reportInstanceId = request.getInstanceId();
      logger.info("reportVolumeInfo form :{}, request :{}", instanceName, request);

      List<VolumeMetadataThrift> volumeMetadataThriftList = request.getVolumeMetadatas();
      Map<Long, Long> totalSegmentUnitMetadataNumber = request.getTotalSegmentUnitMetadataNumber();

      /* for the Dead volume, not care the dead volume **/
      Iterator iterator = volumeMetadataThriftList.iterator();
      while (iterator.hasNext()) {
        VolumeMetadataThrift volumeMetadataThrift = (VolumeMetadataThrift) iterator.next();
        //the volume in report is dead
        if (volumeMetadataThrift.getVolumeStatus().equals(VolumeStatusThrift.Dead)) {
          VolumeMetadata volumeMetadataInDb = volumeStore
              .getVolume(volumeMetadataThrift.getVolumeId());

          //the volume in db is dead
          if (volumeMetadataInDb != null && volumeMetadataInDb.getVolumeStatus()
              .equals(VolumeStatus.Dead)) {
            volumeRecycleStore.deleteVolumeRecycleInfo(volumeMetadataThrift.getVolumeId());
            iterator.remove();
          }
        }
      }

      logger.warn("get reportVolumeInfo request form :{} {}, have volumes:{} ", instanceName,
          reportInstanceId,
          volumeMetadataThriftList.stream().map(VolumeMetadataThrift::getName)
              .collect(Collectors.toSet()));

      ReportVolumeInfoResponse response = new ReportVolumeInfoResponse();
      response.setRequestId(request.getRequestId());

      List<Long> volumeIdListForReport = new ArrayList<>();
      //if the volume EquilibriumO ok, make it
      Map<Long, List<Long>> removeVolumeInInfoTableWhenEquilibriumOk = new HashMap<>();

      if (volumeMetadataThriftList.isEmpty() && request.getVolumeMetadatasForDelete().isEmpty()) {
        //just for make the instance last report time
        instanceIncludeVolumeInfoManger
            .saveVolumeInfo(reportInstanceId, volumeIdListForReport,
                removeVolumeInInfoTableWhenEquilibriumOk,
                totalSegmentUnitMetadataNumber);
        logger.warn("send reportVolumeInfo response to :{} {}, value is :{}", instanceName,
            reportInstanceId, response);
        return response;
      }

      /* just for log ***/
      List<ReportVolumeInformation> reportVolumeInformationList = new ArrayList<>();
      volumeMetadataThriftList.forEach((v) -> reportVolumeInformationList
          .add(new ReportVolumeInformation(v.getVolumeId(), v.getVolumeSize(),
              v.getExtendingSize(), v.getName(), v.getVolumeStatus(), v.getInAction(),
              v.getReadWrite(), v.getCsiLaunchCount(), v.getFormatStep())));
      logger.warn("reportVolumeInfo form :{} {}, volume info :{}", instanceName, reportInstanceId,
          reportVolumeInformationList);

      //set follower not report this volume next time
      Set<Long> notifyThisInstanceNotRepotThisVolumeWhenEquilibriumOk = new HashSet<>();
      List<VolumeMetadata> volumeInfoListForReport = new ArrayList<>();
      List<VolumeMetadataThrift> volumeMetadataThriftListForDelete = request
          .getVolumeMetadatasForDelete();

      for (VolumeMetadataThrift volumeMetadataThrift : volumeMetadataThriftList) {

        /* check volume if it is in Equilibrium */
        boolean needSaveToVolumeInfoTable = true;

        long volumeId = volumeMetadataThrift.getVolumeId();
        VolumeStatusThrift volumeStatusThrift = volumeMetadataThrift.getVolumeStatus();

        /* check the Equilibrium volume task is run or not, check volume if it is in
         * Equilibrium **/

        //check the volume report form which instance when Equilibrium
        if (instanceVolumeInEquilibriumManger
            .getVolumeReportToInstanceEquilibriumBuildWithVolumeId().isEmpty()
            && instanceVolumeInEquilibriumManger.getEquilibriumOkVolume().isEmpty()) {
          //the Equilibrium task not run or
          needSaveToVolumeInfoTable = true;
        } else {
          Pair<Boolean, Long> volumeFormNewInstance = instanceVolumeInEquilibriumManger
              .checkVolumeFormNewInstanceEquilibrium(volumeId, reportInstanceId);

          //volume in Equilibrium or not
          boolean volumeEquilibriumInToInstance = volumeFormNewInstance.getFirst();

          //form instance id
          long oldInstanceId = volumeFormNewInstance.getSecond();
          List<Long> volumesEquilibriumOk = new ArrayList<>();

          //the volume in in Equilibrium which form new instance, is form To(new) instance
          if (volumeEquilibriumInToInstance) { //Equilibrium volume report form To(new) instance
            if (volumeStatusThrift.equals(VolumeStatusThrift.Available)
                || volumeStatusThrift.equals(VolumeStatusThrift.Stable)) {
              // only Available, can remove volume in old table
              //remove the volume in oldInstance table
              if (removeVolumeInInfoTableWhenEquilibriumOk.get(oldInstanceId) != null) {
                volumesEquilibriumOk = removeVolumeInInfoTableWhenEquilibriumOk.get(oldInstanceId);
              } else {
                removeVolumeInInfoTableWhenEquilibriumOk.put(oldInstanceId, volumesEquilibriumOk);
              }

              //move volume in Equilibrium table
              instanceVolumeInEquilibriumManger
                  .removeVolumeInEquilibriumTable(oldInstanceId, volumeId);

              //save ok in Equilibrium table
              volumesEquilibriumOk.add(volumeId);
              instanceVolumeInEquilibriumManger.saveEquilibriumOkVolume(volumeId, oldInstanceId);

              logger.warn(
                  "when Equilibrium, the volume:{} report by To instance :{},and it is "
                      + "Equilibrium ok",
                  volumeId, reportInstanceId);
            } else {
              needSaveToVolumeInfoTable = false;
              logger.warn(
                  "when Equilibrium, the volume:{} is Equilibrium not ok, and it report by To "
                      + "instance :{}"

                      + "the status is", volumeId, volumeStatusThrift, reportInstanceId);
            }

          } else {
            long instanceFrom = instanceVolumeInEquilibriumManger
                .checkVolumeEquilibriumOk(volumeId);
            if (instanceFrom != 0
                && reportInstanceId == instanceFrom) { //remove the volume in instanceFrom
              needSaveToVolumeInfoTable = false;

              //set info to old instance, delete this volume and not report it again, set in 
              // response
              notifyThisInstanceNotRepotThisVolumeWhenEquilibriumOk.add(volumeId);

              logger.warn(
                  "when in Equilibrium, the volume :{} report by form instance :{},and it is "
                      + "Equilibrium ok, "

                      + "and set it not report to current instance next time", volumeId,
                  reportInstanceId);
              //move volume in Equilibrium table

              //volume has change in volume info table, so not delete it again
            } else {
              if (oldInstanceId != 0) {
                logger.warn("current volume :{} is in Equilibriuming, which in form instance :{}",
                    volumeId, oldInstanceId);
              } else {

                //the volume not in Equilibrium
              }
            }
          } //end with the form instance
        } //end check current volume in Equilibrium ok or not

        // check the Equilibrium volume
        if (needSaveToVolumeInfoTable) {
          VolumeMetadata volumeMetadata = RequestResponseHelper
              .buildVolumeFrom(volumeMetadataThrift);
          volumeInfoListForReport.add(volumeMetadata);

          //save the value instance <--> volume id, for rebuild the map when master down
          //when Equilibrium volume, the Equilibrium volume still report

          volumeIdListForReport.add(volumeMetadata.getVolumeId());
        }
      }

      // rebuild the instance <--> volume id table for the new master
      final Set<Long> volumeExistInTable = instanceIncludeVolumeInfoManger
          .saveVolumeInfo(reportInstanceId, volumeIdListForReport,
              removeVolumeInInfoTableWhenEquilibriumOk, totalSegmentUnitMetadataNumber);

      /* the volume what to delete, so delete in volume and storagePoolStore db **/
      for (VolumeMetadataThrift volumeMetadataThrift : volumeMetadataThriftListForDelete) {
        VolumeMetadata volumeMetadataToDelete = RequestResponseHelper
            .buildVolumeFrom(volumeMetadataThrift);
        try {
          logger.warn("for reportVolumeInfo, to delete this volume :{} ",
              volumeMetadataToDelete.getVolumeId());
          storagePoolStore.deleteVolumeId(volumeMetadataToDelete.getStoragePoolId(),
              volumeMetadataToDelete.getVolumeId());
          volumeStore.deleteVolume(volumeMetadataToDelete);

          //delete volume info in instance volume info
          instanceIncludeVolumeInfoManger
              .removeVolumeInfo(reportInstanceId, volumeMetadataToDelete.getVolumeId());

          //This volume is dead and remove in db, tell the report instance remove it in deleteTable
          notifyThisInstanceNotRepotThisVolumeWhenEquilibriumOk
              .add(volumeMetadataThrift.getVolumeId());
        } catch (SQLException | IOException e) {
          logger.error(
              "when reportVolumeInfo,the volume delete, so delete volume in db, find exception:",
              e);
        }
      }

      /* follower update volume to db(master), update report volume to db **/
      List<VolumeMetadata> volumesInDb = volumeStore.listVolumes();
      List<VolumeMetadata> volumesInReportButNotInDb = new ArrayList<>();
      List<Long> volumesIdInDb = volumesInDb.stream().map(v -> v.getVolumeId())
          .collect(Collectors.toList());

      //the volume in master change, deleting or action, volume name change
      List<VolumeMetadata> volumeChangeList = new ArrayList<>();

      //check the report volume store in db or not, if not, save to db
      for (VolumeMetadata volumeInReport : volumeInfoListForReport) {
        if (!volumesIdInDb.contains(volumeInReport.getVolumeId())) {
          volumesInReportButNotInDb.add(volumeInReport);
        }
      }

      /* save the volume to db **/
      for (VolumeMetadata volumeInReportButNotInDb : volumesInReportButNotInDb) {
        logger.warn("found a volume in report but not in db , may be the db has been delete :{}",
            volumeInReportButNotInDb);
        volumeStore.saveVolume(volumeInReportButNotInDb);
      }

      //check the report volume which in db
      volumeInfoListForReport.removeAll(volumesInReportButNotInDb);

      for (VolumeMetadata volumeInReport : volumeInfoListForReport) {
        long volumeId = volumeInReport.getVolumeId();
        LockForSaveVolumeInfo.VolumeLockInfo lock = null;

        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
        } catch (Exception e) {
          logger.warn("the volume is work by :{}", e);
          continue;
        }

        try {
          VolumeMetadata volumeInDb = volumeStore.getVolume(volumeId);
          VolumeStatus masterVolumeStatus = volumeInDb.getVolumeStatus();
          VolumeStatus reportVolumeStatus = volumeInReport.getVolumeStatus();
          boolean volumeIsChangeInMaster = false;
          /* 1. check status, follower can not set status when the master volume is deleting
           *     the user Deleting one volume
           */
          if ((masterVolumeStatus == VolumeStatus.Deleting)
              && (reportVolumeStatus != VolumeStatus.Deleting
              && reportVolumeStatus != VolumeStatus.Deleted
              && reportVolumeStatus != VolumeStatus.Dead)) {

            logger.warn(
                "in reportVolumeInfo find volume delete by user, the volume :{} {}, the master "
                    + "status is :{}, "

                    + "the report status is :{}, so change to follower", volumeInDb.getName(),
                volumeId,
                masterVolumeStatus, reportVolumeStatus);
            volumeIsChangeInMaster = true;
            volumeInReport.setVolumeStatus(masterVolumeStatus);
          }

          /* 2. check status, follower can not set status when the master volume is Recycling
           *     the user Recycling one volume
           */
          if ((masterVolumeStatus == VolumeStatus.Recycling)

              && (reportVolumeStatus == VolumeStatus.Deleting
              || reportVolumeStatus == VolumeStatus.Deleted)) {

            logger.warn(
                "find volume Recycling by user, the volume :{} {}, the master status is :{}, the "
                    + "report status is :{}, "

                    + "so change to follower", volumeInDb.getName(), volumeId, masterVolumeStatus,
                reportVolumeStatus);
            //if status Deleting or Recycling, use the master status
            volumeIsChangeInMaster = true;
            volumeInReport.setVolumeStatus(masterVolumeStatus);
          }

          /* 3. check action, follower can not change action, only the master change action
           *     if different, use the master action
           ***/
          VolumeInAction masterVolumeAction = volumeInDb.getInAction();
          VolumeInAction reportVolumeAction = volumeInReport.getInAction();

          if (!masterVolumeAction.equals(reportVolumeAction)) {
            volumeIsChangeInMaster = true;
            logger.warn(
                "find volume action change, the volume :{} {}, the master action is :{}, the "
                    + "report action is :{}, "

                    + "so change to follower", volumeInDb.getName(), volumeId, masterVolumeAction,
                reportVolumeAction);
            volumeInReport.setInAction(masterVolumeAction);
          }

          /* 4 . check volume name , when move volume or other, the master change the volume name
           *      if different, use the master name
           ***/
          if (!volumeInDb.getName().equals(volumeInReport.getName())) {
            volumeIsChangeInMaster = true;
            logger.warn(
                "find volume name change, the volume :{}, the master name is :{}, the report name"
                    + " is :{}, so change to follower",
                volumeId, volumeInDb.getName(), volumeInReport.getName());
            volumeInReport.setName(volumeInDb.getName());
          }

          /* 5 . check volume readAndWrite , the console change the volume readAndWrite
           *      if different, use the master value
           ***/
          if (!volumeInDb.getReadWrite().equals(volumeInReport.getReadWrite())) {
            volumeIsChangeInMaster = true;
            logger.warn(
                "find volume readAndWrite change, the volume :{} {}, the master readAndWrite is "
                    + ":{},"

                    + " the report readAndWrite is :{}, so change to follower",
                volumeInDb.getName(),
                volumeId,
                volumeInDb.getReadWrite(), volumeInReport.getReadWrite());
            volumeInReport.setReadWrite(volumeInDb.getReadWrite());
          }

          /* 6 . check volume ExtendingSize , when extend the volume, the ExtendingSize change
           *      if different and the volume size is equals, the master value
           ***/
          if (volumeInDb.getExtendingSize() != volumeInReport.getExtendingSize()
              && volumeInDb.getVolumeSize() == volumeInReport.getVolumeSize()) {
            logger.warn(
                "find volume ExtendingSize change, the volume :{} {}, the master ExtendingSize is"
                    + " :{}, "

                    + "LastExtendedTime :{}, the report ExtendingSize is :{}, LastExtendedTime :{}"

                    + "so change to follower", volumeInDb.getName(), volumeId,
                volumeInDb.getExtendingSize(), volumeInDb.getLastExtendedTime(),
                volumeInReport.getExtendingSize(), volumeInReport.getLastExtendedTime());
            volumeIsChangeInMaster = true;
            volumeInReport.setExtendingSize(volumeInDb.getExtendingSize());
            volumeInReport.setLastExtendedTime(volumeInDb.getLastExtendedTime());
          }

          /* 7 . check volume MarkDelete , the inforcenter change the volume MarkDelete
           *      if different, use the master value
           ***/

          //change the mark delete info
          if (!new Boolean(volumeInDb.isMarkDelete())
              .equals(new Boolean(volumeInReport.isMarkDelete()))) {
            logger.warn(
                "find volume isMarkDelete change, the volume :{} {}, the master isMarkDelete is "
                    + ":{}, "

                    + "the report isMarkDelete is :{}, so change to follower", volumeInDb.getName(),
                volumeId, volumeInDb.isMarkDelete(), volumeInReport.isMarkDelete());
            volumeInReport.setMarkDelete(volumeInDb.isMarkDelete());
          }

          if (volumeInDb.getVolumeDescription() != null && !volumeInDb.getVolumeDescription()
              .equals(volumeInReport.getVolumeDescription())) {
            volumeIsChangeInMaster = true;
            logger.warn(
                "find volume volumeDescription change, the volume :{}, the master "
                    + "volumeDescription is :{}, the volumeDescription name is :{}, "

                    + "so change to follower",
                volumeId, volumeInDb.getVolumeDescription(), volumeInReport.getVolumeDescription());
            volumeInReport.setVolumeDescription(volumeInDb.getVolumeDescription());
          }

          /* this info only change in master, the Rebalance task do in master ***/
          logger.info("get the getRebalanceInfo :{}", volumeInDb.getRebalanceInfo());
          volumeInReport.setRebalanceInfo(volumeInDb.getRebalanceInfo());
          volumeInReport.setStableTime(volumeInDb.getStableTime());
          //
          volumeInReport.setClientLastConnectTime(volumeInDb.getClientLastConnectTime());

          volumeStore.saveVolume(volumeInReport);
          if (volumeIsChangeInMaster) {
            volumeChangeList.add(volumeInReport);
          }
        } finally {
          lock.setVolumeOperation(LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
          lock.unlock();
        }
      }

      /* master set info to follower, about:
       *  1. delete volume
       *  2. Recycling volume
       *  3. update some volume info :name, action,ReadWriteType and so on
       *  4. volumesToMove,
       *  5. Extending volume
       *
       * */
      //the volumes change in master
      List<VolumeMetadataThrift> volumeMetadataListToFlower = new ArrayList<>(
          volumeChangeList.size());
      for (VolumeMetadata volumeMetadata : volumeChangeList) {
        volumeMetadataListToFlower
            .add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
      }

      /* set the move volume to this instance, and notify it not report this volume again ***/
      notifyThisInstanceNotRepotThisVolumeWhenEquilibriumOk.addAll(volumeExistInTable);

      response.setVolumeMetadatasChangeInMaster(volumeMetadataListToFlower);
      response.setNotReportThisVolume(notifyThisInstanceNotRepotThisVolumeWhenEquilibriumOk);
      if (response.getVolumeMetadatasChangeInMasterSize() != 0
          || response.getNotReportThisVolumeSize() != 0) {
        logger.info("send reportVolumeInfo response to :{} {}, value is :{}", instanceName,
            reportInstanceId, response);
      }
      logger.warn("send reportVolumeInfo response to :{} {}, value is :{}", instanceName,
          reportInstanceId, response);
      return response;
    } finally {
      logger.info("nothing need to do here");
    }
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public InstanceVolumeInEquilibriumManger getInstanceVolumeInEquilibriumManger() {
    return instanceVolumeInEquilibriumManger;
  }

  public void setInstanceVolumeInEquilibriumManger(
      InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger) {
    this.instanceVolumeInEquilibriumManger = instanceVolumeInEquilibriumManger;
  }

  public InstanceIncludeVolumeInfoManger getInstanceIncludeVolumeInfoManger() {
    return instanceIncludeVolumeInfoManger;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public LockForSaveVolumeInfo getLockForSaveVolumeInfo() {
    return lockForSaveVolumeInfo;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }

  public void setVolumeRecycleStore(VolumeRecycleStore volumeRecycleStore) {
    this.volumeRecycleStore = volumeRecycleStore;
  }

  /**
   * just for log.
   ***/
  class ReportVolumeInformation {

    private String name;
    private long volumeId;
    private VolumeStatusThrift volumeStatus;
    private VolumeInActionThrift inAction;
    private ReadWriteTypeThrift readWrite;
    private long volumeSize;
    private long extendingSize;
    private int csiLaunchCount;
    private int formatStep;

    public ReportVolumeInformation(long volumeId, long volumeSize, long extendingSize, String name,
        VolumeStatusThrift volumeStatus, VolumeInActionThrift inAction,
        ReadWriteTypeThrift readWrite,
        int csiLaunchCount, int formatStep) {
      this.volumeId = volumeId;
      this.volumeSize = volumeSize;
      this.extendingSize = extendingSize;
      this.name = name;
      this.volumeStatus = volumeStatus;
      this.inAction = inAction;
      this.readWrite = readWrite;
      this.csiLaunchCount = csiLaunchCount;
      this.formatStep = formatStep;
    }

    @Override
    public String toString() {
      return "ReportVolumeInformation{"

          + "name='" + name + '\''

          + ", volumeId=" + volumeId

          + ", volumeStatus=" + volumeStatus

          + ", inAction=" + inAction

          + ", readWrite=" + readWrite

          + ", volumeSize=" + volumeSize

          + ", extendingSize=" + extendingSize

          + ", csiLaunchCount=" + csiLaunchCount

          + ", formatStep=" + formatStep

          + '}';
    }
  }
}
