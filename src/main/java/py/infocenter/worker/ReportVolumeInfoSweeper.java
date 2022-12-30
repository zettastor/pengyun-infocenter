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

package py.infocenter.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.exception.GenericThriftClientFactoryException;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.reportvolume.ReportVolumeManager;
import py.infocenter.store.VolumeStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.periodic.Worker;
import py.thrift.infocenter.service.ReportVolumeInfoRequest;
import py.thrift.infocenter.service.ReportVolumeInfoResponse;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.VolumeMetadata;

/**
 * report volume info to master HA.
 */
public class ReportVolumeInfoSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(ReportVolumeInfoSweeper.class);
  private VolumeStore volumeStore;
  private InformationCenterClientFactory infoCenterClientFactory;
  private AppContext appContext;
  private VolumeInformationManger volumeInformationManger;
  private HaInstanceManger haInstanceManger;
  private ReportVolumeManager reportVolumeManager;

  //       volumeId, the time count
  private Map<Long, Long> timeToDeleteVolumeForEquilibrium = new ConcurrentHashMap<>();
  private InformationCenterClientWrapper informationCenterClientWrapper = null;

  @Override
  public void doWork() {
    if (appContext.getStatus() != InstanceStatus.HEALTHY
        && appContext.getStatus() != InstanceStatus.SUSPEND) {
      logger.info("only the master and follower do it");
      return;
    }

    //check to delete volume for Equilibrium ok
    deleteVolumeForEquilibrium();

    haInstanceManger = volumeInformationManger.getHaInstanceManger();
    Instance master = haInstanceManger.getMaster();

    if (master == null) {
      logger.warn("can not find the master, the all ok instance is empty, report volume next time");
      haInstanceManger.updateInstance();
      return;
    }

    //check the master
    if (appContext.getStatus() == InstanceStatus.HEALTHY) {
      if (master.getId().getId() != appContext.getInstanceId().getId()) {
        logger.warn(
            "find the master id change, the memory :{}, the dih get :{}, {} so need update master"
                + " info, report volume next time",
            master, appContext.getInstanceId(), appContext.getInstanceName());
        haInstanceManger.updateInstance();
        return;
      }
    }

    long currentInstanceIdToReport = 0;
    EndPoint endPoint = master.getEndPoint();
    InstanceId masterInstanceId = master.getId();
    currentInstanceIdToReport = masterInstanceId.getId();

    /*  just report the volume below to this HA *****/
    List<VolumeMetadata> volumesReport = volumeStore.listVolumesForReport();
    List<Pair<VolumeMetadata, Long>> volumesReportMerge = new ArrayList<>();

    //check, if the volume will delete (this volume report by more instance, so delete it in 
    // current instance), not report
    Set<Long> toDeleteVolumes = volumeInformationManger.getVolumeToDeleteForEquilibrium();
    Iterator iterator = volumesReport.iterator();
    while (iterator.hasNext()) {
      long volumeId = ((VolumeMetadata) iterator.next()).getVolumeId();
      if (toDeleteVolumes.contains(volumeId)) {
        iterator.remove();
      }
    }

    //merge info
    for (VolumeMetadata volumeMetadata : volumesReport) {
      volumesReportMerge.add(volumeInformationManger.mergeVolumeInfo(volumeMetadata));
    }

    /* make report request ****/
    List<VolumeMetadataThrift> volumeMetadataThriftList = new ArrayList<>(
        volumesReportMerge.size());
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>(volumesReportMerge.size());
    for (Pair<VolumeMetadata, Long> value : volumesReportMerge) {
      VolumeMetadata volumeMetadata = value.getFirst();
      long number = value.getSecond();

      volumeMetadataThriftList
          .add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
      totalSegmentUnitMetadataNumberMap.put(volumeMetadata.getVolumeId(), number);
    }

    List<VolumeMetadata> volumesToDelete = volumeStore.listVolumesWhichToDeleteForReport();
    List<VolumeMetadataThrift> volumeMetadataThriftListToDelete = new ArrayList<>(
        volumesToDelete.size());
    for (VolumeMetadata volumeMetadata : volumesToDelete) {
      volumeMetadataThriftListToDelete
          .add(RequestResponseHelper.buildThriftVolumeFrom(volumeMetadata, false));
    }

    ReportVolumeInfoRequest request = new ReportVolumeInfoRequest(RequestIdBuilder.get(),
        currentInstanceIdToReport, appContext.getMainEndPoint().getHostName(),
        volumeMetadataThriftList,
        volumeMetadataThriftListToDelete, totalSegmentUnitMetadataNumberMap);

    ReportVolumeInfoResponse response = null;
    if (appContext.getStatus() == InstanceStatus.SUSPEND) {
      InstanceId followerInstanceId = appContext.getInstanceId();

      String followerHostName = appContext.getMainEndPoint().getHostName();
      String masterHostName = master.getEndPoint().getHostName();
      if (followerHostName.equals(masterHostName)) {
        logger.warn(
            "I am follower :{} {}, the master :{} {}, i can not report to myself, i just report "
                + "to master",
            followerInstanceId.getId(), appContext.getMainEndPoint().getHostName(),
            masterInstanceId.getId(), master.getEndPoint().getHostName());
        haInstanceManger.updateInstance();
        return;
      }

      //set the report instance id
      request.setInstanceId(followerInstanceId.getId());
      logger.info(
          "I am follower :{}, i am report to master :{} {}, and the size :{}, {}, volumesReport: "
              + "{}, volumesToDelete: {}",
          followerInstanceId.getId(), masterInstanceId.getId(), master.getEndPoint().getHostName(),
          volumesReportMerge.size(),
          volumesToDelete.size(), volumesReportMerge, volumesToDelete);

      List<Long> reportVolumeId = new ArrayList<>();
      for (Pair<VolumeMetadata, Long> value : volumesReportMerge) {
        reportVolumeId.add(value.getFirst().getVolumeId());
      }

      List<Long> reportVolumeIdWhichToDelete = new ArrayList<>();
      for (VolumeMetadata volumeMetadata : volumesToDelete) {
        reportVolumeIdWhichToDelete.add(volumeMetadata.getVolumeId());
      }

      logger.warn(
          "I am follower :{} {}, i am report to master :{} {}, reportVolumeId :{}, "
              + "reportVolumeIdWhichToDelete: {}",
          followerInstanceId.getId(), appContext.getMainEndPoint().getHostName(),
          masterInstanceId.getId(), master.getEndPoint().getHostName(), reportVolumeId,
          reportVolumeIdWhichToDelete);

      try {
        informationCenterClientWrapper = infoCenterClientFactory.build(endPoint);
        response = informationCenterClientWrapper.reportVolumeInfo(request);
      } catch (GenericThriftClientFactoryException e) {
        logger.warn("when reportVolumeInfo may be the master : {} {} is down, the exception :",
            masterInstanceId.getId(), endPoint, e);
        haInstanceManger.updateInstance();
        return;
      } catch (TException e) {
        logger.warn("when reportVolumeInfo may be the master : {} {} is down, the exception :",
            masterInstanceId.getId(), endPoint, e);
        haInstanceManger.updateInstance();
        return;
      }

    } else {
      //the master report to myself
      response = reportVolumeManager.reportVolumeInfo(request);
    }

    //check response
    if ((response.isSetVolumeMetadatasChangeInMaster() && !response
        .getVolumeMetadatasChangeInMaster().isEmpty())
        || (response.isSetNotReportThisVolume() && !response.getNotReportThisVolume().isEmpty())) {
      logger.warn("when report, get the response value :{}", response);
    } else {
      return;
    }

    //update volume to follower memory
    List<VolumeMetadataThrift> volumeMetadataThriftList1 = response
        .getVolumeMetadatasChangeInMaster();
    if (!volumeMetadataThriftList1.isEmpty()) {
      for (VolumeMetadataThrift volumeMetadataThrift : volumeMetadataThriftList1) {
        long volumeId = volumeMetadataThrift.getVolumeId();
        VolumeMetadata volumeMetadataChangeFromMaster = RequestResponseHelper
            .buildVolumeFrom(volumeMetadataThrift);
        VolumeMetadata volumeMetadataLocal = volumeStore.getVolumeForReport(volumeId);

        /* find the change ***/
        //change VolumeStatus
        if (!volumeMetadataChangeFromMaster.getVolumeStatus()
            .equals(volumeMetadataLocal.getVolumeStatus())) {
          logger.warn(
              "for the report response,find volume status change, the volume :{} {}, the master "
                  + "status is :{}, "

                  + "i report status is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.getVolumeStatus(),
              volumeMetadataLocal.getVolumeStatus());
          volumeMetadataLocal.setVolumeStatus(volumeMetadataChangeFromMaster.getVolumeStatus());
        }

        //change action
        if (!volumeMetadataChangeFromMaster.getInAction()
            .equals(volumeMetadataLocal.getInAction())) {
          logger.warn(
              "for the report response,find volume action change, the volume :{} {}, the master "
                  + "action is :{}, "

                  + "i report action is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.getInAction(),
              volumeMetadataLocal.getInAction());
          volumeMetadataLocal.setInAction(volumeMetadataChangeFromMaster.getInAction());
        }

        //chang name
        if (!volumeMetadataChangeFromMaster.getName().equals(volumeMetadataLocal.getName())) {
          logger.warn(
              "for the report response,find volume name change, the volume :{}, the master name "
                  + "is :{}, i report name is :{},"

                  + "so change the value from response", volumeId,
              volumeMetadataChangeFromMaster.getName(),
              volumeMetadataLocal.getName());
          volumeMetadataLocal.setName(volumeMetadataChangeFromMaster.getName());
        }

        //change read and write
        if (!volumeMetadataChangeFromMaster.getReadWrite()
            .equals(volumeMetadataLocal.getReadWrite())) {
          logger.warn(
              "for the report response,find volume ReadWrite change, the volume :{} {}, the "
                  + "master ReadWrite is :{}, "

                  + "i report ReadWrite is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.getReadWrite(),
              volumeMetadataLocal.getReadWrite());
          volumeMetadataLocal.setReadWrite(volumeMetadataChangeFromMaster.getReadWrite());
        }

        //change ExtendingSize and volume size for extend volume
        if (volumeMetadataChangeFromMaster.getExtendingSize() != volumeMetadataLocal
            .getExtendingSize()) {
          logger.warn(
              "for the report response,find volume ExtendingSize change, the volume :{} {}, the "
                  + "master ExtendingSize is :{}, "

                  + "i report ExtendingSize is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.getExtendingSize(),
              volumeMetadataLocal.getExtendingSize());
          volumeMetadataLocal.setExtendingSize(volumeMetadataChangeFromMaster.getExtendingSize());
        }

        //change ExtendingSize and volume size for extend volume
        if (checkLastExtendedTimeChange(volumeMetadataChangeFromMaster, volumeMetadataLocal)) {
          logger.warn(
              "for the report response,find volume LastExtendedTime change, the volume :{} {}, "
                  + "the master LastExtendedTime is :{}, "

                  + "i report LastExtendedTime is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.getLastExtendedTime(),
              volumeMetadataLocal.getLastExtendedTime());
          volumeMetadataLocal
              .setLastExtendedTime(volumeMetadataChangeFromMaster.getLastExtendedTime());
        }

        //change the mark delete info
        if (!new Boolean(volumeMetadataChangeFromMaster.isMarkDelete())
            .equals(new Boolean(volumeMetadataLocal.isMarkDelete()))) {
          logger.warn(
              "for the report response,find volume isMarkDelete change, the volume :{} {}, the "
                  + "master isMarkDelete is :{}, "

                  + "i report isMarkDelete is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.isMarkDelete(),
              volumeMetadataLocal.isMarkDelete());
          volumeMetadataLocal.setMarkDelete(volumeMetadataChangeFromMaster.isMarkDelete());
        }

        if (volumeMetadataChangeFromMaster.getVolumeDescription() != null
            && !volumeMetadataChangeFromMaster.getVolumeDescription()
            .equals(volumeMetadataLocal.getVolumeDescription())) {
          logger.warn(
              "for the report response,find volume description change, the volume :{} {}, the "
                  + "master description is :{}, "

                  + "i report description is :{}, so change the value from response",
              volumeMetadataLocal.getName(),
              volumeId, volumeMetadataChangeFromMaster.getVolumeDescription(),
              volumeMetadataLocal.getVolumeDescription());
          volumeMetadataLocal
              .setVolumeDescription(volumeMetadataChangeFromMaster.getVolumeDescription());
        }

        volumeStore.saveVolumeForReport(volumeMetadataLocal);
      }
    }

    // set not report volume to master and delete it in follower
    /* remove the volume in Delete table **/
    Set<Long> notReportThisVolume = response.getNotReportThisVolume();
    volumesToDelete = volumeStore.listVolumesWhichToDeleteForReport();

    for (VolumeMetadata volume : volumesToDelete) {
      long volumeId = volume.getVolumeId();
      if (notReportThisVolume.contains(volumeId)) {
        //remove it
        volumeStore.deleteVolumesWhichToDeleteForReport(volumeId);
        notReportThisVolume.remove(volumeId);
        logger
            .warn("the volume :{} is dead, so remove it in dead volume table, the after table :{}",
                volumeId, volumeStore.listVolumesWhichToDeleteForReport());
      }
    }

    volumeInformationManger.addVolumeToDeleteForEquilibrium(notReportThisVolume);
  }

  /**
   * when volume Equilibrium ok, delete it in follower memory.
   */
  private void deleteVolumeForEquilibrium() {
    Set<Long> toDeleteVolumes = volumeInformationManger.getVolumeToDeleteForEquilibrium();
    if (toDeleteVolumes.isEmpty()) {
      return;
    } else {
      logger.warn(
          "in current instance, the volume:{} will not report to master, so delete it in memory",
          toDeleteVolumes);
      //20s later, remove the volume in follower
      Iterator iterator = toDeleteVolumes.iterator();
      while (iterator.hasNext()) {
        long volumeId = (long) iterator.next();
        if (timeToDeleteVolumeForEquilibrium.containsKey(volumeId)) {
          long time = timeToDeleteVolumeForEquilibrium.get(volumeId);
          if (time >= 20) { //20 s

            //delete volume in memory
            volumeInformationManger.deleteVolumeForReport(volumeId);
            timeToDeleteVolumeForEquilibrium.remove(volumeId);
            iterator.remove();
            logger.warn("when deleteVolumeForEquilibrium, the volume id is :{}", volumeId);
          } else {
            time++;
            timeToDeleteVolumeForEquilibrium.put(volumeId, time);
          }
        } else {
          //init the count for delete volume
          timeToDeleteVolumeForEquilibrium.put(volumeId, 0L);
        }
      }
    }
  }



  public boolean checkLastExtendedTimeChange(VolumeMetadata volumeMetadataChangeFromMaster,
      VolumeMetadata volumeMetadataLocal) {
    if (volumeMetadataChangeFromMaster.getLastExtendedTime() != null
        && !volumeMetadataChangeFromMaster.getLastExtendedTime()
        .equals(volumeMetadataLocal.getLastExtendedTime())) {
      return true;
    }

    return false;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public InformationCenterClientFactory getInfoCenterClientFactory() {
    return infoCenterClientFactory;
  }

  public void setInfoCenterClientFactory(InformationCenterClientFactory infoCenterClientFactory) {
    this.infoCenterClientFactory = infoCenterClientFactory;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public void setReportVolumeManager(ReportVolumeManager reportVolumeManager) {
    this.reportVolumeManager = reportVolumeManager;
  }
}
