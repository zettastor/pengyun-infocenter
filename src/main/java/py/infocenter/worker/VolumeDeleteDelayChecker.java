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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.icshare.VolumeDeleteDelayInformation;
import py.icshare.VolumeRecycleInformation;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.volume.recycle.VolumeRecycleManager;
import py.instance.InstanceStatus;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.periodic.Worker;
import py.querylog.eventdatautil.EventDataWorker;
import py.thrift.share.VolumeDeletingExceptionThrift;
import py.volume.VolumeMetadata;


public class VolumeDeleteDelayChecker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(VolumeDeleteDelayChecker.class);

  private long recycleDeleteTimeSecond;
  private long recycleKeepTimeSecond;
  private VolumeDelayStore volumeDelayStore;
  private VolumeRecycleStore volumeRecycleStore;
  private AppContext appContext;
  private VolumeRecycleManager volumeRecycleManager;
  private InformationCenterImpl informationCenter;
  private int updateCount = 3;



  public VolumeDeleteDelayChecker(long recycleKeepTimeSecond, long recycleDeleteTimeSecond,
      VolumeDelayStore volumeDelayStore,
      VolumeRecycleStore volumeRecycleStore, AppContext appContext,
      VolumeRecycleManager volumeRecycleManager) {
    this.recycleDeleteTimeSecond = recycleDeleteTimeSecond;
    this.volumeDelayStore = volumeDelayStore;
    this.volumeRecycleStore = volumeRecycleStore;
    this.appContext = appContext;
    this.volumeRecycleManager = volumeRecycleManager;
    this.recycleKeepTimeSecond = recycleKeepTimeSecond;
  }

  @Override
  public void doWork() {
    logger.info("begin VolumeDeleteDelayChecker, recycleDeleteTimeSecond: {}",
        recycleDeleteTimeSecond);
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("only the master can do it");
      return;
    }


    doVolumeDeleteDelay();


    if (1 == (updateCount++ / 3)) {
      doVolumeRecycleInfo();
      updateCount = 0;
    }
  }

  private void doVolumeDeleteDelay() {
    List<VolumeDeleteDelayInformation> volumeDeleteDelayInformationList = volumeDelayStore
        .listVolumesDelayInfo();
    for (VolumeDeleteDelayInformation volumeDeleteDelayInformation :
        volumeDeleteDelayInformationList) {

      //the delay is stop
      if (volumeDeleteDelayInformation.isStopDelay()) {
        continue;
      }

      long volumeId = volumeDeleteDelayInformation.getVolumeId();
      long time = volumeDeleteDelayInformation.getTimeForDelay();

      //time enough
      long timeLeft = time - recycleDeleteTimeSecond;
      if (timeLeft > 0) {
        //update the
        logger.warn("for volume :{}. the timeLeft is :{} ", volumeId, timeLeft);
        volumeDelayStore.updateVolumeDelayTimeInfo(volumeId, timeLeft);
      } else {
        VolumeMetadata volumeMetadata = volumeRecycleManager.getVolumeStore().getVolume(volumeId);
        if (volumeMetadata == null) {
          logger.warn("for volume :{} in delete delay check, can not find the volume", volumeId);
          //remove in db
          volumeDelayStore.deleteVolumeDelayInfo(volumeId);
          continue;
        }

        try {
          logger.warn("for volume :{} in delete delay check, begin to move recycle", volumeId);
          volumeRecycleManager.checkVolumeInfo(volumeId);


          volumeDelayStore.deleteVolumeDelayInfo(volumeId);


          VolumeRecycleInformation volumeRecycleInformation = new VolumeRecycleInformation(volumeId,
              System.currentTimeMillis());
          volumeRecycleStore.saveVolumeRecycleInfo(volumeRecycleInformation);

          generateEventData(volumeMetadata, true, "");
        } catch (Exception e) {
          generateEventData(volumeMetadata, false, e.getMessage());
        }
      }
    }
  }

  private void doVolumeRecycleInfo() {
    List<VolumeRecycleInformation> volumeRecycleInformationList = volumeRecycleStore
        .listVolumesRecycleInfo();
    logger.info("the volume recycle information is :{}", volumeRecycleInformationList);
    for (VolumeRecycleInformation volumeRecycleInformation : volumeRecycleInformationList) {
      long currentTime = System.currentTimeMillis();
      if (currentTime - volumeRecycleInformation.getTimeInRecycle()
          < recycleKeepTimeSecond * 1000) {
        continue;
      } else {
        //the volume can delete
        long volumeId = volumeRecycleInformation.getVolumeId();
        VolumeMetadata volumeMetadata = volumeRecycleManager.getVolumeStore().getVolume(volumeId);
        if (volumeMetadata == null) {
          logger.warn("for volume :{} in Recycle check, can not find the volume", volumeId);
          volumeRecycleStore.deleteVolumeRecycleInfo(volumeId);
          continue;
        }

        py.thrift.icshare.DeleteVolumeRequest deleteVolumeRequest =
            new py.thrift.icshare.DeleteVolumeRequest();
        deleteVolumeRequest.setRequestId(RequestIdBuilder.get());
        deleteVolumeRequest.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        deleteVolumeRequest.setVolumeName(volumeMetadata.getName());
        deleteVolumeRequest.setVolumeId(volumeId);
        try {
          logger.warn("for volume :{} in recycle check, begin to delete the volume, the time:{}",
              volumeId, volumeRecycleInformation.getTimeInRecycle());
          informationCenter.deleteVolume(deleteVolumeRequest);
          //remove in db when volume delete
        } catch (Exception e) {
          //not VolumeDeletingExceptionThrift
          if (!(e instanceof VolumeDeletingExceptionThrift)) {
            logger.warn("for volume :{} in Recycle check, delete volume find some error:", volumeId,
                e);
          }
        }
      }
    }
  }

  private void generateEventData(VolumeMetadata volumeMetadata, boolean recovery, String value) {
    logger.warn("volumeDeleteDelay generate eventdata for volume: {}, recovery status: {}",
        volumeMetadata.getVolumeId(), recovery);
    Map<String, String> defaultNameValues = new HashMap<>();
    defaultNameValues
        .put(UserDefineName.VolumeID.toString(), String.valueOf(volumeMetadata.getVolumeId()));
    defaultNameValues.put(UserDefineName.VolumeName.toString(), volumeMetadata.getName());
    defaultNameValues.put(UserDefineName.VolumeDeleteDelayException.toString(), value);
    EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER, defaultNameValues);

    if (recovery) {
      eventDataWorker
          .work(OperationName.Volume.toString(), CounterName.VOLUMEDELETE_DELAY.toString(), 1, 0);
    } else {
      eventDataWorker
          .work(OperationName.Volume.toString(), CounterName.VOLUMEDELETE_DELAY.toString(), 0, 0);
    }
  }

  public void setInformationCenter(InformationCenterImpl informationCenter) {
    this.informationCenter = informationCenter;
  }
}
