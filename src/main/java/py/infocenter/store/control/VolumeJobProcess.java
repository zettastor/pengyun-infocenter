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

package py.infocenter.store.control;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.common.RequestIdBuilder;
import py.exception.NotEnoughSpaceException;
import py.icshare.VolumeCreationRequest;
import py.infocenter.create.volume.CreateVolumeManager;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.volume.VolumeMetadata;


public class VolumeJobProcess implements VolumeJobStore {

  private static final Logger logger = LoggerFactory.getLogger(VolumeJobProcess.class);
  private static final int CHECK_NEW_VOLUME_STATUS_COUNT = 15;
  private static final int AVAILABLE_REACH_COUNT = 5;
  private static final int MAX_DELAY_UNMOUNT_VOLUME_COUNT = 30;
  private VolumeInformationManger volumeInformationManger;
  private CreateVolumeManager createVolumeManager;
  private VolumeJobStoreDb volumeJobStoredb;
  private InformationCenterImpl informationCenter;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  public VolumeJobProcess() {
  }

  @SuppressWarnings("unchecked")
  @Override
  @Transactional
  public boolean processCreateVolumeRequest() {
    List<VolumeCreationRequest> requests = volumeJobStoredb.getCreateOrExtendVolumeRequest();
    if (requests != null && requests.size() > 0) {
      VolumeCreationRequest volumeRequest = requests.get(0);
      try {
        /* Reserving volume is going to call info center to reserve the space if
         * there are enough space.
         * The list of instances returned by this API is ignored. Later,
         * saveCreateOrExtendVolumeRequest() will call
         * reserveVolume() again to get a list of instances. It really doesn't
         * hurt to call this API twice with
         * the exactly same request because information center has handled this case
         ****/
        logger.warn("Going to createVolume: {}", volumeRequest);
        createVolumeManager.createVolume(volumeRequest);
      } catch (NotEnoughSpaceException e) {
        // fail the request only when there is no enough space
        logger.error("there is enough space for creating volume request: {},the exception is:",
            volumeRequest, e);
      } catch (Exception e) {
        // create delete volume request for recycling resources
        logger.error("can not create volume request:{}, the exception is ", volumeRequest, e);
      } finally {
        // delete create request first
        volumeJobStoredb.deleteCreateOrExtendVolumeRequest(volumeRequest);
      }

      if (requests.size() > 1) {
        return true;
      }
    }
    return false;
  }

  @Override
  @Transactional
  public void processDeleteVolumeRequest() {
    List<DeleteVolumeRequest> requests = volumeJobStoredb.getDeleteVolumeRequest();
    if (requests.size() > 0) {
      logger.warn("get delete volume requests:{}", requests);
    }

    
    for (DeleteVolumeRequest request : requests) {
      int availableTimes = 0;
      for (int checkTime = 0; checkTime < CHECK_NEW_VOLUME_STATUS_COUNT; checkTime++) {
        try {
          VolumeMetadata volumeNew = volumeInformationManger
              .getVolumeNew(request.getNewVolumeId(), request.getAccountId());

          
          boolean cloneFinishStatus = true;

          // if new volume is available
          if (volumeNew.isVolumeAvailable() && cloneFinishStatus) {
            availableTimes++;
            Thread.sleep(1000);
          } else {
            break;
          }

          if (availableTimes >= AVAILABLE_REACH_COUNT) {
            // delete current volume
            py.thrift.icshare.DeleteVolumeRequest deleteVolumeRequest =
                new py.thrift.icshare.DeleteVolumeRequest();
            deleteVolumeRequest.setRequestId(RequestIdBuilder.get());
            deleteVolumeRequest.setAccountId(request.getAccountId());
           
            deleteVolumeRequest.setVolumeName(request.getNewVolumeName());
            deleteVolumeRequest.setVolumeId(request.getVolumeId());
            informationCenter.deleteVolume(deleteVolumeRequest);
            logger.warn("after move volume or move online ok, begin to delete old volume:{}",
                request.getVolumeId());

            volumeJobStoredb.deleteDeleteVolumeRequest(request);
            break;
          }
        } catch (Exception e) {
          logger.error("failed to check new volume:{} and deleted volume:{}",
              request.getNewVolumeId(),
              request.getVolumeId(), e);
        }
      }
    }
  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public CreateVolumeManager getCreateVolumeManager() {
    return createVolumeManager;
  }

  public void setCreateVolumeManager(CreateVolumeManager createVolumeManager) {
    this.createVolumeManager = createVolumeManager;
  }

  public VolumeJobStoreDb getVolumeJobStoredb() {
    return volumeJobStoredb;
  }

  public void setVolumeJobStoredb(VolumeJobStoreDb volumeJobStoredb) {
    this.volumeJobStoredb = volumeJobStoredb;
  }

  public InformationCenterImpl getInformationCenter() {
    return informationCenter;
  }

  public void setInformationCenter(InformationCenterImpl informationCenter) {
    this.informationCenter = informationCenter;
  }

  public LockForSaveVolumeInfo getLockForSaveVolumeInfo() {
    return lockForSaveVolumeInfo;
  }

  public void setLockForSaveVolumeInfo(LockForSaveVolumeInfo lockForSaveVolumeInfo) {
    this.lockForSaveVolumeInfo = lockForSaveVolumeInfo;
  }
}
