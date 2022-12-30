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

package py.infocenter.create.volume;

import static py.icshare.InstanceMetadata.DatanodeStatus.OK;

import java.util.List;
import java.util.Map;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.exception.InternalErrorException;
import py.exception.InvalidInputException;
import py.exception.NotEnoughSpaceException;
import py.icshare.InstanceMetadata;
import py.icshare.VolumeCreationRequest;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.service.RefreshTimeAndFreeSpace;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;


public class ReserveInformation {

  private static final Logger logger = LoggerFactory.getLogger(ReserveInformation.class);
  private StorageStore storageStore;
  private VolumeStore volumeStore; // store the all volume;
  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private RefreshTimeAndFreeSpace refreshTimeAndFreeSpace;
  private int segmentWrappCount = 10; //default value


  public ReserveInformation() {
    refreshTimeAndFreeSpace = RefreshTimeAndFreeSpace.getInstance();
  }


  public synchronized Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
      reserveVolume(VolumeCreationRequest request)
      throws NotEnoughSpaceException, InvalidInputException, InternalErrorException, TException {

    logger.warn("reserveVolume request: {}", request);
    long segmentSize = request.getSegmentSize();
    long expectedSize = request.getVolumeSize();
    if (expectedSize > request.getVolumeSize()) {
      expectedSize = request.getVolumeSize();
    }

    if (expectedSize < segmentSize) {
      logger.error("volume size : {} less than segment size : {}", expectedSize, segmentSize);
      throw new InvalidInputException();
    }

    VolumeType volumeType = VolumeType.valueOf(request.getVolumeType());

    // calculate the total capacity, should not exceed the threshold
    long freeSpace = 0L;
    for (InstanceMetadata entry : storageStore.list()) {
      if (entry.getDatanodeStatus().equals(OK)) {
        freeSpace += entry.getFreeSpace();
      }
    }

    if (refreshTimeAndFreeSpace.getLastRefreshTime() == 0 || (
        System.currentTimeMillis() - refreshTimeAndFreeSpace.getLastRefreshTime()
            >= InfoCenterConstants.getRefreshPeriodTime())) {
      refreshTimeAndFreeSpace.setActualFreeSpace(freeSpace);
      refreshTimeAndFreeSpace.setLastRefreshTime(System.currentTimeMillis());
    }

    VolumeMetadata volumeMetadata = volumeStore.getVolume(request.getVolumeId());

    if (volumeMetadata == null) {
      throw new InternalErrorException();
    }

    logger.warn("Found volume: {}", volumeMetadata);

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segIndex2InstancesToRemote = null;
    try {
      // reserve volume in existing instances
      segIndex2InstancesToRemote = segmentUnitsDistributionManager
          .reserveVolume(expectedSize, volumeType, false,
              getSegmentWrappCount(),
              volumeMetadata.getStoragePoolId());
    } finally {
      logger.info("nothing need to do here");
    }

    if (segIndex2InstancesToRemote == null) {
      logger.error("no space for create volume {}, volume size is {}, current free space is {}",
          volumeMetadata.getName(), volumeMetadata.getVolumeSize(), freeSpace);
      throw new NotEnoughSpaceException();
    }

    logger.warn("reserveVolume result: {}", segIndex2InstancesToRemote);
    return segIndex2InstancesToRemote;
  }


  public StorageStore getStorageStore() {
    return storageStore;
  }

  public void setStorageStore(StorageStore storageStore) {
    this.storageStore = storageStore;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public SegmentUnitsDistributionManager getSegmentUnitsDistributionManager() {
    return segmentUnitsDistributionManager;
  }

  public void setSegmentUnitsDistributionManager(
      SegmentUnitsDistributionManager segmentUnitsDistributionManager) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
  }

  public int getSegmentWrappCount() {
    return segmentWrappCount;
  }

  public void setSegmentWrappCount(int segmentWrappCount) {
    this.segmentWrappCount = segmentWrappCount;
  }
}
