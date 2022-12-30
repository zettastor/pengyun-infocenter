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

package py.infocenter.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.volume.VolumeExtendStatus;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;

/**
 * Thread-safe volume store.
 *
 */
public class TwoLevelVolumeStoreImpl implements VolumeStore {

  private static final Logger logger = LoggerFactory.getLogger(TwoLevelVolumeStoreImpl.class);

  private VolumeStore inMemoryVolumeStore;
  private VolumeStore dbVolumeStore;

  private Map<Long, VolumeMetadata> volumeTableForReport = new ConcurrentHashMap<>();
  private Map<Long, VolumeMetadata> volumeTableForToDelete = new ConcurrentHashMap<Long,
      VolumeMetadata>();

  public TwoLevelVolumeStoreImpl(VolumeStore inMemoryStore, VolumeStore dbStore) {
    this.inMemoryVolumeStore = inMemoryStore;
    this.dbVolumeStore = dbStore;
  }

  @Override
  public void saveVolume(VolumeMetadata volumeMetadata) {
    // first get from memory,if it's null. then get from DB
    VolumeMetadata volumeData = getVolume(volumeMetadata.getVolumeId());
    logger.info("save volume ,get the volume :{}", volumeData);
    // if volumeMetadata has not been in memory map, or volumeMetadata has changed
    if (volumeData != null && volumeData.getDeadTime() != 0) {
      logger.debug("saved deadtime: {} going to save deadtime: {}", volumeData.getDeadTime(),
          volumeMetadata.getDeadTime());
      // we do this way to make sure that deadTime won't be covered by 0 value
      volumeMetadata.setDeadTime(volumeData.getDeadTime());
    }

    /* just the master can save volume to db **/
    if (!volumeMetadata.equals(volumeData)) {
      dbVolumeStore.saveVolume(volumeMetadata);
      logger.info("save volume ,the volume :{}", volumeMetadata);
    }
    inMemoryVolumeStore.saveVolume(volumeMetadata);
  }

  @Override
  public void deleteVolume(VolumeMetadata volumeMetadata) {
    // the interface of this implement should be modified to throw some
    // exceptions out if it execute failed.
    dbVolumeStore.deleteVolume(volumeMetadata);
    inMemoryVolumeStore.deleteVolume(volumeMetadata);
  }

  @Override
  public List<VolumeMetadata> listVolumes() {
    /*
     * we can just get volumes from the memory cache,because we had make the data from database
     * is the same with the
     * data in memory
     */
    return inMemoryVolumeStore.listVolumes();
  }

  @Override
  public void loadVolumeFromDbToMemory() {
    List<VolumeMetadata> volumesInDb = dbVolumeStore.listVolumes();
    List<VolumeMetadata> volumesInMemory = inMemoryVolumeStore.listVolumes();
    List<VolumeMetadata> volumesInDbButNotInMemory = new ArrayList<VolumeMetadata>();
    for (VolumeMetadata volumeInDb : volumesInDb) {
      boolean exist = false;
      for (VolumeMetadata volumeInMemory : volumesInMemory) {
        if (volumeInDb.getVolumeId() == volumeInMemory.getVolumeId()) {
          exist = true;
        }
      }
      if (!exist) {
        volumesInDbButNotInMemory.add(volumeInDb);
      }
    }

    for (VolumeMetadata volumeInDbButNotInMemory : volumesInDbButNotInMemory) {
      inMemoryVolumeStore.saveVolume(volumeInDbButNotInMemory);
    }
  }

  @Override
  public void loadVolumeInDb() {
    List<VolumeMetadata> volumesInDb = dbVolumeStore.listVolumes();
    List<VolumeMetadata> volumesInMemery = inMemoryVolumeStore.listVolumes();
    List<VolumeMetadata> volumesInDbButNotInMemery = new ArrayList<>();
    for (VolumeMetadata volumeInDb : volumesInDb) {
      boolean exist = false;
      for (VolumeMetadata volumeInMemery : volumesInMemery) {
        if (volumeInDb.getVolumeId() == volumeInMemery.getVolumeId()) {
          exist = true;
        }
      }
      if (!exist) {
        volumesInDbButNotInMemery.add(volumeInDb);
      }
    }
    for (VolumeMetadata volumeInDbButNotInMemery : volumesInDbButNotInMemery) {
      logger.warn("found a volume in db but not in memory , delete it in db {}",
          volumeInDbButNotInMemery);
      dbVolumeStore.deleteVolume(volumeInDbButNotInMemery);
    }
  }

  @Override
  public VolumeMetadata getVolume(Long volumeId) {

    VolumeMetadata volume = inMemoryVolumeStore.getVolume(volumeId);
    if (volume == null) {
      volume = dbVolumeStore.getVolume(volumeId);
      logger.debug("DB volume: {}", volume);
      if (volume != null) {
        logger.debug(
            "TwoLevelVolumeStoreImpl::getVolume.Save volume[{}] into Memory.\nCurrent volumes in "
                + "memory are {[]}",
            volume.getName(), inMemoryVolumeStore.toString());
        inMemoryVolumeStore.saveVolume(volume);
      }
    }

    VolumeMetadata volumeToReturn = new VolumeMetadata();
    return volumeToReturn.deepCopy(volume);
  }

  @Override
  public VolumeMetadata followerGetVolume(Long volumeId) {
    //the follower get volume must form db
    return dbVolumeStore.getVolume(volumeId);

  }

  @Override
  public void clearData() {
    inMemoryVolumeStore.clearData();
  }

  @Override
  public VolumeMetadata getVolumeNotDeadByName(String name) {
    // Get VolumeMetadata from Memory first for quickly search,
    // if not in memory, then search for DB
    VolumeMetadata volume = inMemoryVolumeStore.getVolumeNotDeadByName(name);
    if (null == volume) {
      volume = dbVolumeStore.getVolumeNotDeadByName(name);
    }
    return volume;
  }

  @Override
  public int updateStatusAndVolumeInAction(long volumeId, String status, String volumeInAction) {
    // update db first
    dbVolumeStore.updateStatusAndVolumeInAction(volumeId, status, volumeInAction);
    return inMemoryVolumeStore.updateStatusAndVolumeInAction(volumeId, status, volumeInAction);
  }

  @Override
  public void updateStableTime(long volumeId, long timeMs) {
    inMemoryVolumeStore.updateStableTime(volumeId, timeMs);
  }

  @Override
  public long updateRebalanceVersion(long volumeId, long version) {
    long ver = inMemoryVolumeStore.updateRebalanceVersion(volumeId, version);
    return dbVolumeStore.updateRebalanceVersion(volumeId, ver);
  }

  @Override
  public long incrementRebalanceVersion(long volumeId) {
    long ver = inMemoryVolumeStore.incrementRebalanceVersion(volumeId);
    return dbVolumeStore.updateRebalanceVersion(volumeId, ver);
  }

  @Override
  public long getRebalanceVersion(long volumeId) {
    return inMemoryVolumeStore.getRebalanceVersion(volumeId);
  }

  @Override
  public void updateRebalanceInfo(long volumeId, VolumeRebalanceInfo rebalanceInfo) {
    inMemoryVolumeStore.updateRebalanceInfo(volumeId, rebalanceInfo);
  }

  /* just for report, only save in memory **/
  @Override
  public void saveVolumeForReport(VolumeMetadata volumeMetadata) {
    volumeTableForReport.put(volumeMetadata.getVolumeId(), volumeMetadata);
  }

  @Override
  public List<VolumeMetadata> listVolumesForReport() {

    List<VolumeMetadata> volumes = new ArrayList<>();
    volumes.addAll(volumeTableForReport.values());
    return volumes;
  }

  @Override
  public void updateVolumeForReport(VolumeMetadata volumeMetadata) {

    saveVolumeForReport(volumeMetadata);
  }

  @Override
  public List<VolumeMetadata> listVolumesWhichToDeleteForReport() {

    List<VolumeMetadata> volumes = new ArrayList<>();
    volumes.addAll(volumeTableForToDelete.values());
    return volumes;
  }

  @Override
  public void saveVolumesWhichToDeleteForReport(VolumeMetadata volumeMetadata) {
    volumeTableForToDelete.put(volumeMetadata.getVolumeId(), volumeMetadata);
  }

  @Override
  public void deleteVolumesWhichToDeleteForReport(Long volumeId) {
    volumeTableForToDelete.remove(volumeId);
  }

  @Override
  public void markVolumeDelete(Long volumeId) {
    // update db first
    dbVolumeStore.markVolumeDelete(volumeId);
    inMemoryVolumeStore.markVolumeDelete(volumeId);
  }

  @Override
  public void deleteVolumeForReport(VolumeMetadata volumeMetadata) {
    volumeTableForReport.remove(volumeMetadata.getVolumeId());
  }

  @Override
  public VolumeMetadata getVolumeForReport(Long volumeId) {
    VolumeMetadata volume = volumeTableForReport.get(volumeId);
    return volume;
  }

  @Override
  public String updateVolumeLayoutForReport(long volumeId, int startSegmentIndex,
      int newSegmentsCount, String volumeLayout) {
    VolumeMetadata volume = volumeTableForReport.get(volumeId);
    if (volumeLayout != null) {
      volume.setVolumeLayout(volumeLayout);
      return volumeLayout;
    }
    logger.warn("now to update volume layout from {} to {}", startSegmentIndex,
        startSegmentIndex + newSegmentsCount);
    for (int index = startSegmentIndex; index < startSegmentIndex + newSegmentsCount; index++) {
      volume.updateVolumeLayout(index, true);
    }
    return volume.getVolumeLayout();
  }

  @Override
  public void clearDataForReport() {
    volumeTableForReport.clear();
  }

  @Override
  public VolumeMetadata listVolumesByIdForReport(long rootId) {
    VolumeMetadata root = volumeTableForReport.get(rootId);
    if (root == null) {
      logger.error("root volume:{} is null", rootId);
    }
    return root;

  }

  @Override
  public int updateExtendingSizeForReport(long volumeId, long extendingSize,
      long thisTimeExtendSize) {
    VolumeMetadata volume = volumeTableForReport.get(volumeId);
    volume.setExtendingSize(extendingSize);
    if (thisTimeExtendSize != 0) {
      String oldEachTimeExtendVolumeSize = volume.getEachTimeExtendVolumeSize();
      String eachTimeExtendVolumeSize =
          String.valueOf((int) Math.ceil(thisTimeExtendSize * 1.0 / volume.getSegmentSize())) + ",";
      volume.setEachTimeExtendVolumeSize(oldEachTimeExtendVolumeSize + eachTimeExtendVolumeSize);
    }
    return 1; // there must be only one volume extendingSize been changed
  }

  @Override
  public int updateDeadTimeForReport(long volumeId, long deadTime) {
    VolumeMetadata volume = volumeTableForReport.get(volumeId);
    volume.setDeadTime(deadTime);
    return 1; // there must be only one volume status been changed
  }

  @Override
  public int updateStatusAndVolumeInActionForReport(long volumeId, String status,
      String volumeInAction) {
    VolumeMetadata volume = volumeTableForReport.get(volumeId);
    volume.setVolumeStatus(VolumeStatus.valueOf(status));
    if (!volumeInAction.equals("")) {
      volume.setInAction(VolumeInAction.valueOf(volumeInAction));
    }

    return 1; // there must be only one volume status been changed
  }

  @Override
  public int updateVolumeExtendStatusForReport(long volumeId, String status) {
    VolumeMetadata volume = volumeTableForReport.get(volumeId);
    volume.setVolumeExtendStatus(VolumeExtendStatus.valueOf(status));
    return 1;
  }

  @Override
  public String toString() {
    String tmp = "";
    for (VolumeMetadata volumeMetadata : volumeTableForReport.values()) {
      tmp += volumeMetadata.getName();
      tmp += "\n";
    }
    return tmp;
  }

  @Override
  public String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount,
      String volumeLayout) {
    inMemoryVolumeStore
        .updateVolumeLayout(volumeId, startSegmentIndex, newSegmentsCount, volumeLayout);
    return dbVolumeStore
        .updateVolumeLayout(volumeId, startSegmentIndex, newSegmentsCount, volumeLayout);
  }

  @Override
  public void updateDescription(long volumeId, String description) {
    // update db first
    dbVolumeStore.updateDescription(volumeId, description);
    inMemoryVolumeStore.updateDescription(volumeId, description);
  }

  @Override
  public void updateClientLastConnectTime(long volumeId, long time) {
    // update db first
    dbVolumeStore.updateClientLastConnectTime(volumeId, time);
    inMemoryVolumeStore.updateClientLastConnectTime(volumeId, time);
  }

}
