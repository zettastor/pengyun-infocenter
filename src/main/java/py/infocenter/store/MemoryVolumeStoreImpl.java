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
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;


public class MemoryVolumeStoreImpl implements VolumeStore {

  private static final Logger logger = LoggerFactory.getLogger(MemoryVolumeStoreImpl.class);

  private Map<Long, VolumeMetadata> volumeTable = new ConcurrentHashMap<Long, VolumeMetadata>();



  public MemoryVolumeStoreImpl() {
  }

  @Override
  public void saveVolume(VolumeMetadata volumeMetadata) {
    volumeTable.put(volumeMetadata.getVolumeId(), volumeMetadata);
  }

  @Override
  public void deleteVolume(VolumeMetadata volumeMetadata) {
    volumeTable.remove(volumeMetadata.getVolumeId());
  }

  @Override
  public List<VolumeMetadata> listVolumes() {
    List<VolumeMetadata> volumes = new ArrayList<>();
    volumes.addAll(volumeTable.values());
    return volumes;
  }

  @Override
  public VolumeMetadata getVolume(Long volumeId) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    return volume;
  }

  @Override
  public VolumeMetadata getVolumeNotDeadByName(String name) {
    for (VolumeMetadata volumeMetadata : volumeTable.values()) {
      if (volumeMetadata.getName().equals(name) && VolumeStatus.Dead != volumeMetadata
          .getVolumeStatus() && !volumeMetadata.isMarkDelete()) {
        return volumeMetadata;
      }
    }
    return null;
  }

  @Override
  public int updateStatusAndVolumeInAction(long volumeId, String status, String volumeInAction) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setVolumeStatus(VolumeStatus.valueOf(status));
    if (!volumeInAction.equals("")) {
      volume.setInAction(VolumeInAction.valueOf(volumeInAction));
    }

    return 1; // there must be only one volume status been changed
  }

  @Override
  public String toString() {
    String tmp = "";
    for (VolumeMetadata volumeMetadata : volumeTable.values()) {
      tmp += volumeMetadata.getName();
      tmp += "\n";
    }
    return tmp;
  }

  @Override
  public void updateStableTime(long volumeId, long timeMs) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setStableTime(timeMs);
  }

  @Override
  public long updateRebalanceVersion(long volumeId, long version) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.getRebalanceInfo().setRebalanceVersion(version);
    return version;
  }

  @Override
  public long incrementRebalanceVersion(long volumeId) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    long ver = volume.getRebalanceInfo().getRebalanceVersion() + 1;
    volume.getRebalanceInfo().setRebalanceVersion(ver);
    return ver;
  }

  @Override
  public long getRebalanceVersion(long volumeId) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    return volume.getRebalanceInfo().getRebalanceVersion();
  }

  @Override
  public void updateRebalanceInfo(long volumeId, VolumeRebalanceInfo rebalanceInfo) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setRebalanceInfo(rebalanceInfo);
  }

  @Override
  public void clearData() {
    volumeTable.clear();
  }

  @Override
  public String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount,
      String volumeLayout) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setVolumeLayout(volumeLayout);
    return volume.getVolumeLayout();
  }

  @Override
  public void updateDescription(long volumeId, String description) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setVolumeDescription(description);
  }

  @Override
  public void updateClientLastConnectTime(long volumeId, long time) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setClientLastConnectTime(time);
  }

  @Override
  public VolumeMetadata followerGetVolume(Long volumeId) {
    return null;
  }

  @Override
  public void deleteVolumeForReport(VolumeMetadata volumeMetadata) {

  }

  @Override
  public VolumeMetadata getVolumeForReport(Long volumeId) {
    return null;
  }

  @Override
  public String updateVolumeLayoutForReport(long volumeId, int startSegmentIndex,
      int newSegmentsCount, String volumeLayout) {
    return null;
  }

  @Override
  public void clearDataForReport() {

  }

  @Override
  public VolumeMetadata listVolumesByIdForReport(long rootId) {
    return null;
  }

  @Override
  public int updateDeadTimeForReport(long volumeId, long deadTime) {
    return 0;
  }

  @Override
  public int updateStatusAndVolumeInActionForReport(long volumeId, String status,
      String volumeInAction) {
    return 0;
  }

  @Override
  public int updateVolumeExtendStatusForReport(long volumeId, String status) {
    return 0;
  }

  @Override
  public int updateExtendingSizeForReport(long volumeId, long extendingSize,
      long thisTimeExtendSize) {
    return 0;
  }

  @Override
  public void saveVolumeForReport(VolumeMetadata volumeMetadata) {
  }

  @Override
  public List<VolumeMetadata> listVolumesForReport() {
    return null;
  }

  @Override
  public void updateVolumeForReport(VolumeMetadata volumeMetadata) {

  }

  @Override
  public List<VolumeMetadata> listVolumesWhichToDeleteForReport() {
    return null;
  }

  @Override
  public void saveVolumesWhichToDeleteForReport(VolumeMetadata volumeMetadata) {

  }

  @Override
  public void deleteVolumesWhichToDeleteForReport(Long volumeId) {

  }

  @Override
  public void markVolumeDelete(Long volumeId) {
    VolumeMetadata volume = volumeTable.get(volumeId);
    volume.setMarkDelete(true);
  }

  @Override
  public void loadVolumeFromDbToMemory() {
  }

  @Override
  public void loadVolumeInDb() {

  }


}
