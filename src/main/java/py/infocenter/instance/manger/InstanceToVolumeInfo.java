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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.infocenter.store.VolumeStore;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

public class InstanceToVolumeInfo {

  private static final Logger logger = LoggerFactory.getLogger(InstanceToVolumeInfo.class);
  /**
   * save the all volume which come form master and follower in memory instanceId  volumeId volume
   * id, ok segment number.
   **/
  private Map<Long, Long> volumeInfo;
  private long lastReportedTime;
  private VolumeStore volumeStore;


  public InstanceToVolumeInfo() {
    this.volumeInfo = new HashMap<>();
    this.lastReportedTime = 0;
  }

  public void addVolume(long volumeId, Long okSegmentNumber) {
    volumeInfo.put(volumeId, okSegmentNumber);
  }

  public void moveVolume(long volumeId) {
    volumeInfo.remove(volumeId);
  }

  public boolean containsValue(long volumeId) {
    return volumeInfo.containsKey(volumeId);
  }

  
  public Set<Long> getVolumeInfo() {
    Set<Long> volumeInfos = new HashSet<>();
    for (Map.Entry<Long, Long> entry : volumeInfo.entrySet()) {
      volumeInfos.add(entry.getKey());
    }

    return volumeInfos;
  }

  
  public void setVolumeInfo(Set<Long> volumeInfos) {
    for (Long volumeId : volumeInfos) {
      //init 0
      volumeInfo.put(volumeId, 0L);
    }
  }

  public long getVolumesegmentUnitNumber(long volumeId) {
    return volumeInfo.get(volumeId);
  }

  /**
   * volume size, volume segment number.
   ***/
  public Pair<Long, Long> getTotalVolumeSize() {
    Pair<Long, Long> totalVolumeSizeAndSegmentInfo = new Pair<>();
    long totalVolumeSize = 0;
    long totalVolumeSegmentSize = 0;
    for (Long volumeId : getVolumeInfo()) {
      VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
      if (volumeMetadata == null) {
        logger.error("when distribute Volume, can not find the volume :{}", volumeId);
        continue;
      } else {
        if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Dead)) {
          logger.warn("when distribute Volume, can not care the Dead volume :{}", volumeId);
        } else {
          totalVolumeSize += volumeMetadata.getVolumeSize();
          totalVolumeSegmentSize += volumeMetadata.getSegmentCount();
        }
      }
    }

    totalVolumeSizeAndSegmentInfo.setFirst(totalVolumeSize);
    totalVolumeSizeAndSegmentInfo.setSecond(totalVolumeSegmentSize);
    return totalVolumeSizeAndSegmentInfo;
  }

  public long getLastReportedTime() {
    return lastReportedTime;
  }

  public void setLastReportedTime(long lastReportedTime) {
    this.lastReportedTime = lastReportedTime;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  @Override
  public String toString() {
    return "InstanceToVolumeInfo{"

        + "volumeInfo=" + volumeInfo

        + ", lastReportedTime=" + lastReportedTime

        + '}';
  }
}
