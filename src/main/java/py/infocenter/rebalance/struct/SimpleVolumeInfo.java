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

package py.infocenter.rebalance.struct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


@Deprecated
public class SimpleVolumeInfo {

  private final long volumeId;
  private final Map<Integer, SimpleSegmentInfo> segments;
  private final VolumeType volumeType;
  private final long storagePoolId;

  private VolumeStatus status;



  public SimpleVolumeInfo(long volumeId, VolumeType volumeType, long storagePoolId) {
    this.volumeId = volumeId;
    this.volumeType = volumeType;
    this.storagePoolId = storagePoolId;
    this.segments = new HashMap<>();
  }

  public void addSegment(SimpleSegmentInfo segment) {
    segments.put(segment.getSegId().getIndex(), segment);
    segment.setVolume(this);
  }

  public SimpleSegmentInfo getSegment(int index) {
    return segments.get(index);
  }

  public long getVolumeId() {
    return volumeId;
  }

  public VolumeStatus getStatus() {
    return status;
  }

  public void setStatus(VolumeStatus status) {
    this.status = status;
  }

  public List<SimpleSegmentInfo> getSegments() {
    return new ArrayList<>(segments.values());
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

}
