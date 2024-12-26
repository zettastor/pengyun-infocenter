/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
