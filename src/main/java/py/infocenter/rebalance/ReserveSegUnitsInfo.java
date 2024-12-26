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

package py.infocenter.rebalance;

import java.util.Set;
import py.archive.segment.SegmentUnitType;

/**
 *  for ReserveSegUnits request info.
 */
public class ReserveSegUnitsInfo {

  public long segmentSize; // required
  public Set<Long> excludedInstanceIds; // required
  public int numberOfSegUnits; // required
  public long volumeId; // required
  public int segIndex; // required
  public SegmentUnitType segmentUnitType;


  
  public ReserveSegUnitsInfo(long segmentSize, Set<Long> excludedInstanceIds,
      int numberOfSegUnits, long volumeId, int segIndex, SegmentUnitType segmentUnitType) {
    this.segmentSize = segmentSize;
    this.excludedInstanceIds = excludedInstanceIds;
    this.numberOfSegUnits = numberOfSegUnits;
    this.volumeId = volumeId;
    this.segIndex = segIndex;
    this.segmentUnitType = segmentUnitType;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public Set<Long> getExcludedInstanceIds() {
    return excludedInstanceIds;
  }

  public void setExcludedInstanceIds(Set<Long> excludedInstanceIds) {
    this.excludedInstanceIds = excludedInstanceIds;
  }

  public int getNumberOfSegUnits() {
    return numberOfSegUnits;
  }

  public void setNumberOfSegUnits(int numberOfSegUnits) {
    this.numberOfSegUnits = numberOfSegUnits;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getSegIndex() {
    return segIndex;
  }

  public void setSegIndex(int segIndex) {
    this.segIndex = segIndex;
  }

  public SegmentUnitType getSegmentUnitType() {
    return segmentUnitType;
  }

  public void setSegmentUnitType(SegmentUnitType segmentUnitType) {
    this.segmentUnitType = segmentUnitType;
  }

}
