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
