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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.instance.InstanceId;
import py.membership.SegmentMembership;


@Deprecated
public class SimpleSegmentInfo {

  private final Map<InstanceId, SimpleSegUnitInfo> segUnits = new HashMap<>();
  private final SegmentMembership highestMembership;
  private SegId segId;
  private SimpleVolumeInfo volume;


  
  public SimpleSegmentInfo(SegId segId, SegmentMembership highestMembership) {
    this.segId = segId;
    segUnits.clear();
    this.highestMembership = highestMembership;
  }

  public void addSegmentUnit(SimpleSegUnitInfo segUnit) {
    segUnits.put(segUnit.getInstanceId(), segUnit);
    segUnit.setSegment(this);
  }

  public void removeSegmentUnit(SimpleSegUnitInfo segUnit) {
    segUnits.remove(segUnit.getInstanceId());
  }

  public SegId getSegId() {
    return segId;
  }

  public void setSegId(SegId segId) {
    this.segId = segId;
  }

  public Map<InstanceId, SimpleSegUnitInfo> getSegUnitsMap() {
    return segUnits;
  }

  public Collection<SimpleSegUnitInfo> getSegUnits() {
    return segUnits.values();
  }


  
  public SimpleSegUnitInfo getPrimary() {
    for (SimpleSegUnitInfo segUnit : segUnits.values()) {
      if (segUnit.getStatus() == SegmentUnitStatus.Primary) {
        return segUnit;
      }
    }
    return null;
  }


  
  public List<SimpleSegUnitInfo> getSecondaries() {
    List<SimpleSegUnitInfo> secondaries = new ArrayList<>();
    for (SimpleSegUnitInfo segUnit : segUnits.values()) {
      if (segUnit.getStatus() == SegmentUnitStatus.Secondary) {
        secondaries.add(segUnit);
      }
    }
    return secondaries;
  }

  public SimpleVolumeInfo getVolume() {
    return volume;
  }

  public void setVolume(SimpleVolumeInfo volume) {
    this.volume = volume;
  }

  public SegmentMembership getHighestMembership() {
    return highestMembership;
  }

  @Override
  public String toString() {
    return "[" + getClass().getSimpleName() + " segId=" + segId + ", units=" + segUnits + "]";
  }
}
