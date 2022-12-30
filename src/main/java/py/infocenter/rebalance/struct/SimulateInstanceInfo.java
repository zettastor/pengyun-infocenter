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
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.InstanceMetadata;
import py.instance.InstanceId;

/**
 * instance information simulation.
 */
public class SimulateInstanceInfo implements Comparable<SimulateInstanceInfo> {

  private static final Logger logger = LoggerFactory.getLogger(SimulateInstanceInfo.class);
  private final long domainId;
  private final int groupId;
  private final int diskCount;
  private final long segmentSize;
  private final int realWeight; //weight from datanode
  private final InstanceMetadata.DatanodeType datanodeType;
  private InstanceId instanceId;
  private List<Long> archiveIds;
  private long freeSpace;
  private int freeFlexibleSegmentUnitCount;
  private int weight;


  public SimulateInstanceInfo(InstanceId instanceId, Collection<Long> disks, int groupId,
      long domainId, long freeSpace,
      int freeFlexibleSegmentUnitCount, long segmentSize, int realWeight, int weight,
      InstanceMetadata.DatanodeType datanodeType) {
    this.instanceId = instanceId;
    this.groupId = groupId;
    this.domainId = domainId;
    if (disks != null) {
      this.diskCount = disks.size();
      this.archiveIds = new ArrayList<>(disks);
    } else {
      this.diskCount = 0;
      this.archiveIds = new ArrayList<>();
    }
    this.freeSpace = freeSpace;
    this.segmentSize = segmentSize;
    this.freeFlexibleSegmentUnitCount = freeFlexibleSegmentUnitCount;
    this.realWeight = realWeight;
    this.weight = weight;
    this.datanodeType = datanodeType;
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(InstanceId instanceId) {
    this.instanceId = instanceId;
  }

  public int getGroupId() {
    return groupId;
  }

  public long getDomainId() {
    return domainId;
  }

  public int getDiskCount() {
    return diskCount;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public List<Long> getArchiveIds() {
    return archiveIds;
  }

  public void setArchiveIds(List<Long> archiveIds) {
    this.archiveIds = archiveIds;
  }

  public long getFreeSpace() {
    return freeSpace;
  }

  public void setFreeSpace(long freeSpace) {
    this.freeSpace = freeSpace;
  }

  public int getFreeFlexibleSegmentUnitCount() {
    return freeFlexibleSegmentUnitCount;
  }

  public void setFreeFlexibleSegmentUnitCount(int freeFlexibleSegmentUnitCount) {
    this.freeFlexibleSegmentUnitCount = freeFlexibleSegmentUnitCount;
  }

  public int getRealWeight() {
    return realWeight;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  public InstanceMetadata.DatanodeType getDatanodeType() {
    return datanodeType;
  }

  @Override
  public final int compareTo(@Nonnull SimulateInstanceInfo other) {
    int compare = Double.compare(weight, other.weight);
    if (compare == 0) {
      return Long.compare(instanceId.getId(), other.getInstanceId().getId());
    } else {
      return compare;
    }
  }

  @Override
  public String toString() {
    return "SimulateInstanceInfo{"

        + "instanceId=" + instanceId

        + ", domainId=" + domainId

        + ", groupId=" + groupId

        + ", diskCount=" + diskCount

        + ", segmentSize=" + segmentSize

        + ", archiveIds=" + archiveIds

        + ", freeSpace=" + freeSpace

        + ", freeFlexibleSegmentUnitCount=" + freeFlexibleSegmentUnitCount

        + ", realWeight=" + realWeight

        + ", weight=" + weight

        + ", datanodeType=" + datanodeType

        + '}';
  }
}
