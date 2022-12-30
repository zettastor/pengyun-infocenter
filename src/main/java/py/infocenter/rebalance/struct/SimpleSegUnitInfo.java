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

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.infocenter.rebalance.InstanceInfo;
import py.instance.InstanceId;


@Deprecated
public class SimpleSegUnitInfo {

  private static final Logger logger = LoggerFactory.getLogger(SimpleSegUnitInfo.class);
  private static final long FAKED_ARCHIVE_ID = -1L;
  private final SegId segId;
  private final InstanceId instanceId;
  private int groupId;
  private long storagePoolId;
  private long archiveId;
  private boolean isFaked;
  private SegmentUnitStatus status;
  private SimpleSegmentInfo segment;

  private SegmentUnitMetadata realSegUnit;


  
  public SimpleSegUnitInfo(SegmentUnitMetadata segmentUnit, int groupId, long storagePoolId) {
    this.realSegUnit = segmentUnit;
    this.segId = segmentUnit.getSegId();
    this.instanceId = segmentUnit.getInstanceId();
    this.status = segmentUnit.getStatus();
    this.archiveId = segmentUnit.getArchiveId();
    this.groupId = groupId;
    this.storagePoolId = storagePoolId;
    this.isFaked = false;
  }


  
  public SimpleSegUnitInfo(SegId segId, int groupId, long storagePoolId, InstanceId instanceId,
      long archiveId) {
    this.segId = segId;
    this.groupId = groupId;
    this.storagePoolId = storagePoolId;
    this.instanceId = instanceId;
    this.archiveId = archiveId;
    this.isFaked = true;
  }


  
  public SimpleSegUnitInfo(SegId segId, int groupId, long storagePoolId, InstanceId instanceId) {
    this.segId = segId;
    this.groupId = groupId;
    this.storagePoolId = storagePoolId;
    this.instanceId = instanceId;
    this.archiveId = FAKED_ARCHIVE_ID;
    this.isFaked = true;
  }

  public boolean hasArchiveId() {
    return archiveId != FAKED_ARCHIVE_ID;
  }


  
  public boolean canBeMovedTo(InstanceInfo destination) {
    // check status
    if (this.getStatus() == SegmentUnitStatus.Primary
        || this.getStatus() == SegmentUnitStatus.PrePrimary
        || this.getStatus() == SegmentUnitStatus.Deleting
        || this.getStatus() == SegmentUnitStatus.Deleted) {
      logger.debug("the segment unit's status is {}", this.getStatus());
      return false;
    }

    SimpleSegmentInfo segment = this.getSegment();
    Collection<SimpleSegUnitInfo> members = segment.getSegUnits();
    // membership stable
    if (members.size() != segment.getVolume().getVolumeType().getNumMembers()) {
      logger.debug("the segment unit's membership is not full {}", members);
      return false;
    }

    // check if the destination is in the proper group and other member's status
    int destinationGroupId = destination.getGroupId();
    boolean hasPrimary = false;
    for (SimpleSegUnitInfo member : members) {
      if (member == this) {
        continue;
      } else {
        if (member.getStatus() == SegmentUnitStatus.Primary) {
          hasPrimary = true;
        }
        if (member.getStatus() != SegmentUnitStatus.Primary
            && member.getStatus() != SegmentUnitStatus.Arbiter
            && member.getStatus() != SegmentUnitStatus.Secondary) {
          logger.debug("one of the other member is {}", member.getStatus());
          return false;
        }
        if (member.getGroupId() == destinationGroupId) {
          logger.debug("one of the member's group mismatch");
          return false;
        }
      }
    }

    if (!hasPrimary) {
      logger.debug("there is no primary in the membership !");
      return false;
    }

    logger.debug("this segment unit is perfect !");
    return true;
  }

  public void freeMySelf() {
    segment.removeSegmentUnit(this);
  }

  public int getGroupId() {
    return groupId;
  }

  public void setGroupId(int groupId) {
    this.groupId = groupId;
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(long archiveId) {
    this.archiveId = archiveId;
  }

  public boolean isFaked() {
    return isFaked;
  }

  public void setFaked(boolean isFaked) {
    this.isFaked = isFaked;
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public SegId getSegId() {
    return segId;
  }

  public SimpleSegmentInfo getSegment() {
    return segment;
  }

  public void setSegment(SimpleSegmentInfo segment) {
    this.segment = segment;
  }

  public SegmentUnitStatus getStatus() {
    return status;
  }

  public void setStatus(SegmentUnitStatus status) {
    this.status = status;
  }

  public SegmentUnitMetadata getSegmentUnit() {
    return realSegUnit;
  }

  @Override
  public String toString() {
    return "[" + getClass().getSimpleName() + " segId=" + segId + ", instanceId=" + instanceId
        + ", archiveId="
        + archiveId + ", status=" + status + "]";
  }
}
