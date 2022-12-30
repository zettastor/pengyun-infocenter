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

import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.instance.InstanceId;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeMetadata.VolumeSourceType;
import py.volume.VolumeType;

/**
 * segment unit simulation.
 */
public class SimulateSegmentUnit {

  private final InstanceId instanceId;
  private final SegId segId;
  private final long archiveId;
  private final VolumeType volumeType;
  protected SegmentUnitType segmentUnitType;
  private SegmentUnitStatus status;
  private boolean enableLaunchMultiDrivers;
  private VolumeMetadata.VolumeSourceType volumeSource;


  
  public SimulateSegmentUnit(InstanceId instanceId, SegId segId, long archiveId,
      VolumeType volumeType, SegmentUnitStatus status) {
    this.instanceId = instanceId;
    this.segId = segId;
    this.archiveId = archiveId;
    this.status = status;
    this.volumeType = volumeType;
  }


  public SimulateSegmentUnit(InstanceId instanceId, SimulateSegmentUnit simulateSegmentUnit) {
    this.instanceId = instanceId;
    this.segId = simulateSegmentUnit.getSegId();
    this.archiveId = simulateSegmentUnit.getArchiveId();
    this.status = simulateSegmentUnit.getStatus();
    this.volumeType = simulateSegmentUnit.getVolumeType();
  }


  public SimulateSegmentUnit(SegmentUnitMetadata segmentUnitMetadata) {
    this.instanceId = segmentUnitMetadata.getInstanceId();
    this.segId = segmentUnitMetadata.getSegId();
    this.archiveId = segmentUnitMetadata.getArchiveId();
    this.status = segmentUnitMetadata.getStatus();
    this.volumeType = segmentUnitMetadata.getVolumeType();
    this.enableLaunchMultiDrivers = segmentUnitMetadata.isEnableLaunchMultiDrivers();
    this.volumeSource = segmentUnitMetadata.getVolumeSource();
    this.segmentUnitType = segmentUnitMetadata.getSegmentUnitType();
  }

  public InstanceId getInstanceId() {
    return instanceId;
  }

  public SegId getSegId() {
    return segId;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public SegmentUnitStatus getStatus() {
    return status;
  }

  public void setStatus(SegmentUnitStatus status) {
    this.status = status;
  }

  public boolean isEnableLaunchMultiDrivers() {
    return enableLaunchMultiDrivers;
  }

  public void setEnableLaunchMultiDrivers(boolean enableLaunchMultiDrivers) {
    this.enableLaunchMultiDrivers = enableLaunchMultiDrivers;
  }

  public VolumeSourceType getVolumeSource() {
    return volumeSource;
  }

  public void setVolumeSource(VolumeSourceType volumeSource) {
    this.volumeSource = volumeSource;
  }


  public SegmentUnitType getSegmentUnitType() {
    return segmentUnitType;
  }

  public void setSegmentUnitType(SegmentUnitType segmentUnitType) {
    this.segmentUnitType = segmentUnitType;
  }

  @Override
  public String toString() {
    return "SimulateSegmentUnit{"
        + "instanceId=" + instanceId
        + ", segId=" + segId
        + ", archiveId=" + archiveId
        + ", volumeType=" + volumeType
        + ", status=" + status
        + ", enableLaunchMultiDrivers=" + enableLaunchMultiDrivers
        + ", volumeSource=" + volumeSource
        + ", segmentUnitType=" + segmentUnitType
        + '}';
  }
}
