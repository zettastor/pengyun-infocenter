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
