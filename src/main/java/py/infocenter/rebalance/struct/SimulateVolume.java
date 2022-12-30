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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import py.archive.segment.SegmentMetadata;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * volume simulation.
 */
public class SimulateVolume {

  private final long poolId;
  private final long volumeId;
  private final VolumeType volumeType;
  private final int segmentCount;
  private final int segmentWrapperCount;
  private Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = new HashMap<>();

  private ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();

  private ObjectCounter<Long> secondaryCounter = new TreeSetObjectCounter<>();

  private ObjectCounter<Long> arbiterCounter = new TreeSetObjectCounter<>();

  private Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap = new HashMap<>();

  private Map<Long, ObjectCounter<Long>> primary2SecondaryCounterMap = new HashMap<>();


  public SimulateVolume(VolumeMetadata volumeMetadata) {
    this.poolId = volumeMetadata.getStoragePoolId();
    this.volumeId = volumeMetadata.getVolumeId();
    this.volumeType = volumeMetadata.getVolumeType();
    this.segmentCount = volumeMetadata.getSegmentCount();
    this.segmentWrapperCount = volumeMetadata.getSegmentWrappCount();

    if (volumeMetadata.getSegmentTable() != null) {
      for (int segIndex : volumeMetadata.getSegmentTable().keySet()) {
        SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(segIndex);
        segIndex2SimulateSegmentMap.put(segIndex, new SimulateSegment(segmentMetadata));

        //get P\S distribution
        parseVolumeInfo(segIndex, segmentMetadata);
      }
    }
  }

  private void parseVolumeInfo(long segIndex, SegmentMetadata segmentMetadata) {
    SegmentMembership membership = segmentMetadata.getLatestMembership();


    long wrapperIndex = segIndex / segmentWrapperCount;
    ObjectCounter<Long> normalOfWrapperCounter = wrapperIndex2NormalCounterMap
        .computeIfAbsent(wrapperIndex, value -> new TreeSetObjectCounter<>());

    long primaryId = membership.getPrimary().getId();
    primaryCounter.increment(primaryId);
    normalOfWrapperCounter.increment(primaryId);

    //save secondary combinations of primary
    ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap
        .computeIfAbsent(primaryId, value -> new TreeSetObjectCounter<>());
    for (InstanceId secondaryIdObj : membership.getSecondaries()) {
      secondaryCounter.increment(secondaryIdObj.getId());
      secondaryOfPrimaryCounter.increment(secondaryIdObj.getId());
      normalOfWrapperCounter.increment(secondaryIdObj.getId());
    }

    for (InstanceId arbiterIdObj : membership.getArbiters()) {
      arbiterCounter.increment(arbiterIdObj.getId());
    }
  }

  public int getSegmentCount() {
    return segmentCount;
  }

  public long getPoolId() {
    return poolId;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public Map<Integer, SimulateSegment> getSegIndex2SimulateSegmentMap() {
    return segIndex2SimulateSegmentMap;
  }

  public VolumeType getVolumeType() {
    return volumeType;
  }

  public ObjectCounter<Long> getPrimaryCounter() {
    return primaryCounter;
  }

  public ObjectCounter<Long> getSecondaryCounter() {
    return secondaryCounter;
  }

  public ObjectCounter<Long> getArbiterCounter() {
    return arbiterCounter;
  }

  public Map<Long, ObjectCounter<Long>> getWrapperIndex2NormalCounterMap() {
    return wrapperIndex2NormalCounterMap;
  }

  public Map<Long, ObjectCounter<Long>> getPrimary2SecondaryCounterMap() {
    return primary2SecondaryCounterMap;
  }

  /**
   * primary virtual migrate.
   *
   * @param segIndex    segment index of volume
   * @param source      migrate from
   * @param destination migrate to
   */
  public void migratePrimary(int segIndex, long source, long destination) {
    SimulateSegment segment = segIndex2SimulateSegmentMap.get(segIndex);

    final long primaryIdBeforeMigrate = segment.getPrimaryId().getId();
    Set<Long> secondarySetBeforeMigrate = new HashSet<>();          //must use long, not 
    // InstanceId, because segment's instanceId will changed below
    for (InstanceId secondaryId : segment.getSecondaryIdSet()) {
      secondarySetBeforeMigrate.add(secondaryId.getId());
    }

    segment.migratePrimary(new InstanceId(source), new InstanceId(destination));

    final long primaryIdAfterMigrate = segment.getPrimaryId().getId();
    Set<Long> secondarySetAfterMigrate = new HashSet<>();
    for (InstanceId secondaryId : segment.getSecondaryIdSet()) {
      secondarySetAfterMigrate.add(secondaryId.getId());
    }

    //primary counter update
    primaryCounter.decrement(source);
    primaryCounter.increment(destination);

    //secondary counter update
    secondaryCounter.decrement(destination);
    secondaryCounter.increment(source);

    //secondary of primary update
    ObjectCounter<Long> secondaryOfPrimaryCounterBeforeMigrate = primary2SecondaryCounterMap
        .get(primaryIdBeforeMigrate);
    for (long instanceId : secondarySetBeforeMigrate) {
      secondaryOfPrimaryCounterBeforeMigrate.decrement(instanceId);
    }

    ObjectCounter<Long> secondaryOfPrimaryCounterAfterMigrate = primary2SecondaryCounterMap
        .get(primaryIdAfterMigrate);
    for (long instanceId : secondarySetAfterMigrate) {
      secondaryOfPrimaryCounterAfterMigrate.increment(instanceId);
    }
  }

  /**
   * secondary virtual migrate.
   *
   * @param segIndex    segment index of volume
   * @param source      migrate from
   * @param destination migrate to
   */
  public void migrateSecondary(int segIndex, long source, long destination) {
    SimulateSegment segment = segIndex2SimulateSegmentMap.get(segIndex);
    segment.migrateSecondary(new InstanceId(source), new InstanceId(destination));

    //secondary counter update
    secondaryCounter.decrement(source);
    secondaryCounter.increment(destination);

    //secondary of primary update
    long primaryId = segment.getPrimaryId().getId();
    ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap.get(primaryId);
    secondaryOfPrimaryCounter.decrement(source);
    secondaryOfPrimaryCounter.increment(destination);

    //wrapper count update
    long wrapperIndex = segIndex / segmentWrapperCount;
    ObjectCounter<Long> normalCounter = wrapperIndex2NormalCounterMap.get(wrapperIndex);
    normalCounter.decrement(source);
    normalCounter.increment(destination);
  }

  /**
   * arbiter virtual migrate.
   *
   * @param segIndex    segment index of volume
   * @param source      migrate from
   * @param destination migrate to
   */
  public void migrateArbiter(int segIndex, long source, long destination) {
    SimulateSegment segment = segIndex2SimulateSegmentMap.get(segIndex);
    segment.migrateArbiter(new InstanceId(source), new InstanceId(destination));

    //secondary counter update
    arbiterCounter.decrement(source);
    arbiterCounter.increment(destination);
  }

  @Override
  public String toString() {
    return "SimulateVolume{"

        + "poolId=" + poolId

        + ", volumeId=" + volumeId

        + ", volumeType=" + volumeType

        + ", segmentCount=" + segmentCount

        + ", primaryCounter=" + primaryCounter

        + ", secondaryCounter=" + secondaryCounter

        + ", arbiterCounter=" + arbiterCounter

        + ", primary2SecondaryCounterMap=" + primary2SecondaryCounterMap

        + '}';
  }
}
