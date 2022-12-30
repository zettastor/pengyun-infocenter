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

package py.infocenter.rebalance.selector;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegId;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.struct.BaseRebalanceTask;
import py.infocenter.rebalance.struct.InternalRebalanceTask;
import py.infocenter.rebalance.struct.SimulateInstanceInfo;
import py.infocenter.rebalance.struct.SimulateSegment;
import py.infocenter.rebalance.struct.SimulateVolume;
import py.instance.InstanceId;
import py.thrift.infocenter.service.SegmentNotFoundExceptionThrift;
import py.volume.VolumeMetadata;

/**
 * select rebalance task by current pool environment and volume segment unit distribution.
 */
public class VolumeRebalanceSelector {

  private static final Logger logger = LoggerFactory.getLogger(VolumeRebalanceSelector.class);
  private final RebalanceConfiguration config = RebalanceConfiguration.getInstance();
  private final VolumeMetadata realVolume;      //volume information
  private final SimulateInstanceBuilder simulateInstanceBuilder;
  private Map<Long, SimulateInstanceInfo> canUsedInstanceId2SimulateInstanceMap;
  private Map<Long, InstanceMetadata> instanceId2InstanceMetadataMap = new HashMap<>(); //all 
  // instance
  private Multimap<Integer, Long> groupId2InstanceIdMap = Multimaps
      .synchronizedSetMultimap(HashMultimap.create());
  private SimulateVolume simulateVolume;  //simple volume's information
  private ObjectCounter<Long> primaryCounter = new TreeSetObjectCounter<>();      //primary 
  // segment unit counter in volume
  private ObjectCounter<Long> secondaryCounter = new TreeSetObjectCounter<>();    //get secondary
  // segment unit counter in volume
  private ObjectCounter<Long> arbiterCounter = new TreeSetObjectCounter<>();    //get arbiter 
  // segment unit counter in volume
  private Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap = new HashMap<>();
  //get normal segment unit counter of wrapper index in volume(used for check overload)
  private Map<Long, ObjectCounter<Long>> primary2SecondaryCounterMap = new HashMap<>();
  //get secondary combinations of primary in volume, <primary, secondary combinations>
  private boolean isVolumeParsed = false;
  private ObjectCounter<Long> primaryExpectCounter = new TreeSetObjectCounter<>();
  private ObjectCounter<Long> arbiterExpiredCounter = new TreeSetObjectCounter<>();
  private Map<Long, ObjectCounter<Long>> secondaryOfPrimaryExpectCounterMap = new HashMap<>();
  //<primaryId, secondaryOfPrimaryExpiredCounter>


  public VolumeRebalanceSelector(SimulateInstanceBuilder simulateInstanceBuilder,
      VolumeMetadata volumeMetadata,
      List<InstanceMetadata> instanceMetadataList) {
    this.simulateInstanceBuilder = simulateInstanceBuilder;
    //this.canUsedInstanceId2SimulateInstanceMap = new HashMap<>(simulateInstanceBuilder
    // .getInstanceId2SimulateInstanceMap());
    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      instanceId2InstanceMetadataMap
          .put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
      groupId2InstanceIdMap
          .put(instanceMetadata.getGroup().getGroupId(), instanceMetadata.getInstanceId().getId());
    }
    this.realVolume = volumeMetadata;
    this.simulateVolume = new SimulateVolume(volumeMetadata);

    this.canUsedInstanceId2SimulateInstanceMap = simulateInstanceBuilder
        .removeCanNotUsedInstance(simulateVolume.getVolumeType());
  }


  public SimulateVolume getSimulateVolume() {
    return simulateVolume;
  }


  private boolean isPrimaryNeedToDoRebalance(boolean isByThreshold) {
    int segmentCount = realVolume.getSegmentCount();

    long maxDistributeCount = 0;
    long minDistributeCount = 0;
    boolean firstLoop = true;

    //calculate desired primary count by datanode weight
    for (long instanceId : canUsedInstanceId2SimulateInstanceMap.keySet()) {
      long factCount = primaryCounter.get(instanceId);
      Validate.isTrue(factCount >= 0);

      if (firstLoop) {
        minDistributeCount = factCount;
        maxDistributeCount = factCount;
        firstLoop = false;
      } else {
        maxDistributeCount = Math.max(maxDistributeCount, factCount);
        minDistributeCount = Math.min(minDistributeCount, factCount);
      }
    }
    if (firstLoop) {
      logger.warn("not found any used instance when check primary, no need do rebalance.");
      return true;
    }

    long diffBalanceCount = Math.abs(maxDistributeCount - minDistributeCount);

    if (isByThreshold) {
      double threshold = config.getPressureThreshold();
      long accuracy = (long) Math.pow(10, config.getPressureThresholdAccuracy());
      if ((int) ((double) diffBalanceCount / segmentCount * accuracy) > (int) (threshold
          * accuracy)) {
        logger.warn(
            "The difference of primary distribution between max distribute count and min "
                + "distribute count is over threshold, will caught rebalance. max distribute "
                + "count:{}, min distribute count:{}, segment count:{}, threshold:{}",
            maxDistributeCount, minDistributeCount, segmentCount, threshold);
        return true;
      }
    } else if (diffBalanceCount > 0) {
      logger.info(
          "The difference of primary distribution between max distribute count and min distribute"
              + " count is bigger than 0, will caught rebalance. max distribute count:{}, min "
              + "distribute count:{}, segment count:{}",
          maxDistributeCount, minDistributeCount, segmentCount);
      return true;
    }

    logger.debug("primary distribute very balance, no need to do rebalance");
    return false;
  }


  private boolean isSecondaryOfPrimaryNeedToDoRebalance(long primaryId,
      ObjectCounter<Long> secondaryOfPrimaryCounter, Collection<Long> instanceIdList,
      Set<Long> excludeInstanceIdSet, long segmentCount, boolean isByThreshold) {
    ObjectCounter<Long> secondaryId2DesiredCounter = secondaryOfPrimaryExpectCounterMap
        .get(primaryId);

    long maxDistributeCount = 0;
    long minDistributeCount = 0;
    boolean firstLoop = true;

    for (long instanceId : instanceIdList) {
      if (excludeInstanceIdSet.contains(instanceId)) {
        continue;
      }

      long factCount = secondaryOfPrimaryCounter.get(instanceId);
      Validate.isTrue(factCount >= 0);
      if (firstLoop) {
        minDistributeCount = factCount;
        maxDistributeCount = factCount;
        firstLoop = false;
      } else {
        maxDistributeCount = Math.max(maxDistributeCount, factCount);
        minDistributeCount = Math.min(minDistributeCount, factCount);
      }
    }
    if (firstLoop) {
      logger.warn("not found any used instance when check secondary, no need do rebalance.");
      return true;
    }

    long diffBalanceCount = Math.abs(maxDistributeCount - minDistributeCount);

    if (isByThreshold) {
      double threshold = config.getPressureThreshold();
      long accuracy = (long) Math.pow(10, config.getPressureThresholdAccuracy());
      if ((int) ((double) diffBalanceCount / segmentCount * accuracy) > (int) (threshold
          * accuracy)) {
        logger.warn(
            "The difference of secondary when primary is {} distribution between max distribute "
                + "count and min distribute count is over threshold, will caught rebalance. max "
                + "distribute count:{}, min distribute count:{}, segment count:{}, threshold:{}",
            primaryId, maxDistributeCount, minDistributeCount, segmentCount, threshold);
        return true;
      }
    } else if (diffBalanceCount > 0) {
      logger.info(
          "The difference of secondary when primary is {} distribution between max distribute "
              + "count and min distribute count is bigger than 0, will caught rebalance. max "
              + "distribute count:{}, min distribute count:{}, segment count:{}",
          primaryId, maxDistributeCount, minDistributeCount, segmentCount);
      return true;
    }
    return false;
  }


  private boolean isPsNeedToDoRebalance() {
    int segmentCount = realVolume.getSegmentCount();

    //calculate desired primary count by datanode weight
    for (SimulateInstanceInfo primaryInfo : canUsedInstanceId2SimulateInstanceMap.values()) {
      long primaryId = primaryInfo.getInstanceId().getId();

      ObjectCounter<Long> secondaryOfPrimaryCount = primary2SecondaryCounterMap
          .get(primaryInfo.getInstanceId().getId());

      HashSet<Long> excludeInstanceIdSet = new HashSet<>(
          groupId2InstanceIdMap.get(primaryInfo.getGroupId()));

      if (isSecondaryOfPrimaryNeedToDoRebalance(primaryId, secondaryOfPrimaryCount,
          canUsedInstanceId2SimulateInstanceMap.keySet(),
          excludeInstanceIdSet, segmentCount, true)) {
        logger.warn(
            "The difference of secondary distribution is over threshold when primary is {}, will "
                + "caught rebalance",
            primaryInfo.getInstanceId().getId());
        return true;
      }
    }

    logger.debug("PS distribute very balance, no need to do rebalance");
    return false;
  }

  private boolean isArbiterNeedToDoRebalance(boolean isByThreshold) {
    if (canUsedInstanceId2SimulateInstanceMap.isEmpty()) {
      return false;
    }

    int segmentCount = realVolume.getSegmentCount();

    //calculate desired arbiter count by datanode weight
    for (long instanceId : arbiterCounter.getAll()) {
      long factCount = arbiterCounter.get(instanceId);
      long maxDesiredCount = arbiterExpiredCounter.get(instanceId);

      long diffBalanceCount = Math.abs(factCount - maxDesiredCount);

      if (isByThreshold) {
        double threshold = config.getPressureThreshold();
        long accuracy = (long) Math.pow(10, config.getPressureThresholdAccuracy());
        if ((int) ((double) diffBalanceCount / segmentCount * accuracy) > (int) (threshold
            * accuracy)) {
          logger.warn(
              "The difference of arbiter:{} distribution between fact count and desired count is "
                  + "over threshold, will caught rebalance. desired count:{}, fact count:{}, "
                  + "segment count:{}, threshold:{}",
              instanceId, maxDesiredCount, factCount, segmentCount, threshold);
          return true;
        }
      } else if (diffBalanceCount > 0) {
        logger.info(
            "The difference of arbiter:{} distribution between fact count and desired count is "
                + "bigger than 0, will caught rebalance. desired count:{}, fact count:{}, segment"
                + " count:{}",
            instanceId, maxDesiredCount, factCount, segmentCount);
        return true;
      }
    }

    logger.debug("arbiter distribute very balance, no need to do rebalance");
    return false;
  }

  /**
   * check volume is need to do rebalance by P,S distribution arbiter distribution not care.
   *
   * @return true if need to do rebalance
   */
  public boolean isNeedToDoRebalance() {
    int segmentCount = realVolume.getSegmentCount();
    if (segmentCount < config.getMinOfSegmentCountCanDoRebalance()) {
      logger.info("volume:{} just have {} segment, no need do rebalance", realVolume.getVolumeId(),
          segmentCount);
      return false;
    }

    // is primary need to do rebalance
    if (isPrimaryNeedToDoRebalance(true)) {
      return true;
    }

    // is PS combinations distribute need to do rebalance
    return isPsNeedToDoRebalance();
  }

  /**
   * print log that rebalance result result.
   */
  public void printRebalanceResultLog() {
    for (SimulateInstanceInfo simulateInstanceInfo : canUsedInstanceId2SimulateInstanceMap
        .values()) {
      logger.warn("Instance:{} type:{} real weight:{} used weight:{}",
          simulateInstanceInfo.getInstanceId(),
          simulateInstanceInfo.getDatanodeType(), simulateInstanceInfo.getRealWeight(),
          simulateInstanceInfo.getWeight());
    }

    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    Map<Long, ObjectCounter<Long>> wrapperIndex2NormalIdCounterMap = new HashMap<>();
    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

    Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume
        .getSegIndex2SimulateSegmentMap();
    //volume segment traversal
    for (int segmentIndex : segIndex2SimulateSegmentMap.keySet()) {
      SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);

      // calculate wrapper index
      // we will record normal segment unit counter of data node to used for overload
      long wrapperIndex = segmentIndex / realVolume.getSegmentWrappCount();
      ObjectCounter<Long> normalOfWrapperCounter = wrapperIndex2NormalIdCounterMap
          .computeIfAbsent(wrapperIndex, value -> new TreeSetObjectCounter<>());

      long primaryId = simulateSegment.getPrimaryId().getId();
      primaryIdCounter.increment(primaryId);
      normalOfWrapperCounter.increment(primaryId);

      //save secondary combinations of primary
      ObjectCounter<Long> secondaryOfPrimaryCounter = primaryId2SecondaryIdCounterMap
          .computeIfAbsent(primaryId, value -> new TreeSetObjectCounter<>());
      for (InstanceId secondaryIdObj : simulateSegment.getSecondaryIdSet()) {
        secondaryIdCounter.increment(secondaryIdObj.getId());
        secondaryOfPrimaryCounter.increment(secondaryIdObj.getId());
        normalOfWrapperCounter.increment(secondaryIdObj.getId());
      }

      for (InstanceId arbiterIdObj : simulateSegment.getArbiterIdSet()) {
        arbiterIdCounter.increment(arbiterIdObj.getId());
      }
    }

    logger.warn("volume:{} rebalance expire of primary distribution: {}",
        simulateVolume.getVolumeId(), primaryExpectCounter);
    logger.warn("volume:{} rebalance result of primary distribution: {}",
        simulateVolume.getVolumeId(), primaryCounter);
    logger.warn("volume:{} rebalance result of secondary distribution: {}",
        simulateVolume.getVolumeId(), secondaryCounter);
    logger.warn("volume:{} rebalance result of arbiter distribution: {}",
        simulateVolume.getVolumeId(), arbiterCounter);
    logger.warn(
        "volume:{} rebalance result of secondary when primary down(primaryId, secondary "
            + "distribution): {}",
        simulateVolume.getVolumeId(), primary2SecondaryCounterMap);
    logger.warn(
        "volume:{} rebalance expire of secondary when primary down(primaryId, secondary "
            + "distribution): {}",
        simulateVolume.getVolumeId(), secondaryOfPrimaryExpectCounterMap);
    //logger.debug("rebalance result of wrapper counter(wrapperIndex, normal distribution): {}", 
    // wrapperIndex2NormalCounterMap);
    logger.warn("volume:{} rebalance simulate result of primary distribution: {}",
        simulateVolume.getVolumeId(), primaryIdCounter);
    logger.warn("volume:{} rebalance simulate result of secondary distribution: {}",
        simulateVolume.getVolumeId(), secondaryIdCounter);
    logger.warn("volume:{} rebalance simulate result of arbiter distribution: {}",
        simulateVolume.getVolumeId(), arbiterIdCounter);
    logger.warn(
        "volume:{} rebalance simulate result of secondary when primary down(primaryId, secondary "
            + "distribution): {}",
        simulateVolume.getVolumeId(), primaryId2SecondaryIdCounterMap);
    //logger.debug("rebalance simulate result of wrapper counter(wrapperIndex, normal 
    // distribution): {}", wrapperIndex2NormalIdCounterMap);
  }


  public List<InternalRebalanceTask> selectPrimaryRebalanceTask() throws NoNeedToRebalance {
    if (canUsedInstanceId2SimulateInstanceMap.isEmpty()) {
      throw new NoNeedToRebalance();
    }

    //is need to do rebalance
    if (!isPrimaryNeedToDoRebalance(false)) {
      logger.info("Primary distribute very balance, no need to rebalance");
      throw new NoNeedToRebalance();
    }

    List<InternalRebalanceTask> selTaskList = new LinkedList<>();
    int segmentCount = realVolume.getSegmentCount();

    //get primary distribution priority list(distribute ratio from less to more)
    LinkedList<Long> primaryPriorityList = simulateInstanceBuilder
        .getPrimaryPriorityList(canUsedInstanceId2SimulateInstanceMap.keySet(),
            primaryCounter, primaryExpectCounter, segmentCount);
    boolean hasMigrateSrc = true;
    boolean hasMigrateDest = true;
    Set<Long> migrateSrcInsIdSet = new HashSet<>();
    List<Long> migrateDestInsIdList = new ArrayList<>();
    while (!primaryPriorityList.isEmpty() && (hasMigrateSrc || hasMigrateDest)) {
      //sort from more to less
      if (hasMigrateSrc) {
        long primaryId = primaryPriorityList.pollLast();


        long desiredCountOfPrimaryId = primaryExpectCounter.get(primaryId);
        long factCountOfPrimaryId = primaryCounter.get(primaryId);
        if (factCountOfPrimaryId > desiredCountOfPrimaryId) {
          migrateSrcInsIdSet.add(primaryId);
        } else {
          hasMigrateSrc = false;
        }
      }

      //sort from less to more
      if (hasMigrateDest && !primaryPriorityList.isEmpty()) {


        long primaryId = primaryPriorityList.pollFirst();
        long desiredCountOfPrimaryId = primaryExpectCounter.get(primaryId);
        long factCountOfPrimaryId = primaryCounter.get(primaryId);
        if (factCountOfPrimaryId < desiredCountOfPrimaryId) {
          migrateDestInsIdList.add(primaryId);
        } else {
          hasMigrateDest = false;
        }
      }
    }

    if (migrateSrcInsIdSet.size() == 0 || migrateDestInsIdList.size() == 0) {
      logger.info(
          "Primary distribute not balance, but not find suitable step to rebalance. "
              + "primaryCount:{}",
          primaryCounter);
      throw new NoNeedToRebalance();
    }

    Set<Long> migrateDestInsIdSet = new HashSet<>(migrateDestInsIdList);


    Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume
        .getSegIndex2SimulateSegmentMap();
    List<Integer> segIndexSet = new LinkedList<>(segIndex2SimulateSegmentMap.keySet());
    Collections.shuffle(segIndexSet);
    for (int segmentIndex : segIndexSet) {
      SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);

      long migrateSrcId = simulateSegment.getPrimaryId().getId();
      if (!migrateSrcInsIdSet.contains(migrateSrcId)) {
        continue;
      }

      boolean hasFoundDest = false;
      int minMigrateDestIdIndex = migrateDestInsIdList.size() - 1;
      for (InstanceId insId : simulateSegment.getSecondaryIdSet()) {
        if (!migrateDestInsIdSet.contains(insId.getId())) {
          continue;
        }

        minMigrateDestIdIndex = Math
            .min(minMigrateDestIdIndex, migrateDestInsIdList.indexOf(insId.getId()));
        hasFoundDest = true;
      }

      if (hasFoundDest) {
        InternalRebalanceTask task = new InternalRebalanceTask(simulateSegment.getSegId(),
            simulateSegment.getPrimaryId(),
            new InstanceId(migrateDestInsIdList.get(minMigrateDestIdIndex)),
            0L, BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance);
        virtualMigrate(task);
        selTaskList.add(task);
        break;
      }
    }

    if (selTaskList.isEmpty()) {
      logger.info(
          "Primary distribute not balance, but not find suitable step to rebalance. "
              + "primaryCount:{}",
          primaryCounter);
      throw new NoNeedToRebalance();
    }

    return selTaskList;
  }


  public List<InternalRebalanceTask> selectPsRebalanceTask() throws NoNeedToRebalance {
    if (canUsedInstanceId2SimulateInstanceMap.isEmpty()) {
      throw new NoNeedToRebalance();
    }

    List<InternalRebalanceTask> selTaskList = new LinkedList<>();
    //datanode traversal
    for (long primaryId : primary2SecondaryCounterMap.keySet()) {
      InstanceMetadata primaryInfo = instanceId2InstanceMetadataMap.get(primaryId);
      ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap.get(primaryId);

      long primaryFactCount = primaryCounter.get(primaryId);
      long secondaryCountPerSegment = realVolume.getVolumeType().getNumSecondaries();
      long segmentCount = realVolume.getSegmentCount();

      HashSet<Long> excludeInstanceIdSet = new HashSet<>(
          groupId2InstanceIdMap.get(primaryInfo.getGroup().getGroupId()));

      if (!isSecondaryOfPrimaryNeedToDoRebalance(primaryId, secondaryOfPrimaryCounter,
          canUsedInstanceId2SimulateInstanceMap.keySet(),
          excludeInstanceIdSet, segmentCount, false)) {
        logger.info("secondary distribute very balance when primary is {}", primaryId);
        continue;
      }

      //get expired count

      ObjectCounter<Long> secondaryIdOfPrimary2DesiredCounter = secondaryOfPrimaryExpectCounterMap
          .get(primaryId);

      //get secondary distribution priority list(distribute ratio from less to more)
      LinkedList<Long> secondaryOfPrimaryPriorityList = simulateInstanceBuilder
          .getSecondaryPriorityList(
              canUsedInstanceId2SimulateInstanceMap.keySet(), primaryId, primaryFactCount,
              secondaryOfPrimaryCounter, secondaryCounter, secondaryIdOfPrimary2DesiredCounter,
              segmentCount, secondaryCountPerSegment);

      //instance in middle of primaryPriorityList may be no need to migrate, so we can poll it 
      // from before, and from tail
      boolean hasMigrateSrc = true;
      boolean hasMigrateDest = true;
      LinkedList<Long> migrateSrcInsIdList = new LinkedList<>();
      LinkedList<Long> migrateDestInsIdList = new LinkedList<>();
      while (!secondaryOfPrimaryPriorityList.isEmpty() && (hasMigrateSrc || hasMigrateDest)) {
        //sort from more to less
        if (hasMigrateSrc) {
          long secondaryId = secondaryOfPrimaryPriorityList.pollLast();


          long desiredCountOfPrimaryId = secondaryIdOfPrimary2DesiredCounter.get(secondaryId);
          long factCountOfPrimaryId = secondaryOfPrimaryCounter.get(secondaryId);
          if (factCountOfPrimaryId > desiredCountOfPrimaryId) {
            migrateSrcInsIdList.add(secondaryId);
          } else if (factCountOfPrimaryId < desiredCountOfPrimaryId) {
            migrateDestInsIdList.add(secondaryId);
          } else {
            hasMigrateSrc = false;
          }
        }

        //sort from less to more
        if (hasMigrateDest && !secondaryOfPrimaryPriorityList.isEmpty()) {
          long secondaryId = secondaryOfPrimaryPriorityList.pollFirst();
          long desiredCountOfPrimaryId = secondaryIdOfPrimary2DesiredCounter.get(secondaryId);
          long factCountOfPrimaryId = secondaryOfPrimaryCounter.get(secondaryId);
          if (factCountOfPrimaryId < desiredCountOfPrimaryId) {
            migrateDestInsIdList.add(secondaryId);
          } else if (factCountOfPrimaryId > desiredCountOfPrimaryId) {
            migrateSrcInsIdList.add(secondaryId);
          } else {
            hasMigrateDest = false;
          }
        }
      }

      if (migrateSrcInsIdList.size() == 0 || migrateDestInsIdList.size() == 0) {
        logger.info(
            "secondary of Primary:{} distribute not balance, but not find suitable step to "
                + "rebalance. primaryCount:{}",
            primaryId, primaryCounter);
        continue;
      }

      long migrateSrcId = -1;
      long migrateDestId = -1;
      InternalRebalanceTask overloadTask = null;
      //get migrate object by memberships
      Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume
          .getSegIndex2SimulateSegmentMap();
      List<Integer> segIndexSet = new LinkedList<>(segIndex2SimulateSegmentMap.keySet());
      Collections.shuffle(segIndexSet);
      for (int segmentIndex : segIndexSet) {
        SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);

        long wrapperIndex = segmentIndex / realVolume.getSegmentWrappCount();
        ObjectCounter<Long> normalOfWrapperCounter = wrapperIndex2NormalCounterMap
            .get(wrapperIndex);

        if (primaryId != simulateSegment.getPrimaryId().getId()) {
          continue;
        }

        Set<Integer> usedGroupIdSet = new HashSet<>();
        Set<Long> secondarySet = new HashSet<>();
        for (InstanceId secondary : simulateSegment.getSecondaryIdSet()) {
          secondarySet.add(secondary.getId());
          int groupId = canUsedInstanceId2SimulateInstanceMap.get(secondary.getId()).getGroupId();
          usedGroupIdSet.add(groupId);
        }

        Set<Long> arbiterSet = new HashSet<>();
        for (InstanceId arbiter : simulateSegment.getArbiterIdSet()) {
          arbiterSet.add(arbiter.getId());
          int groupId = canUsedInstanceId2SimulateInstanceMap.get(arbiter.getId()).getGroupId();
          usedGroupIdSet.add(groupId);
        }

        Set<Long> srcSet = new HashSet<>(secondarySet);
        srcSet.retainAll(migrateSrcInsIdList);
        Set<Long> destSet = new HashSet<>(migrateDestInsIdList);
        destSet.removeIf(id -> secondarySet.contains(id) || arbiterSet.contains(id));
        if (srcSet.size() <= 0 || destSet.size() <= 0) {
          continue;
        }

        //from more to less
        for (long srcId : migrateSrcInsIdList) {
          if (!srcSet.contains(srcId)) {
            continue;
          }

          migrateSrcId = srcId;

          //get src instance group id
          int srcGroupId = canUsedInstanceId2SimulateInstanceMap.get(migrateSrcId).getGroupId();

          //from less to more
          for (long destId : migrateDestInsIdList) {
            if (destSet.contains(destId)) {
              //get dest instance group id
              int destGroupId = canUsedInstanceId2SimulateInstanceMap.get(destId).getGroupId();
              if (srcGroupId != destGroupId && usedGroupIdSet.contains(destGroupId)) {
                continue;
              }

              //has space?
              long freeFlexible = 0;
              long freeSpace = 0;
              InstanceMetadata instanceMetadata = instanceId2InstanceMetadataMap.get(destId);
              List<RawArchiveMetadata> rawArchiveMetadataList = instanceMetadata.getArchives();
              for (RawArchiveMetadata rawArchiveMetadata : rawArchiveMetadataList) {
                freeFlexible += rawArchiveMetadata.getFreeFlexibleSegmentUnitCount();
                freeSpace += rawArchiveMetadata.getLogicalFreeSpace();
              }

              if (freeSpace < realVolume.getSegmentSize()) {
                continue;
              }

              //is overload?
              int diskCount = canUsedInstanceId2SimulateInstanceMap.get(destId).getDiskCount();
              long normalSegUnitCountOnDestIns = normalOfWrapperCounter.get(destId);
              if (normalSegUnitCountOnDestIns >= diskCount) {
                //save srcId and destId
                if (overloadTask == null) {
                  overloadTask = new InternalRebalanceTask(simulateSegment.getSegId(),
                      new InstanceId(migrateSrcId), new InstanceId(destId), 0L,
                      BaseRebalanceTask.RebalanceTaskType.PSRebalance);
                }
                continue;
              }

              migrateDestId = destId;
              break;
            }
          }   //for (long destId : migrateDestInsIdList){

          //when src is migrateSrcId, and found suitable dest instance
          if (migrateDestId != -1) {
            break;
          }
        }   //for (long srcId : migrateSrcInsIdList){

        //if all src and des instance was not found suitable task
        if (migrateDestId == -1) {
          continue;
        }

        InternalRebalanceTask task = new InternalRebalanceTask(simulateSegment.getSegId(),
            new InstanceId(migrateSrcId), new InstanceId(migrateDestId), 0L,
            BaseRebalanceTask.RebalanceTaskType.PSRebalance);

        virtualMigrate(task);
        selTaskList.add(task);
        return selTaskList;
      }   //for (int segmentIndex : segIndexSet) {

      //if all dest datanode is overload, no need care overload
      if (overloadTask != null) {
        virtualMigrate(overloadTask);
        selTaskList.add(overloadTask);
        return selTaskList;
      }
    }   //for (long primaryId : primary2SecondaryCounterMap.keySet()){

    logger.info(
        "Secondary of primary distribute not balance, but not find suitable step to rebalance.");
    throw new NoNeedToRebalance();
  }



  public List<InternalRebalanceTask> selectArbiterRebalanceTask() throws NoNeedToRebalance {
    if (realVolume.getVolumeType().getNumArbiters() <= 0) {
      throw new NoNeedToRebalance();
    }

    //if instance contains simple datanode, don't do arbiter rebalance
    if (simulateInstanceBuilder.getSimpleGroupIdSet().size() > 0
        || simulateInstanceBuilder.getMixGroupIdSet().size() > 0) {
      throw new NoNeedToRebalance();
    }

    if (canUsedInstanceId2SimulateInstanceMap.isEmpty()) {
      throw new NoNeedToRebalance();
    }

    //is need to do rebalance
    if (!isArbiterNeedToDoRebalance(false)) {
      throw new NoNeedToRebalance();
    }

    List<InternalRebalanceTask> selTaskList = new LinkedList<>();

    Map<Long, Long> srcInsId2CanMigrateOutCountMap = new HashMap<>();
    Map<Long, Long> destInsId2CanMigrateInCountMap = new HashMap<>();
    for (long insId : arbiterCounter.getAll()) {
      long expectCount = arbiterExpiredCounter.get(insId);
      long factCount = arbiterCounter.get(insId);
      long migrateCount = expectCount - factCount;
      if (migrateCount < 0) {
        srcInsId2CanMigrateOutCountMap.put(insId, migrateCount);
      } else if (migrateCount > 0) {
        destInsId2CanMigrateInCountMap.put(insId, migrateCount);
      }
    }

    if (srcInsId2CanMigrateOutCountMap.size() == 0 || destInsId2CanMigrateInCountMap.size() == 0) {
      logger.info(
          "arbiter distribute not balance, but not find suitable step to rebalance. "
              + "arbiterCounter:{}",
          arbiterCounter);
      throw new NoNeedToRebalance();
    }

    //if maxArbiterId and minArbiterId in same group, it will migrate failed, so we don't care
    //get migrate object by memberships
    Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume
        .getSegIndex2SimulateSegmentMap();
    List<Integer> segIndexSet = new LinkedList<>(segIndex2SimulateSegmentMap.keySet());
    //Collections.shuffle(segIndexSet);
    for (int segmentIndex : segIndexSet) {
      SimulateSegment simulateSegment = segIndex2SimulateSegmentMap.get(segmentIndex);
      final long primaryId = simulateSegment.getPrimaryId().getId();
      Set<Long> secondarySet = new HashSet<>();
      for (InstanceId secondary : simulateSegment.getSecondaryIdSet()) {
        secondarySet.add(secondary.getId());
      }

      Set<Long> arbiterSet = new HashSet<>();
      for (InstanceId arbiter : simulateSegment.getArbiterIdSet()) {
        arbiterSet.add(arbiter.getId());
      }

      Set<Long> srcSet = new HashSet<>(arbiterSet);
      srcSet.retainAll(srcInsId2CanMigrateOutCountMap.keySet());
      Set<Long> destSet = new HashSet<>(destInsId2CanMigrateInCountMap.keySet());
      destSet.removeIf(
          id -> (primaryId == id) || (secondarySet.contains(id)) || (arbiterSet.contains(id)));
      // if arbiter not in migrate source list, or migrate dest instance is not empty, it means A
      // cannot be migrated
      if (srcSet.size() <= 0 || destSet.size() <= 0) {
        continue;
      }

      long migrateSrcId = srcSet.iterator().next();
      long migrateDestId = destSet.iterator().next();

      InternalRebalanceTask task = new InternalRebalanceTask(simulateSegment.getSegId(),
          new InstanceId(migrateSrcId), new InstanceId(migrateDestId), 0L,
          BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance);

      virtualMigrate(task);
      selTaskList.add(task);

      //update migrate src and dest list
      long currentMigrateCount = srcInsId2CanMigrateOutCountMap.get(migrateSrcId) + 1;
      if (currentMigrateCount == 0) {
        srcInsId2CanMigrateOutCountMap.remove(migrateSrcId);
      } else {
        srcInsId2CanMigrateOutCountMap.put(migrateSrcId, currentMigrateCount);
      }

      currentMigrateCount = destInsId2CanMigrateInCountMap.get(migrateDestId) - 1;
      if (currentMigrateCount == 0) {
        destInsId2CanMigrateInCountMap.remove(migrateDestId);
      } else {
        destInsId2CanMigrateInCountMap.put(migrateDestId, currentMigrateCount);
      }
    }   //for (int segmentIndex : segIndexSet) {

    if (selTaskList.isEmpty()) {
      logger.info(
          "arbiter distribute not balance, but not find suitable step to rebalance. "
              + "arbiterCount:{}",
          arbiterCounter);
      throw new NoNeedToRebalance();
    }

    return selTaskList;
  }

  /**
   * virtual migrate segment unit by task.
   *
   * @param task migrate method
   */
  private void virtualMigrate(InternalRebalanceTask task) {
    SegId segId = task.getSegId();
    long source = task.getSrcInstanceId().getId();
    long destination = task.getDestInstanceId().getId();

    if (task.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {
      simulateVolume.migratePrimary(segId.getIndex(), source, destination);
    } else if (task.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PSRebalance) {
      simulateVolume.migrateSecondary(segId.getIndex(), source, destination);
    } else if (task.getTaskType() == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance) {
      simulateVolume.migrateArbiter(segId.getIndex(), source, destination);
    }
  }

  /**
   * parse volume metadata, then get primary and secondary distribution.
   *
   * @return VolumeRebalanceSelector object
   * @throws SegmentNotFoundExceptionThrift when infocenter just start, volumeMetadata is not null
   *                                        because it load from DB, but segmentTable may be null
   *                                        because it load after datanode report
   */
  public VolumeRebalanceSelector parseVolumeInfo() throws SegmentNotFoundExceptionThrift {
    if (isVolumeParsed) {
      return this;
    }

    Map<Integer, SimulateSegment> segIndex2SimulateSegmentMap = simulateVolume
        .getSegIndex2SimulateSegmentMap();

    // when infocenter just start, volumeMetadata is not null because it load from DB,
    // but segmentTable may be null because it load after datanode report
    if (segIndex2SimulateSegmentMap == null || segIndex2SimulateSegmentMap.isEmpty()) {
      logger.error("Has no segment in volume:{}.",
          simulateVolume.getVolumeId());
      throw new SegmentNotFoundExceptionThrift();
    } else if (segIndex2SimulateSegmentMap.size() != simulateVolume.getSegmentCount()) {
      logger.error("segmentTable size:{} not equal with volume segment count:{}",
          segIndex2SimulateSegmentMap.size(), simulateVolume.getSegmentCount());
      throw new SegmentNotFoundExceptionThrift();
    }

    primaryCounter = simulateVolume.getPrimaryCounter();
    secondaryCounter = simulateVolume.getSecondaryCounter();
    arbiterCounter = simulateVolume.getArbiterCounter();
    primary2SecondaryCounterMap = simulateVolume.getPrimary2SecondaryCounterMap();
    wrapperIndex2NormalCounterMap = simulateVolume.getWrapperIndex2NormalCounterMap();

    for (SimulateInstanceInfo instanceInfo : canUsedInstanceId2SimulateInstanceMap.values()) {
      long instanceId = instanceInfo.getInstanceId().getId();
      if (primaryCounter.get(instanceId) == 0) {
        primaryCounter.set(instanceId, 0);
      }

      if (secondaryCounter.get(instanceId) == 0) {
        secondaryCounter.set(instanceId, 0);
      }

      if (arbiterCounter.get(instanceId) == 0) {
        arbiterCounter.set(instanceId, 0);
      }

      int groupId = instanceInfo.getGroupId();
      ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryCounterMap
          .computeIfAbsent(instanceId, value -> new TreeSetObjectCounter<>());
      for (SimulateInstanceInfo instanceInfoTemp : canUsedInstanceId2SimulateInstanceMap.values()) {
        //same group with primary, cannot be secondary
        if (groupId == instanceInfoTemp.getGroupId()) {
          continue;
        }

        long instanceIdTemp = instanceInfoTemp.getInstanceId().getId();
        if (secondaryOfPrimaryCounter.get(instanceIdTemp) == 0) {
          secondaryOfPrimaryCounter.set(instanceIdTemp, 0);
        }
      }
    }

    logger.info(
        "primaryCounter:{}; secondaryCounter:{}; arbiterCounter:{}; primary2SecondaryCounterMap:{}",
        primaryCounter, secondaryCounter, arbiterCounter, primary2SecondaryCounterMap);

    //calc expired counter
    primaryExpectCounter = simulateInstanceBuilder
        .getExpectReserveCount(canUsedInstanceId2SimulateInstanceMap.keySet(), null,
            realVolume.getSegmentCount(), realVolume.getSegmentCount(), false, true);

    //Arbiter desired count is calculate on datanode count, not weight
    arbiterExpiredCounter = simulateInstanceBuilder
        .getExpectReserveCount(arbiterCounter.getAll(), null, realVolume.getSegmentCount(),
            realVolume.getSegmentCount() * simulateVolume.getVolumeType().getNumArbiters(),
            true, false);

    for (long primaryId : primary2SecondaryCounterMap.keySet()) {
      InstanceMetadata primaryInfo = instanceId2InstanceMetadataMap.get(primaryId);
      HashSet<Long> excludeInstanceIdSet = new HashSet<>(
          groupId2InstanceIdMap.get(primaryInfo.getGroup().getGroupId()));
      long primaryFactCount = primaryExpectCounter.get(primaryId);
      //get expired count
      ObjectCounter<Long> secondaryIdOfPrimary2DesiredCountMap;
      if (simulateVolume.getVolumeType().getNumSecondaries() > 1) {
        secondaryIdOfPrimary2DesiredCountMap = simulateInstanceBuilder.getExpectReserveCountByGroup(
            canUsedInstanceId2SimulateInstanceMap.keySet(), excludeInstanceIdSet,
            realVolume.getSegmentCount(),
            primaryFactCount * realVolume.getVolumeType().getNumSecondaries(),
            true, true);
      } else {
        secondaryIdOfPrimary2DesiredCountMap = simulateInstanceBuilder.getExpectReserveCount(
            canUsedInstanceId2SimulateInstanceMap.keySet(), excludeInstanceIdSet,
            realVolume.getSegmentCount(),
            primaryFactCount * realVolume.getVolumeType().getNumSecondaries(),
            true, true);
      }

      secondaryOfPrimaryExpectCounterMap.put(primaryId, secondaryIdOfPrimary2DesiredCountMap);
    }

    isVolumeParsed = true;
    return this;
  }

}
