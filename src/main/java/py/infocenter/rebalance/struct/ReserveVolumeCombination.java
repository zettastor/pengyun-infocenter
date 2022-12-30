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


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.selector.SegmentUnitReserver;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.volume.VolumeType;

/**
 * combine  P、S by normal instance to generate a P/S combinationList.
 */
public class ReserveVolumeCombination {

  private static final Logger logger = LoggerFactory.getLogger(ReserveVolumeCombination.class);
  private final LinkedList<Long> normalInstanceIdList;    //all normal data node
  private final int reserveSegmentCount;                  //segment count that will be reserved
  private final long segmentSize;                         //segment size
  private final boolean isSimpleConfiguration;            //is simple volume
  private final VolumeType volumeType;                    //volume type
  private final SimulateInstanceBuilder simulateInstanceBuilder;  //all instance info manager in 
  // current pool
  private LinkedList<Deque<Long>> combinationList;        //result combination of P and S
  private Map<Long, SimulateInstanceInfo> canUsedInstanceId2SimulateInstanceMap;
  private Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap; //all instance info 
  // (include simple and normal)

  public ReserveVolumeCombination(SimulateInstanceBuilder simulateInstanceBuilder,
      boolean isSimpleConfiguration,
      int reserveSegmentCount, long segmentSize, VolumeType volumeType,
      LinkedList<Long> normalInstanceIdList) {
    this.simulateInstanceBuilder = simulateInstanceBuilder;
    this.reserveSegmentCount = reserveSegmentCount;
    this.normalInstanceIdList = normalInstanceIdList;
    this.instanceId2SimulateInstanceMap = simulateInstanceBuilder
        .getInstanceId2SimulateInstanceMap();
    this.segmentSize = segmentSize;
    this.isSimpleConfiguration = isSimpleConfiguration;
    this.volumeType = volumeType;

    this.canUsedInstanceId2SimulateInstanceMap = simulateInstanceBuilder
        .removeCanNotUsedInstance(volumeType);

    combinationList = new LinkedList<>();
  }


  @Deprecated
  public static LinkedList<Long> getSecondaryPriorityList(
      ObjectCounter<Long> secondaryOfPrimaryCounter,
      ObjectCounter<Long> secondaryDistributeCounter) {
    LinkedList<Long> selSecondaryList = new LinkedList<>();

    /*
     * The datanode is sorted according to the PS composite distribution, with the lowest
     * distribution in the front
     */
    Multimap<Long, Long> secondaryCount2InstanceIdOfPrimaryMap = HashMultimap.create();
    Iterator<Long> secondaryOfPrimaryCounterItor = secondaryOfPrimaryCounter.iterator();
    while (secondaryOfPrimaryCounterItor.hasNext()) {
      long nodeId = secondaryOfPrimaryCounterItor.next();
      long nodeCount = secondaryOfPrimaryCounter.get(nodeId);
      secondaryCount2InstanceIdOfPrimaryMap.put(nodeCount, nodeId);
    }
    LinkedList<Long> countList = new LinkedList<>(secondaryCount2InstanceIdOfPrimaryMap.keySet());
    Collections.sort(countList);

    for (long count : countList) {
      /*
       * In order of S distribution, the least comes first
       */
      Multimap<Long, Long> sortMap = HashMultimap.create();
      for (long nodeId : secondaryCount2InstanceIdOfPrimaryMap.get(count)) {
        sortMap.put(secondaryDistributeCounter.get(nodeId), nodeId);
      }
      LinkedList<Long> sortList = new LinkedList<>(sortMap.keySet());
      Collections.sort(sortList);

      /*
       * Random ordering of the same conditions
       */
      for (long value : sortList) {
        LinkedList shuffledList = new LinkedList<>(sortMap.get(value));
        Collections.shuffle(shuffledList);
        selSecondaryList.addAll(shuffledList);
      }
    }

    return selSecondaryList;
  }


  public void combination() throws TException {
    LinkedList<Long> primaryList = new LinkedList<>(
        normalInstanceIdList);      //can be primary list
    LinkedList<Long> secondaryList = new LinkedList<>(
        normalInstanceIdList);    //can be secondary list
    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();   //primary
    // V.S. secondaryList(sort by used time)
    //Map<Long, LinkedList<Long>> primaryId2SecondaryIdListMap = new HashMap<>();   //primary V.S
    // . secondaryList(sort by used time)
    ObjectCounter<Long> instanceIdOfReservedSegmentUnitCounter = new TreeSetObjectCounter<>();
    //already reserved instance counter
    ObjectCounter<Long> secondaryDistributeCounter = new TreeSetObjectCounter<>();  //The 
    // distribution of secondary on all nodes
    ObjectCounter<Long> primaryDistributeCounter = new TreeSetObjectCounter<>();
    while (combinationList.size() < reserveSegmentCount) {
      if (primaryList.size() == 0) {
        logger.error("No enough space to create P/S models");
        throw new NotEnoughSpaceExceptionThrift()
            .setDetail("No enough space to create P/S models");
      }

      primaryList = simulateInstanceBuilder
          .getPrimaryPriorityList(primaryList, primaryDistributeCounter, null, reserveSegmentCount);

      Set<Integer> usedGroupIdSet = new HashSet<>();
      int currentMixGroupUsedCount = 0;

      //select primary
      long primaryId = primaryList.removeFirst();
      SimulateInstanceInfo primaryInfo = instanceId2SimulateInstanceMap.get(primaryId);

      //verify mixed group
      if (simulateInstanceBuilder.getMixGroupIdSet().contains(primaryInfo.getGroupId())) {


        if (currentMixGroupUsedCount >= simulateInstanceBuilder
            .getUsableMixGroupCountPerOneSegment()) {
          continue;
        }
        currentMixGroupUsedCount++;
      }

      // instance has enough space to create one more segment units
      if (!canCreateOneMoreSegmentUnits(primaryInfo, instanceIdOfReservedSegmentUnitCounter)) {


        continue;
      }

      usedGroupIdSet.add(primaryInfo.getGroupId());

      //get secondary select list
      ObjectCounter<Long> secondaryOfPrimaryCounter = primaryId2SecondaryIdCounterMap
          .get(primaryId);
      if (secondaryOfPrimaryCounter == null) {
        secondaryOfPrimaryCounter = new TreeSetObjectCounter<>();
        primaryId2SecondaryIdCounterMap.put(primaryId, secondaryOfPrimaryCounter);

        for (long secondaryId : secondaryList) {
          secondaryOfPrimaryCounter.set(secondaryId, 0);
        }
      }

      //get secondary datanode priority list
      LinkedList<Long> secondaryListOfPrimary = simulateInstanceBuilder
          .getSecondaryPriorityList(secondaryList, primaryId,
              primaryDistributeCounter.get(primaryId) + 1,
              secondaryOfPrimaryCounter, secondaryDistributeCounter, null,
              reserveSegmentCount, volumeType.getNumSecondaries());

      Iterator<Long> secondaryIdIt = secondaryListOfPrimary.iterator();
      ArrayList<Long> selectSecondaryList = new ArrayList<>();
      //select secondary
      for (int j = 0; j < volumeType.getNumSecondaries(); j++) {
        while (secondaryIdIt.hasNext()) {
          long secondaryIdTemp = secondaryIdIt.next();
          SimulateInstanceInfo secondaryTempInfo = instanceId2SimulateInstanceMap
              .get(secondaryIdTemp);


          if (usedGroupIdSet.contains(secondaryTempInfo.getGroupId())) {
            continue;
          }

          //verify mixed group
          if (simulateInstanceBuilder.getMixGroupIdSet().contains(secondaryTempInfo.getGroupId())) {


            if (currentMixGroupUsedCount >= simulateInstanceBuilder
                .getUsableMixGroupCountPerOneSegment()) {
              continue;
            }
            currentMixGroupUsedCount++;
          }

          // instance has enough space to create one more segment units
          if (!canCreateOneMoreSegmentUnits(secondaryTempInfo,
              instanceIdOfReservedSegmentUnitCounter)) {


            continue;
          }

          //save secondary
          selectSecondaryList.add(secondaryIdTemp);
          usedGroupIdSet.add(secondaryTempInfo.getGroupId());
          break;
        }
      }

      if (selectSecondaryList.size() < volumeType.getNumSecondaries()) {
        logger.error("No enough space to create P/S models");
        throw new NotEnoughSpaceExceptionThrift()
            .setDetail("No enough space to create P/S models");
      }

      Deque<Long> segmentCombination = new ArrayDeque<>();
      segmentCombination.add(primaryId);
      segmentCombination.addAll(selectSecondaryList);

      primaryDistributeCounter.increment(primaryId);
      //reserve segment space
      instanceIdOfReservedSegmentUnitCounter.increment(primaryId);
      for (long secondaryIdTemp : selectSecondaryList) {
        secondaryOfPrimaryCounter.increment(secondaryIdTemp);
        instanceIdOfReservedSegmentUnitCounter.increment(secondaryIdTemp);
        secondaryDistributeCounter.increment(secondaryIdTemp);
      }

      //save combination
      combinationList.add(segmentCombination);

      primaryList.offerLast(primaryId);
    }

    if (logger.isDebugEnabled()) {
      verifyBalance();
    }
  }

  /**
   * is instance can create one or more segment unit.
   *
   * @param instanceInfo                           instance information
   * @param instanceIdOfReservedSegmentUnitCounter already reserved segment unit counter of
   *                                               instance
   * @return true:if can
   */
  public boolean canCreateOneMoreSegmentUnits(SimulateInstanceInfo instanceInfo,
      ObjectCounter<Long> instanceIdOfReservedSegmentUnitCounter) {
    boolean canCreate = false;
    long reservedSegmentUnit = instanceIdOfReservedSegmentUnitCounter
        .get(instanceInfo.getInstanceId().getId());
    if (isSimpleConfiguration) {
      long canCreateSegmentCount =
          instanceInfo.getFreeFlexibleSegmentUnitCount() - reservedSegmentUnit;
      if (canCreateSegmentCount >= 1) {
        canCreate = true;
      }
    } else {
      long remainSize = instanceInfo.getFreeSpace() - reservedSegmentUnit * segmentSize;
      if (remainSize >= segmentSize) {
        canCreate = true;
      }
    }

    if (!canCreate) {
      logger.warn("cannot create more {} segment on instance:{}. segment size:{}",
          reservedSegmentUnit, instanceInfo, segmentSize);
    }

    return canCreate;
  }

  /**
   * has enough space to create this combination.
   *
   * @param combination                combination of PS
   * @param reservedSegmentUnitCounter instance that already reserved Segment unit counter
   * @return true:if has enough space
   */
  public boolean canCombinationCreate(Deque<Long> combination,
      ObjectCounter<Long> reservedSegmentUnitCounter) {
    if (combination.size() == 0) {
      return false;
    }
    ObjectCounter<Long> reservedSegmentUnitCounterTemp = reservedSegmentUnitCounter.deepCopy();

    for (long instanceId : combination) {
      SimulateInstanceInfo instanceInfo = instanceId2SimulateInstanceMap.get(instanceId);

      //can no enough space to create segment unit
      if (!canCreateOneMoreSegmentUnits(instanceInfo, reservedSegmentUnitCounterTemp)) {
        return false;
      }

      reservedSegmentUnitCounterTemp.increment(instanceId);
    }

    return true;
  }

  /**
   * poll a combination, which cannot contain reserved arbiter and has no overload instance
   * combinations is a priority.
   *
   * @param overloadInstanceIdSet overload instance
   * @return P, S combination, it may be empty when combinationList is empty
   */
  public Deque<Long> pollSegment(Set<Long> overloadInstanceIdSet,
      ObjectCounter<Long> instanceIdCounterOfWrapper) throws NotEnoughSpaceExceptionThrift {
    Deque<Long> bestSegment = new ArrayDeque<>();

    if (overloadInstanceIdSet.size() == 0) {
      if (!combinationList.isEmpty()) {
        bestSegment = combinationList.removeFirst();
      } else {
        bestSegment = new ArrayDeque<>();
      }
      return bestSegment;
    }

    if (instanceIdCounterOfWrapper.size() < canUsedInstanceId2SimulateInstanceMap.size()) {
      for (long insId : canUsedInstanceId2SimulateInstanceMap.keySet()) {
        if (instanceIdCounterOfWrapper.get(insId) == 0) {
          instanceIdCounterOfWrapper.set(insId, 0);
        }
      }
    }

    int volumeNormalSegCount = volumeType.getNumSecondaries() + SegmentUnitReserver.PRIMARYCOUNT;
    if (instanceIdCounterOfWrapper.size() < volumeNormalSegCount) {
      logger.error("No enough data node to get P/S");
      throw new NotEnoughSpaceExceptionThrift().setDetail("No enough data node to get P/S");
    }

    Iterator<Long> instanceIdCounterOfWrapperIt = instanceIdCounterOfWrapper.iterator();
    long desiredReserveCount = 0;
    int getNum = 0;
    while (instanceIdCounterOfWrapperIt.hasNext()) {
      getNum++;
      desiredReserveCount += instanceIdCounterOfWrapper.get(instanceIdCounterOfWrapperIt.next());
      if (getNum >= volumeNormalSegCount) {
        break;
      }
    }

    try {
      //int maxNoOverloadInstanceNum = 0;
      long bestSegmentReservedCounter = 0xFFFF;
      for (Deque<Long> segmentUnitDeque : combinationList) {

        long curCombinationReserveCount = 0;
        int notOverloadCount = 0;
        for (long instanceId : segmentUnitDeque) {
          //record not overload count
          if (!overloadInstanceIdSet.contains(instanceId)) {
            notOverloadCount++;
          }


          if (normalInstanceIdList.size() - overloadInstanceIdSet.size() != 0
              && notOverloadCount >= normalInstanceIdList.size() - overloadInstanceIdSet.size()) {
            bestSegment = segmentUnitDeque;
            return bestSegment;
          }

          curCombinationReserveCount += instanceIdCounterOfWrapper.get(instanceId);
        }


        if (curCombinationReserveCount <= desiredReserveCount) {
          bestSegment = segmentUnitDeque;
          return bestSegment;
        } else if (curCombinationReserveCount < bestSegmentReservedCounter) {
          //reserved segment the little the best
          bestSegmentReservedCounter = curCombinationReserveCount;
          bestSegment = segmentUnitDeque;
        }
      }







      if (bestSegment.size() == 0 && combinationList.size() > 0) {
        bestSegment = combinationList.getFirst();
      }

      // when combinationsList's size is 0, empty combination will be return
      return bestSegment;
    } finally {
      if (bestSegment != null) {
        combinationList.remove(bestSegment);
      }
    }
  }

  /**
   * reserve a P/S combination of segment at random.
   *
   * @param overloadInstanceIdSet                  instance which already overload
   * @param instanceIdOfReservedSegmentUnitCounter already reserved segment unit counter of
   *                                               instance
   * @return a P/S combination that is not null or empty
   * @throws NotEnoughSpaceExceptionThrift if have no enough space
   */
  public Deque<Long> randomSegment(Set<Long> overloadInstanceIdSet,
      ObjectCounter<Long> instanceIdOfReservedSegmentUnitCounter)
      throws NotEnoughSpaceExceptionThrift {
    Deque<Long> segmentUnitDeque = new ArrayDeque<>();
    Set<Integer> usedGroupIdSet = new HashSet<>();
    int currentMixGroupUsedCount = 0;

    //shuffle normal list
    LinkedList<Long> normalInstanceIdListBac = new LinkedList<>(normalInstanceIdList);
    Collections.shuffle(normalInstanceIdListBac);

    int normalSegmentUnitCountPerSegment =
        volumeType.getNumSecondaries() + SegmentUnitReserver.PRIMARYCOUNT;


    boolean isOverloadList = false;
    Iterator<Long> normalIdIt = normalInstanceIdListBac.iterator();
    Set<Long> overloadNotUsedSet = new HashSet<>(overloadInstanceIdSet);
    for (int i = 0; i < normalSegmentUnitCountPerSegment; i++) {
      boolean canBeSegmentUnit = false;
      while (normalIdIt.hasNext()) {
        long normalIdTemp = normalIdIt.next();
        SimulateInstanceInfo instanceInfo = instanceId2SimulateInstanceMap.get(normalIdTemp);

        //group is already used
        if (usedGroupIdSet.contains(instanceInfo.getGroupId())) {
          continue;
        }

        //verify mixed group
        if (simulateInstanceBuilder.getMixGroupIdSet().contains(instanceInfo.getGroupId())) {


          if (currentMixGroupUsedCount >= simulateInstanceBuilder
              .getUsableMixGroupCountPerOneSegment()) {
            continue;
          }
          currentMixGroupUsedCount++;
        }

        // instance has enough space to create one more segment units
        if (!canCreateOneMoreSegmentUnits(instanceInfo, instanceIdOfReservedSegmentUnitCounter)) {


          continue;
        }

        if (!isOverloadList && overloadNotUsedSet.contains(normalIdTemp)) {
          continue;
        }

        //save secondary
        segmentUnitDeque.add(normalIdTemp);
        usedGroupIdSet.add(instanceInfo.getGroupId());
        canBeSegmentUnit = true;
        break;
      } //while(normalIdIt.hasNext()){


      if (!canBeSegmentUnit && !isOverloadList) {
        normalIdIt = overloadNotUsedSet.iterator();
        isOverloadList = true;
        i--;
      }
    } //for

    if (segmentUnitDeque.size()
        < volumeType.getNumSecondaries() + SegmentUnitReserver.PRIMARYCOUNT) {
      logger.error("No enough data node to create P/S models");
      throw new NotEnoughSpaceExceptionThrift()
          .setDetail("No enough data node to create P/S models");
    }

    return segmentUnitDeque;
  }

  /**
   * just for unit test.
   */
  private void verifyBalance() {
    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

    for (Deque<Long> combination : combinationList) {
      //count necessary primary, secondary
      ObjectCounter<Long> primarySecondaryCounter = new TreeSetObjectCounter<>();
      int first = 0;
      for (long instanceId : combination) {
        if (first == 0) {
          primaryIdCounter.increment(instanceId);

          primarySecondaryCounter = primaryId2SecondaryIdCounterMap
              .computeIfAbsent(instanceId, value -> new TreeSetObjectCounter<>());
          first++;
          continue;
        }

        secondaryIdCounter.increment(instanceId);
        primarySecondaryCounter.increment(instanceId);
      }
    }

    long maxCount;
    long minCount;
    try {


      maxCount = primaryIdCounter.maxValue();
      minCount = primaryIdCounter.minValue();
      if (maxCount - minCount > 10) {
        logger.error("primary average distribute failed ! maxCount:{}, minCount:{}", maxCount,
            minCount);
      }

      //verify necessary secondary max and min count
      maxCount = secondaryIdCounter.maxValue();
      minCount = secondaryIdCounter.minValue();
      if (maxCount - minCount > 10) {
        logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}", maxCount,
            minCount);
      }


      for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap
          .entrySet()) {
        ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
        maxCount = secondaryCounterTemp.maxValue();
        minCount = secondaryCounterTemp.minValue();

        if (maxCount - minCount > 10) {
          logger.error("PS distribution not balance! maxCount:{}, minCount:{}", maxCount, minCount);
        }
      }
    } catch (Exception e) {
      logger.error("catch a exception(may be 0 segment reserved)", e);
    }
  }

}
