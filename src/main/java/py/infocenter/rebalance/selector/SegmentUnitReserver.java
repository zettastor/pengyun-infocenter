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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.struct.InstanceInfoImpl;
import py.infocenter.rebalance.struct.ReserveVolumeCombination;
import py.infocenter.rebalance.struct.SimpleDatanodeManager;
import py.infocenter.rebalance.struct.SimulateInstanceInfo;
import py.infocenter.store.StorageStore;
import py.instance.InstanceId;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.NotEnoughGroupExceptionThrift;
import py.thrift.share.NotEnoughNormalGroupExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeType;


public class SegmentUnitReserver {

  public static final int PRIMARYCOUNT = 1;
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitReserver.class);
  private final long segmentSize;
  private final StorageStore storageStore;
  private final TreeSet<InstanceInfoImpl> instanceInfoSet = new TreeSet<>();
  private final HashMap<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap =
      new HashMap<>();
  //fault_tolerant instance max count
  private final int faultTolerantNormalInstanceCountMax = 2;
  private final int faultTolerantSimpleInstanceCountMax = 1;
  private SimulateInstanceBuilder simulateInstanceBuilder;

  public SegmentUnitReserver(long segmentSize, StorageStore storageStore) {
    this.segmentSize = segmentSize;
    this.storageStore = storageStore;
  }

  public void updateInstanceInfo(SimulateInstanceBuilder simulateInstanceBuilder) {
    this.simulateInstanceBuilder = simulateInstanceBuilder;
    instanceId2SimulateInstanceMap.clear();
    instanceId2SimulateInstanceMap
        .putAll(simulateInstanceBuilder.getInstanceId2SimulateInstanceMap());
  }


  public Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> reserveVolume(
      long expectedSize, VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize,
      SimpleDatanodeManager simpleDatanodeManager, boolean faultTolerant)
      throws TException {
    logger.warn(
        "reserveVolume: expectedSize:{}, volumeType:{}, isSimpleConfiguration:{}, "
            + "segmentWrapSize:{}, simpleDatanodeManager:{}, faultTolerant:{}",
        expectedSize, volumeType, isSimpleConfiguration, segmentWrapSize, simpleDatanodeManager,
        faultTolerant);
    if (expectedSize == 0) {
      logger.error("0 size volume is not allowed to create");
      return new HashMap<>();
    }
    //save all InstanceMetadata, <instanceId, InstanceMetadata>
    HashMap<Long, InstanceMetadata> instanceId2instanceMetadataMap = new HashMap<>();
    // these datanode will be only used to create arbiter segment units
    ObjectCounter<Long> simpleDatanodeIdCounter = new TreeSetObjectCounter<>();


    ObjectCounter<Long> normalDatanodeIdForArbiterCounter = new TreeSetObjectCounter<>();
    // these datanode will be used to create normal segment units
    ObjectCounter<Long> normalDatanodeIdCounter = new TreeSetObjectCounter<>();

    Set<Integer> simpleDatanodeGroupIdSet = simulateInstanceBuilder
        .getSimpleGroupIdSet();    //simple datanode Group Id
    Set<Integer> normalDatanodeGroupIdSet = simulateInstanceBuilder
        .getNormalGroupIdSet();    //normal datanode Group Id
    Set<Integer> mixGroupIdSet = simulateInstanceBuilder
        .getMixGroupIdSet();       //group which has simple datanode and has normal datanode

    for (SimulateInstanceInfo instanceInfo : instanceId2SimulateInstanceMap.values()) {
      InstanceMetadata instanceMetadata = storageStore.get(instanceInfo.getInstanceId().getId());
      instanceId2instanceMetadataMap
          .put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

      if (instanceInfo.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL) {
        //simple segment unit cannot be create at normal datanode
        normalDatanodeIdForArbiterCounter.set(instanceInfo.getInstanceId().getId(), 0);
        normalDatanodeIdCounter.set(instanceInfo.getInstanceId().getId(), 0);
      }
    }

    for (SimulateInstanceInfo instanceInfo : simulateInstanceBuilder.getSimpleDatanodeMap()
        .values()) {
      InstanceMetadata instanceMetadata = storageStore.get(instanceInfo.getInstanceId().getId());
      instanceId2instanceMetadataMap
          .put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

      simpleDatanodeIdCounter.set(instanceInfo.getInstanceId().getId(), 0);
    }

    logger.info(
        "reserveVolume: simpleDatanodeGroupIdSet:{}; simpleDatanodeIdList:{}; "
            + "normalDatanodeGroupIdSet:{}; normalDatanodeIdList:{}; mixedGroupIdSet:{}",
        simpleDatanodeGroupIdSet, simpleDatanodeIdCounter, normalDatanodeGroupIdSet,
        normalDatanodeIdCounter, mixGroupIdSet);


    Map<SegmentUnitTypeThrift, Integer> segmentUnitCountMap = calculateSegmentUnitWillBeCreateCount(
        simpleDatanodeGroupIdSet, normalDatanodeGroupIdSet, mixGroupIdSet, volumeType,
        faultTolerant);
    int numberOfNormalWillReservePerSegment = segmentUnitCountMap
        .get(SegmentUnitTypeThrift.Normal);
    int numberOfArbiterWillReservePerSegment = segmentUnitCountMap
        .get(SegmentUnitTypeThrift.Arbiter);

    int numOfSegments = (int) (expectedSize / segmentSize); // segment count
    int segmentWrapperCount = numOfSegments / segmentWrapSize; // how many wrapper we have
    // maybe segment count is not a integer multiple of wrap size and we will have a remainder.
    int remainder = numOfSegments % segmentWrapSize;

    /*
     * get segment unit P/S combinations list
     */
    ReserveVolumeCombination reserveVolumeCombination = new ReserveVolumeCombination(
        simulateInstanceBuilder,
        isSimpleConfiguration, numOfSegments, segmentSize, volumeType,
        new LinkedList<>(normalDatanodeIdCounter.getAll()));
    //get segment unit P/S combinations list
    reserveVolumeCombination.combination();

    //for distribute arbiter
    ObjectCounter<Long> simpleGroupSimpleDatanodeCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> mixGroupSimpleDatanodeCounter = new TreeSetObjectCounter<>();
    // get simple datanode in simple group and simple datanode in mix group
    Iterator<Long> simpleDatanodeIdListItor = simpleDatanodeIdCounter.iterator();
    while (simpleDatanodeIdListItor.hasNext()) {
      long instanceId = simpleDatanodeIdListItor.next();
      InstanceMetadata instanceMetadata = instanceId2instanceMetadataMap.get(instanceId);
      if (mixGroupIdSet.contains(instanceMetadata.getGroup().getGroupId())) {
        mixGroupSimpleDatanodeCounter.set(instanceId, 0);
      } else {
        simpleGroupSimpleDatanodeCounter.set(instanceId, 0);
      }
    }

    // loop time depends on whether we have a remainder
    int segmentCreateTimesPerSegmentWrapper = segmentWrapperCount;
    if (remainder != 0) {
      segmentCreateTimesPerSegmentWrapper += 1;
    }

    ObjectCounter<Long> reservedSegmentUnitCounter = new TreeSetObjectCounter<>();

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> resultToReturn =
        new HashMap<>();
    for (int wrapperIndex = 0; wrapperIndex < segmentCreateTimesPerSegmentWrapper; wrapperIndex++) {
      int segmentCountThisTime = segmentWrapSize;
      if (wrapperIndex >= segmentWrapperCount) {
        segmentCountThisTime = remainder;
      }

      ObjectCounter<Long> instanceIdCounterForOverload = new TreeSetObjectCounter<>();
      Set<Long> overloadInstanceIdSet = new HashSet<>();

      for (int segmentIndex = 0; segmentIndex < segmentCountThisTime; segmentIndex++) {
        //group which is used to reserve segment unit, whether arbiter or normal
        HashSet<Integer> usedGroupSet = new HashSet<>();

        /*
         * reserve normal
         */
        LinkedList<InstanceIdAndEndPointThrift> selectedNormalList = new LinkedList<>();

        Deque<Long> combinationofps = reserveVolumeCombination
            .pollSegment(overloadInstanceIdSet, instanceIdCounterForOverload);
        if (combinationofps.size() == 0) {
          //Choose normal segment arbitrarily
          combinationofps = reserveVolumeCombination
              .randomSegment(overloadInstanceIdSet, reservedSegmentUnitCounter);
        }

        //is combination can be created
        boolean isThisCombinationOk = reserveVolumeCombination.canCombinationCreate(combinationofps,
            reservedSegmentUnitCounter);
        if (!isThisCombinationOk) {
          segmentIndex--;
          continue;
        }

        //update data
        for (long instanceId : combinationofps) {
          SimulateInstanceInfo instanceInfo = instanceId2SimulateInstanceMap.get(instanceId);

          reservedSegmentUnitCounter.increment(instanceId);
          instanceIdCounterForOverload.increment(instanceId);
          usedGroupSet.add(instanceInfo.getGroupId());

          int instanceReservedCount = (int) instanceIdCounterForOverload.get(instanceId);
          if (instanceReservedCount >= instanceInfo.getDiskCount()) {
            overloadInstanceIdSet.add(instanceId);
          }

          //save select normal segment unit
          InstanceMetadata instanceMetadata = instanceId2instanceMetadataMap.get(instanceId);
          selectedNormalList
              .addLast(RequestResponseHelper.buildInstanceIdAndEndPointFrom(instanceMetadata));
        }

        LinkedList<InstanceIdAndEndPointThrift> selectedArbitersList = new LinkedList<>();

        //remove data node which own group is already used, to reduce the number of cycles
        LinkedList<Long> normalIdForArbiterList = new LinkedList<>(
            normalDatanodeIdForArbiterCounter.getAll());
        for (int usedGroupId : usedGroupSet) {
          normalIdForArbiterList.removeIf(
              id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
        }

        Iterator<Long> simpleGroupSimpleDatanodeCounterItor = simpleGroupSimpleDatanodeCounter
            .iterator();
        Iterator<Long> mixGroupSimpleDatanodeCounterItor = mixGroupSimpleDatanodeCounter.iterator();
        Iterator<Long> normalDatanodeIdListForArbiterItor = normalIdForArbiterList.iterator();

        List<Long> simpleGroupSimpleDatanodeToBeArbiterList = new LinkedList<>();
        List<Long> mixGroupSimpleDatanodeToBeArbiterList = new LinkedList<>();
        List<Long> normalDatanodeToBeArbiterList = new LinkedList<>();

        while (selectedArbitersList.size() < volumeType.getNumArbiters()) {
          if (simpleGroupSimpleDatanodeCounterItor.hasNext()) {
            //arbiter priority selection simple datanode to be created
            InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap
                .get(simpleGroupSimpleDatanodeCounterItor.next());
            //logger.info("get simple datanode from storageStore: {}", simpleDatanodeTemp);
            if (usedGroupSet.stream()
                .noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {

              selectedArbitersList
                  .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
              simpleGroupSimpleDatanodeToBeArbiterList
                  .add(simpleDatanodeTemp.getInstanceId().getId());
              usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
            }
          } else if (mixGroupSimpleDatanodeCounterItor.hasNext()) {
            //arbiter priority selection simple datanode to be created
            InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap
                .get(mixGroupSimpleDatanodeCounterItor.next());
            //logger.info("get simple datanode from storageStore: {}", simpleDatanodeTemp);
            if (usedGroupSet.stream()
                .noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {

              selectedArbitersList
                  .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
              mixGroupSimpleDatanodeToBeArbiterList.add(simpleDatanodeTemp.getInstanceId().getId());
              usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
            }
          } else if (normalDatanodeIdListForArbiterItor.hasNext()) {
            //if simple datanode is not enough , arbiter can create at normal datanode
            InstanceMetadata normalDatanodeTemp = instanceId2instanceMetadataMap
                .get(normalDatanodeIdListForArbiterItor.next());

            if (usedGroupSet.stream()
                .noneMatch(id -> id == normalDatanodeTemp.getGroup().getGroupId())) {
              selectedArbitersList
                  .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(normalDatanodeTemp));
              normalDatanodeToBeArbiterList.add(normalDatanodeTemp.getInstanceId().getId());
              usedGroupSet.add(normalDatanodeTemp.getGroup().getGroupId());
            }
          }
        }

        //put necessary used arbiter to tail, ensure balance
        for (Long simpleDatanodeIdTemp : simpleGroupSimpleDatanodeToBeArbiterList) {
          simpleGroupSimpleDatanodeCounter.increment(simpleDatanodeIdTemp);
        }
        for (Long simpleDatanodeIdTemp : mixGroupSimpleDatanodeToBeArbiterList) {
          mixGroupSimpleDatanodeCounter.increment(simpleDatanodeIdTemp);
        }
        for (Long normalDatanodeIdTemp : normalDatanodeToBeArbiterList) {
          normalDatanodeIdForArbiterCounter.increment(normalDatanodeIdTemp);
        }

        /*
         * reserve fault secondary
         */
        int faultNormalCount =
            numberOfNormalWillReservePerSegment - volumeType.getNumSecondaries() - PRIMARYCOUNT;
        if (faultNormalCount > 0) {
          LinkedList<InstanceIdAndEndPointThrift> selectedFaultNormalList = reserveFaultSecondary(
              faultNormalCount, usedGroupSet, normalDatanodeIdCounter,
              instanceId2instanceMetadataMap, instanceId2SimulateInstanceMap,
              selectedNormalList, reserveVolumeCombination, reservedSegmentUnitCounter, volumeType);

          //put fault normal segment unit to normal segment unit list
          selectedNormalList.addAll(selectedFaultNormalList);
        }

        /*
         * reserve fault arbiter
         */
        int faultArbiterCount = numberOfArbiterWillReservePerSegment - volumeType.getNumArbiters();
        if (faultArbiterCount > 0) {
          LinkedList<InstanceIdAndEndPointThrift> selectedFaultArbitersList = reserveFaultArbiter(
              faultArbiterCount, usedGroupSet,
              simpleGroupSimpleDatanodeCounter, mixGroupSimpleDatanodeCounter,
              normalDatanodeIdForArbiterCounter, instanceId2instanceMetadataMap);

          //put fault segment unit to arbiter segment unit list
          selectedArbitersList.addAll(selectedFaultArbitersList);
        }

        /*
         * save result
         */
        int segmentNo = wrapperIndex * segmentWrapSize + segmentIndex;
        Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> mapTypeToList =
            new HashMap<>();
        mapTypeToList.put(
            isSimpleConfiguration ? SegmentUnitTypeThrift.Flexible : SegmentUnitTypeThrift.Normal,
            selectedNormalList);
        mapTypeToList.put(SegmentUnitTypeThrift.Arbiter, selectedArbitersList);
        resultToReturn.put(segmentNo, mapTypeToList);
      }
    }
    logger.debug("reserveVolume: resultToReturn:{}", resultToReturn);
    printResultLog(volumeType, segmentWrapSize, isSimpleConfiguration, resultToReturn);
    return resultToReturn;
  }

  /**
   * reserve fault secondary fault segment unit not care overload but care space.
   *
   * @param faultNormalCount               will reserve secondary count
   * @param usedGroupSet                   has already used group set
   * @param normalDatanodeIdList           normal data node list
   * @param instanceId2instanceMetadataMap all instance info
   * @return fault secondary list
   */
  private LinkedList<InstanceIdAndEndPointThrift> reserveFaultSecondary(
      int faultNormalCount, Set<Integer> usedGroupSet, ObjectCounter<Long> normalDatanodeIdList,
      HashMap<Long, InstanceMetadata> instanceId2instanceMetadataMap,
      HashMap<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap,
      LinkedList<InstanceIdAndEndPointThrift> selectedNormalList,
      ReserveVolumeCombination reserveVolumeCombination,
      ObjectCounter<Long> reservedSegmentUnitCounter, VolumeType volumeType)
      throws NotEnoughSpaceExceptionThrift {
    LinkedList<InstanceIdAndEndPointThrift> selectedFaultNormalList = new LinkedList<>();

    //remove data node which own group is already used, to reduce the number of cycles
    LinkedList<Long> faultNormalIdList = new LinkedList<>(normalDatanodeIdList.getAll());
    for (int usedGroupId : usedGroupSet) {
      faultNormalIdList.removeIf(
          id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
    }

    //shuffle normalIdList
    Collections.shuffle(faultNormalIdList);

    Set<Integer> mixGroupIdSet = simulateInstanceBuilder.getMixGroupIdSet();
    int usableMixGroupCountInSameSegment = simulateInstanceBuilder
        .getUsableMixGroupCountPerOneSegment();

    Set<Integer> usedMixGroupIdSet = new HashSet<>();
    for (InstanceIdAndEndPointThrift instanceImpl : selectedNormalList) {
      usedMixGroupIdSet.add(instanceImpl.getGroupId());
    }
    usedMixGroupIdSet.retainAll(mixGroupIdSet);
    int usableMixGroupCountThisTime = usableMixGroupCountInSameSegment - usedMixGroupIdSet.size();
    int currentMixGroupUsedCount = 0;

    for (Long faultNormalId : faultNormalIdList) {
      if (selectedFaultNormalList.size() >= faultNormalCount) {
        break;
      }
      //if simple datanode is not enough , arbiter can create at normal datanode
      InstanceMetadata normalDatanodeTemp = instanceId2instanceMetadataMap.get(faultNormalId);
      long instanceId = normalDatanodeTemp.getInstanceId().getId();

      //verify mixed group
      if (mixGroupIdSet.contains(normalDatanodeTemp.getGroup().getGroupId())) {
        if (currentMixGroupUsedCount >= usableMixGroupCountThisTime) {
          continue;
        }
        currentMixGroupUsedCount++;
      }

      SimulateInstanceInfo instanceInfo = instanceId2SimulateInstanceMap.get(instanceId);
      //has enough space to create segment unit
      if (!reserveVolumeCombination.canCreateOneMoreSegmentUnits(
          instanceInfo, reservedSegmentUnitCounter)) {
        continue;
      }

      if (usedGroupSet.stream().noneMatch(id -> id == normalDatanodeTemp.getGroup().getGroupId())) {
        selectedFaultNormalList
            .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(normalDatanodeTemp));

        usedGroupSet.add(normalDatanodeTemp.getGroup().getGroupId());
        reservedSegmentUnitCounter.increment(instanceId);
      }
    }

    if (selectedFaultNormalList.size() < faultNormalCount) {
      logger.error(
          "reserve fault normal segment unit failed! faultNormalCount:{}, "
              + "selectedFaultNormalList:{}",
          faultNormalCount, selectedFaultNormalList);
      throw new NotEnoughSpaceExceptionThrift()
          .setDetail("reserve fault normal segment unit failed!");
    }

    return selectedFaultNormalList;
  }

  /**
   * reserve fault arbiter.
   *
   * @param faultArbiterCount                will reserve arbiter count
   * @param usedGroupSet                     has already used group set
   * @param simpleGroupSimpleDatanodeCounter simple data node which in simple group
   * @param mixGroupSimpleDatanodeCounter    simple data node which in mix group
   * @param normalDatanodeIdForArbiterList   normal data node list
   * @param instanceId2instanceMetadataMap   all instance info
   * @return fault arbiter list
   */
  private LinkedList<InstanceIdAndEndPointThrift> reserveFaultArbiter(int faultArbiterCount,
      Set<Integer> usedGroupSet,
      ObjectCounter<Long> simpleGroupSimpleDatanodeCounter,
      ObjectCounter<Long> mixGroupSimpleDatanodeCounter,
      ObjectCounter<Long> normalDatanodeIdForArbiterList,
      HashMap<Long, InstanceMetadata> instanceId2instanceMetadataMap) {
    final LinkedList<InstanceIdAndEndPointThrift> selectedFaultArbitersList = new LinkedList<>();

    //remove data node which own group is already used, to reduce the number of cycles
    LinkedList<Long> simpleGroupSimpleIdForFaultArbiterList = new LinkedList<>(
        simpleGroupSimpleDatanodeCounter.getAll());
    for (int usedGroupId : usedGroupSet) {
      simpleGroupSimpleIdForFaultArbiterList.removeIf(
          id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
    }

    LinkedList<Long> mixGroupSimpleIdForFaultArbiterList = new LinkedList<>(
        mixGroupSimpleDatanodeCounter.getAll());
    for (int usedGroupId : usedGroupSet) {
      mixGroupSimpleIdForFaultArbiterList.removeIf(
          id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
    }

    LinkedList<Long> normalIdForFaultArbiterList = new LinkedList<>(
        normalDatanodeIdForArbiterList.getAll());
    for (int usedGroupId : usedGroupSet) {
      normalIdForFaultArbiterList.removeIf(
          id -> instanceId2instanceMetadataMap.get(id).getGroup().getGroupId() == usedGroupId);
    }

    //shuffle simpleIdList
    Collections.shuffle(simpleGroupSimpleIdForFaultArbiterList);
    //shuffle simpleIdList
    Collections.shuffle(mixGroupSimpleIdForFaultArbiterList);
    //shuffle normalIdList
    Collections.shuffle(normalIdForFaultArbiterList);

    Iterator<Long> simpleGroupSimpleIdListForFaultItor = simpleGroupSimpleIdForFaultArbiterList
        .iterator();
    Iterator<Long> mixGroupSimpleIdListForFaultItor = mixGroupSimpleIdForFaultArbiterList
        .iterator();
    Iterator<Long> normalIdListForFaultArbiterItor = normalIdForFaultArbiterList.iterator();

    while (selectedFaultArbitersList.size() < faultArbiterCount) {
      if (simpleGroupSimpleIdListForFaultItor.hasNext()) {
        InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap
            .get(simpleGroupSimpleIdListForFaultItor.next());
        if (usedGroupSet.stream()
            .noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {
          logger.info("reserveVolume from simple datanode: {}", simpleDatanodeTemp);
          selectedFaultArbitersList
              .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
          usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
        }
      } else if (mixGroupSimpleIdListForFaultItor.hasNext()) {
        InstanceMetadata simpleDatanodeTemp = instanceId2instanceMetadataMap
            .get(mixGroupSimpleIdListForFaultItor.next());
        if (usedGroupSet.stream()
            .noneMatch(id -> id == simpleDatanodeTemp.getGroup().getGroupId())) {
          logger.info("reserveVolume from simple datanode: {}", simpleDatanodeTemp);
          selectedFaultArbitersList
              .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(simpleDatanodeTemp));
          usedGroupSet.add(simpleDatanodeTemp.getGroup().getGroupId());
        }
      } else if (normalIdListForFaultArbiterItor.hasNext()) {

        InstanceMetadata normalDatanodeTemp = instanceId2instanceMetadataMap
            .get(normalIdListForFaultArbiterItor.next());
        if (usedGroupSet.stream()
            .noneMatch(id -> id == normalDatanodeTemp.getGroup().getGroupId())) {
          selectedFaultArbitersList
              .add(RequestResponseHelper.buildInstanceIdAndEndPointFrom(normalDatanodeTemp));
          usedGroupSet.add(normalDatanodeTemp.getGroup().getGroupId());
        }
      } else {
        break;
      }
    }
    return selectedFaultArbitersList;
  }


  private Map<SegmentUnitTypeThrift, Integer> calculateSegmentUnitWillBeCreateCount(
      Set<Integer> simpleDatanodeGroupIdSet, Set<Integer> normalDatanodeGroupIdSet,
      Set<Integer> mixedGroupIdSet,
      VolumeType volumeType, boolean faultTolerant)
      throws NotEnoughGroupExceptionThrift, NotEnoughNormalGroupExceptionThrift {
    int numberOfMembersPerSegment = volumeType
        .getNumMembers(); // including normal segment units and arbiters
    int numberOfArbitersPerSegment = volumeType.getNumArbiters(); // arbiters
    int numberOfNormalPerSegment = numberOfMembersPerSegment - numberOfArbitersPerSegment;

    int allGroupSize =
        simpleDatanodeGroupIdSet.size() + normalDatanodeGroupIdSet.size() + mixedGroupIdSet.size();

    //verify group count
    if (allGroupSize < numberOfMembersPerSegment) {
      logger.error(
          "Groups not enough! Groups count: {} less than expected least instance number: {} for "
              + "segment",
          allGroupSize, numberOfMembersPerSegment);
      throw new NotEnoughGroupExceptionThrift().setMinGroupsNumber(numberOfMembersPerSegment);
    } else if (normalDatanodeGroupIdSet.size() + mixedGroupIdSet.size()
        < numberOfNormalPerSegment) {
      logger.error(
          "Normal groups not enough! Normal Groups count: {} less than expected least normal "
              + "instance counter: {} ",
          normalDatanodeGroupIdSet.size() + mixedGroupIdSet.size(), numberOfNormalPerSegment);
      throw new NotEnoughNormalGroupExceptionThrift().setMinGroupsNumber(numberOfNormalPerSegment);
    }

    //calculate segment unit count will be create
    int numberOfNormalWillReservePerSegment = numberOfNormalPerSegment;
    int numberOfArbiterWillReservePerSegment = numberOfArbitersPerSegment;
    int faultNormalCountPerSegment =
        numberOfNormalPerSegment + faultTolerantNormalInstanceCountMax;
    int faultArbiterCountPerSegment =
        numberOfArbitersPerSegment + faultTolerantSimpleInstanceCountMax;
    if (numberOfArbiterWillReservePerSegment == 0) {
      faultArbiterCountPerSegment = 0;
    }

    //if has faultTolerant, calculate normal or arbiter segment unit count that will be create
    if (faultTolerant) {

      Deque<GroupFlag> groupModelQueue = new ArrayDeque<>();

      //order Group for calculate
      for (int i = 0; i < allGroupSize; i++) {
        if (i < simpleDatanodeGroupIdSet.size()) {
          groupModelQueue.addLast(GroupFlag.SIMPLE);
        } else if (i < mixedGroupIdSet.size()) {
          groupModelQueue.addLast(GroupFlag.MIXED);
        } else {
          groupModelQueue.addLast(GroupFlag.NORMAL);
        }
      }

      //distribute arbiter that must be required
      for (int i = 0; i < numberOfArbiterWillReservePerSegment; i++) {
        groupModelQueue.pollFirst();
      }
      for (int i = 0; i < numberOfNormalWillReservePerSegment; i++) {
        groupModelQueue.pollLast();
      }

      while (groupModelQueue.size() > 0) {
        //fault normal segment unit and fault arbiter segment unit all be created, break;
        if (numberOfNormalWillReservePerSegment >= faultNormalCountPerSegment
            && numberOfArbiterWillReservePerSegment >= faultArbiterCountPerSegment) {
          break;
        }

        GroupFlag lastGroup = groupModelQueue.pollLast();
        if (lastGroup == GroupFlag.SIMPLE) {
          //simple datanode can only be created arbiter segment unit,
          if (numberOfArbiterWillReservePerSegment < faultArbiterCountPerSegment) {
            numberOfArbiterWillReservePerSegment++;
          } else {
            break;
          }
        } else {
          //crate normal fault datanode priority, then create arbiter
          if (numberOfNormalWillReservePerSegment < faultNormalCountPerSegment) {
            numberOfNormalWillReservePerSegment++;
          } else if (numberOfArbiterWillReservePerSegment < faultArbiterCountPerSegment) {
            numberOfArbiterWillReservePerSegment++;
          }
        }
      }
    }

    Map<SegmentUnitTypeThrift, Integer> returnMap = new HashMap<>();
    returnMap.put(SegmentUnitTypeThrift.Arbiter, numberOfArbiterWillReservePerSegment);
    returnMap.put(SegmentUnitTypeThrift.Normal, numberOfNormalWillReservePerSegment);
    return returnMap;
  }

  @Deprecated
  private boolean canCreateOneMoreSegmentUnits(InstanceInfoImpl instanceInfo,
      boolean isSimpleConfiguration) {
    if (isSimpleConfiguration) {
      return instanceInfo.getFreeFlexibleSegmentUnitCount() >= 1;
    } else {
      return instanceInfo.getFreeSpace() >= segmentSize;
    }
  }

  /**
   * print log that reserve volume result.
   *
   * @param volumeType            volume type
   * @param segmentWrapSize       wrapper count
   * @param isSimpleConfiguration is simple volume
   * @param segIndex2Instances    (segment index, result)
   */
  public void printResultLog(VolumeType volumeType, int segmentWrapSize,
      boolean isSimpleConfiguration,
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          segIndex2Instances) {
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();
    Map<Long, ObjectCounter<Long>> wrapperIndex2NormalCounterMap = new HashMap<>();

    for (int segIndex : segIndex2Instances.keySet()) {

      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> instanceListFromRemote =
          segIndex2Instances
              .get(segIndex);
      List<InstanceIdAndEndPointThrift> arbiterInstanceList = instanceListFromRemote
          .get(SegmentUnitTypeThrift.Arbiter);

      //count necessary arbiter
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiterIdCounter.increment(arbiterInstanceList.get(i).getInstanceId());
      }

      SegmentUnitTypeThrift segUnitType = SegmentUnitTypeThrift.Normal;
      if (isSimpleConfiguration) {
        segUnitType = SegmentUnitTypeThrift.Flexible;
      }

      List<InstanceIdAndEndPointThrift> normalInstanceList = instanceListFromRemote
          .get(segUnitType);

      ObjectCounter<Long> normalCounter = wrapperIndex2NormalCounterMap
          .computeIfAbsent((long) segIndex / segmentWrapSize,
              counter -> new TreeSetObjectCounter<>());

      //count necessary primary
      long primaryId = normalInstanceList.get(0).getInstanceId();
      primaryIdCounter.increment(primaryId);
      normalCounter.increment(primaryId);

      ObjectCounter<Long> secondaryOfPrimaryCounter = primaryId2SecondaryIdCounterMap
          .computeIfAbsent(primaryId,
              k -> new TreeSetObjectCounter<>());

      //count necessary secondary
      for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
        long secondaryId = normalInstanceList.get(i).getInstanceId();
        secondaryIdCounter.increment(secondaryId);

        secondaryOfPrimaryCounter.increment(secondaryId);
        normalCounter.increment(secondaryId);
      }
    }

    logger.warn("reserveVolume result of primary distribution: {}", primaryIdCounter);
    logger.warn("reserveVolume result of secondary distribution: {}", secondaryIdCounter);
    logger.warn("reserveVolume result of arbiter distribution: {}", arbiterIdCounter);
    logger.warn(
        "reserveVolume result of secondary when primary down(primaryId, secondary distribution): "
            + "{}",
        primaryId2SecondaryIdCounterMap);
    logger.debug(
        "reserveVolume result of wrapperIndex2NormalCounterMap(wrapperIndex, secondary "
            + "distribution): {}",
        wrapperIndex2NormalCounterMap);
  }

  /**
   * reserve a passel of segment units.
   *
   * @param numOfSegmentsWeWillReserve segment count
   * @param membersCountPerSegment     segment unit count per segment
   * @return instances with count, added up to be the segment units' count
   * @throws NotEnoughSpaceExceptionThrift if we don't have enough space for the request
   */
  @Deprecated
  ObjectCounter<InstanceId> reserveNormalSegmentUnits(int numOfSegmentsWeWillReserve,
      int membersCountPerSegment,
      boolean isSimpleConfiguration) throws NotEnoughSpaceExceptionThrift {
    ObjectCounter<Integer> groupIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<InstanceId> instanceIdCounter = new TreeSetObjectCounter<>();
    TreeSet<InstanceInfoImpl> overloadedInstanceSet = new TreeSet<>();
    // for the excepted instance, we will remove it temporarily. They will be added back in the end
    List<InstanceInfoImpl> exceptedInstances = new ArrayList<>();
    for (int i = 0; i < numOfSegmentsWeWillReserve; i++) {
      for (int j = 0; j < membersCountPerSegment; j++) {
        while (true) {
          boolean overloaded = false;
          // poll out the first element with the minimum pressure from a sorted set.
          InstanceInfoImpl instance = instanceInfoSet.pollFirst();
          if (instance == null) {
            instance = overloadedInstanceSet.pollFirst();
            overloaded = true;
            if (instance == null) {
              throw new NotEnoughSpaceExceptionThrift();
            }
          }
          if (!canCreateOneMoreSegmentUnits(instance, isSimpleConfiguration)) {
            // not enough space in this instance
            logger.warn("no enough space left in {}", instance);
            continue;
          }

          int groupReservedCount = (int) groupIdCounter.get(instance.getGroupId());
          if (groupReservedCount == numOfSegmentsWeWillReserve) {
            // we have too much instance on the same group
            logger.debug("we have too much instance in group {}", instance);
            exceptedInstances.add(instance);
            continue;
          } else {
            Validate.isTrue(groupReservedCount < numOfSegmentsWeWillReserve);
          }

          int instanceReservedCount = (int) instanceIdCounter.get(instance.getInstanceId());
          if (!overloaded && instanceReservedCount > instance.getDiskCount()) {


            logger.debug("instance overloaded {}", instance);
            overloadedInstanceSet.add(instance);
            exceptedInstances.add(instance);
            continue;
          }

          if (isSimpleConfiguration) {
            instance.addAbogusFlexibleSegmentUnit();
          } else {
            instance.addAbogusSegmentUnit();
          }
          groupIdCounter.increment(instance.getGroupId());
          instanceIdCounter.increment(instance.getInstanceId());
          if (overloaded) {
            overloadedInstanceSet.add(instance);
          } else {
            instanceInfoSet.add(instance);
          }
          break;
        }
      }
    }
    instanceInfoSet.addAll(exceptedInstances);
    return instanceIdCounter;
  }

  /**
   * distribute a passel of instances into each segment, and ensure that any two of segment units in
   * the same segment don't belong to the same group.
   */
  @Deprecated
  Map<Integer, LinkedList<InstanceIdAndEndPointThrift>> distributeInstanceIntoEachSegment(
      int numOfSegmentsWeReserved, int numberOfNormalSegmentUnitsPerSegment,
      ObjectCounter<InstanceId> normalHosts) throws NotEnoughSpaceExceptionThrift {

    ObjectCounter<Integer> groupCounter = new TreeSetObjectCounter<>();

    // a map from segment index to a list of instances to return to client
    Map<Integer, LinkedList<InstanceIdAndEndPointThrift>> segIndex2Instances = new HashMap<>();
    for (InstanceId instanceId : normalHosts.getAll()) {
      InstanceMetadata instance = storageStore.get(instanceId.getId());
      groupCounter.increment(instance.getGroup().getGroupId(), normalHosts.get(instanceId));
    }

    for (int segmentIndex = 0; segmentIndex < numOfSegmentsWeReserved; segmentIndex++) {
      logger.debug("selecting for segment {} {}", segmentIndex, normalHosts);
      LinkedList<InstanceIdAndEndPointThrift> instanceListToReturn = new LinkedList<>();
      segIndex2Instances.put(segmentIndex, instanceListToReturn);

      // we will always pick up an instance from group with most instances left
      Iterator<Integer> groupIterator = groupCounter.descendingIterator();
      // store the selected groups in a set, and decrement all of them in the end
      Set<Integer> groupIdSet = new HashSet<>();

      logger.debug("group counter {}", groupCounter);
      Set<InstanceId> segmentUnitInstanceIdSet = new HashSet<>();
      for (int j = 0; j < numberOfNormalSegmentUnitsPerSegment; j++) {
        int groupId = groupIterator.next();

        Iterator<InstanceId> instanceIterator = normalHosts.descendingIterator();
        while (instanceIterator.hasNext()) {
          InstanceId id = instanceIterator.next();

          Validate.isTrue(normalHosts.get(id) > 0);
          InstanceMetadata instance = storageStore.get(id.getId());
          if (instance.getGroup().getGroupId() != groupId) {
            logger.debug("not suitable {} g {}", id, instance.getGroup().getGroupId());
            continue;
          }
          Validate.notNull(instance);
          normalHosts.decrement(instance.getInstanceId());
          segmentUnitInstanceIdSet.add(instance.getInstanceId());
          groupIdSet.add(instance.getGroup().getGroupId());
          logger.debug("got {} counter {}", instance.getInstanceId(), normalHosts);
          break;
        }
      }
      for (Integer groupId : groupIdSet) {
        groupCounter.decrement(groupId);
      }
      logger.debug("s : {}", segmentUnitInstanceIdSet);
      for (InstanceId instanceId : segmentUnitInstanceIdSet) {
        instanceListToReturn.addLast(
            RequestResponseHelper
                .buildInstanceIdAndEndPointFrom(storageStore.get(instanceId.getId())));
      }
    }
    return segIndex2Instances;
  }

  private enum GroupFlag {
    SIMPLE("S"),          //group which only contains simple datanode
    MIXED(
        "M"),           //group which contains simple datanode and normal datanode at the same time
    NORMAL("N");          //group which only contains normal datanode

    private final String flag;

    GroupFlag(String flag) {
      this.flag = flag;
    }
  }
}
