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

package py.infocenter.rebalance.builder;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.RawArchiveMetadata;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.struct.SimulateInstanceInfo;
import py.infocenter.store.StorageStore;
import py.informationcenter.StoragePool;
import py.instance.InstanceId;
import py.thrift.share.DatanodeTypeNotSetExceptionThrift;
import py.volume.VolumeType;


public class SimulateInstanceBuilder {

  private static final Logger logger = LoggerFactory.getLogger(SimulateInstanceBuilder.class);

  private StoragePool storagePool;
  private StorageStore storageStore;
  private long segmentSize;
  private VolumeType volumeType;
  private Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap = new HashMap<>(); //all
  // instance info (include simple and normal)
  private Map<Long, SimulateInstanceInfo> simpleDatanodeMap = new HashMap<>();   //all simple 
  // instance info (include simple in domain)

  private Set<Integer> allGroupSet = new HashSet<>();   // all group set, to check if we have 
  // enough instances in different groups
  private Set<Integer> simpleGroupIdSet = new HashSet<>();    //simple datanode Group Id (only 
  // have simple datanode)
  private Set<Integer> normalGroupIdSet = new HashSet<>();    //normal datanode Group Id (only 
  // have normal datanode)
  private Set<Integer> mixGroupIdSet = new HashSet<>();       //group which has simple datanode 
  // and has normal datanode

  private int usableMixGroupCountPerOneSegment;       //mix group max count that can be reserved 
  // to P or S per one segment

  public SimulateInstanceBuilder(StoragePool storagePool, StorageStore storageStore,
      long segmentSize, VolumeType volumeType) {
    this.storagePool = storagePool;
    this.storageStore = storageStore;
    this.segmentSize = segmentSize;
    this.volumeType = volumeType;
  }

  public StoragePool getStoragePool() {
    return storagePool;
  }

  public void setStoragePool(StoragePool storagePool) {
    this.storagePool = storagePool;
  }

  public StorageStore getStorageStore() {
    return storageStore;
  }

  public void setStorageStore(StorageStore storageStore) {
    this.storageStore = storageStore;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public Map<Long, SimulateInstanceInfo> getInstanceId2SimulateInstanceMap() {
    return new HashMap<>(instanceId2SimulateInstanceMap);
  }

  public Map<Long, SimulateInstanceInfo> getSimpleDatanodeMap() {
    return new HashMap<>(simpleDatanodeMap);
  }

  public Set<Integer> getAllGroupSet() {
    return new HashSet<>(allGroupSet);
  }

  public Set<Integer> getSimpleGroupIdSet() {
    return new HashSet<>(simpleGroupIdSet);
  }

  public Set<Integer> getNormalGroupIdSet() {
    return new HashSet<>(normalGroupIdSet);
  }

  public Set<Integer> getMixGroupIdSet() {
    return new HashSet<>(mixGroupIdSet);
  }

  public int getUsableMixGroupCountPerOneSegment() {
    return usableMixGroupCountPerOneSegment;
  }

  /**
   * get simulation instance information 1.datanode status must ok 2.datanode in pool
   *
   * @return list of simulation instance
   */
  public SimulateInstanceBuilder collectionInstance() throws DatanodeTypeNotSetExceptionThrift {
    List<InstanceMetadata> instanceList = storageStore.list();
    logger.debug(
        "build instanceInfoSet, storageStore.list size:{} storagePool.getArchivesInDataNode:{}",
        instanceList.size(), storagePool.getArchivesInDataNode());
    int totalWeight = 0;
    long domainId = storagePool.getDomainId();
    Multimap<Long, Long> insId2ArchiveInPoolMap = storagePool.getArchivesInDataNode();
    for (InstanceMetadata instance : instanceList) {
      InstanceId instanceId = instance.getInstanceId();


      if (instance.getDatanodeStatus() != InstanceMetadata.DatanodeStatus.OK) {
        logger.warn("instance:{} status is not OK, cannot add in instanceMap", instanceId);
        continue;
      }


      if (insId2ArchiveInPoolMap.get(instanceId.getId()).isEmpty()) {
        if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE
            && instance.getDomainId() != null && instance.getDomainId() == domainId) {
          SimulateInstanceInfo simulateInstanceInfo = new SimulateInstanceInfo(instanceId, null,
              instance.getGroup().getGroupId(), domainId, 0, 0, segmentSize,
              0, 0, instance.getDatanodeType());
          simpleDatanodeMap.put(instanceId.getId(), simulateInstanceInfo);
        }

        logger
            .info("instance:{} not in pool:{}, cannot add in instanceMap", instance.getInstanceId(),
                storagePool.getPoolId());
        continue;
      }


      long freeSpace = 0;
      int datanodeWeight = 0;
      int freeFlexibleSegmentUnitCount = 0;
      List<Long> archiveIds = new ArrayList<>();
      for (Long archiveId : insId2ArchiveInPoolMap.get(instanceId.getId())) {
        RawArchiveMetadata archive = instance.getArchiveById(archiveId);
        if (archive != null) {
          freeSpace += archive.getLogicalFreeSpace();
          freeFlexibleSegmentUnitCount += archive.getFreeFlexibleSegmentUnitCount();
          archiveIds.add(archiveId);
          datanodeWeight += archive.getWeight();
        }
      }

      if (datanodeWeight == 0 || freeSpace == 0) {
        logger.info("instance:{} has free space:{}, weight:{}", instanceId, freeSpace,
            datanodeWeight);
      }


      totalWeight += datanodeWeight;

      SimulateInstanceInfo simulateInstanceInfo = new SimulateInstanceInfo(instanceId, archiveIds,
          instance.getGroup().getGroupId(), domainId, freeSpace, freeFlexibleSegmentUnitCount,
          segmentSize,
          datanodeWeight, datanodeWeight, instance.getDatanodeType());

      logger.info("collect an instance:{}", simulateInstanceInfo);
      if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE) {
        simpleDatanodeMap.put(instanceId.getId(), simulateInstanceInfo);
      }
      instanceId2SimulateInstanceMap.put(instanceId.getId(), simulateInstanceInfo);
    }

    //calc used weight
    calcAndSetUsedWeight(totalWeight);

    classifyInstance();

    return this;
  }


  private void calcAndSetUsedWeight(int totalWeight) {

    int maxWeightOfInstance = totalWeight / volumeType.getNumMembers();
    LinkedList<SimulateInstanceInfo> simulateInstanceInfoList = new LinkedList<>(
        instanceId2SimulateInstanceMap.values());
    Collections.sort(simulateInstanceInfoList);

    LinkedList<SimulateInstanceInfo> simulateInstanceInfoListBac = new LinkedList<>(
        simulateInstanceInfoList);

    int notUsedWeight = 0;
    int remainWeight = totalWeight;
    while (!simulateInstanceInfoList.isEmpty()) {
      SimulateInstanceInfo simulateInstanceInfo = simulateInstanceInfoList.pollLast();
      int realWeight = simulateInstanceInfo.getRealWeight();

      if (totalWeight == 0) {
        simulateInstanceInfo.setWeight(simulateInstanceInfo.getDiskCount());
        logger.info(
            "totalWeight is 0, instance:{} real weight:{} is invalid, when volume type is {}, "
                + "change it to weight:{}",
            simulateInstanceInfo.getInstanceId(), realWeight, volumeType,
            simulateInstanceInfo.getWeight());
        continue;
      }

      if (realWeight > maxWeightOfInstance) {
        notUsedWeight += (realWeight - maxWeightOfInstance);
        remainWeight -= realWeight;
        simulateInstanceInfo.setWeight(maxWeightOfInstance);
        logger.info(
            "instance:{} real weight:{} is invalid, when volume type is {}, change it to weight:{}",
            simulateInstanceInfo.getInstanceId(), realWeight, volumeType, maxWeightOfInstance);
      } else if (notUsedWeight <= 0) {
        break;
      } else {
        int addWeight = (int) (realWeight * notUsedWeight / (double) remainWeight);


        if (realWeight + addWeight > maxWeightOfInstance) {
          notUsedWeight -= (maxWeightOfInstance - realWeight);
          remainWeight -= realWeight;
          simulateInstanceInfo.setWeight(maxWeightOfInstance);
        } else {
          notUsedWeight -= addWeight;
          remainWeight -= realWeight;
          simulateInstanceInfo.setWeight(realWeight + addWeight);
        }

        logger.info(
            "instance:{} real weight:{} is invalid, when volume type is {}, change it to weight:{}",
            simulateInstanceInfo.getInstanceId(), realWeight, volumeType,
            simulateInstanceInfo.getWeight());
      }
    }   //while(!simulateInstanceInfoList.isEmpty()){


    if (notUsedWeight > 0) {
      while (!simulateInstanceInfoList.isEmpty()) {
        SimulateInstanceInfo simulateInstanceInfo = simulateInstanceInfoListBac.pollLast();
        simulateInstanceInfo.setWeight(simulateInstanceInfo.getWeight() + 1);
      }
    }

  }

  /**
   * classify instance to simple, normal, mix. Then collect group information
   */
  private void classifyInstance() throws DatanodeTypeNotSetExceptionThrift {
    long domainId = storagePool.getDomainId();

    //for (simpleDatanodeManager.getSimpleDatanodeGroupIdSet())
    // initialize group set and simple datanode hosts
    for (SimulateInstanceInfo instanceInfo : instanceId2SimulateInstanceMap.values()) {
      int groupId = instanceInfo.getGroupId();

      if (instanceInfo.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL) {
        normalGroupIdSet.add(groupId);
      } else if (instanceInfo.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE) {
        simpleGroupIdSet.add(groupId);
      } else {
        logger.error("datanode type not set! instance:{}", instanceInfo.getInstanceId());
        throw new DatanodeTypeNotSetExceptionThrift();
      }

      allGroupSet.add(groupId);
    }

    //simpleDatanode may be have no archives, so it cannot add to pool
    //we will create arbiter at all simpleDatanode which be owned domain
    for (SimulateInstanceInfo instanceInfo : simpleDatanodeMap.values()) {
      int groupId = instanceInfo.getGroupId();

      if (instanceInfo.getDomainId() == domainId) {
        allGroupSet.add(groupId);
        simpleGroupIdSet.add(groupId);
      }
    }

    //get MixGroup
    mixGroupIdSet.addAll(simpleGroupIdSet);
    mixGroupIdSet.retainAll(normalGroupIdSet);

    simpleGroupIdSet.removeAll(mixGroupIdSet);
    normalGroupIdSet.removeAll(mixGroupIdSet);

    usableMixGroupCountPerOneSegment = calcUsableMixGroupCountPerOneSegment();
  }


  private int calcUsableMixGroupCountPerOneSegment() {
    //calc usable max count of mix group in same segment
    int usableMixGroupCountPerOneSegment = mixGroupIdSet.size();
    int remainArbiterToCreate = volumeType.getNumArbiters() - simpleGroupIdSet.size();
    if (remainArbiterToCreate >= mixGroupIdSet.size()) {
      usableMixGroupCountPerOneSegment = 0;
    } else if (remainArbiterToCreate > 0 && remainArbiterToCreate < mixGroupIdSet.size()) {
      usableMixGroupCountPerOneSegment = mixGroupIdSet.size() - remainArbiterToCreate;
    }

    return usableMixGroupCountPerOneSegment;
  }

  /**
   * Remove all nodes that cannot be used to P/S forever
   *
   * <p>these nodes are: 1. all simple datanode 2. nodes in mix group, when (mix group + simple
   * group <= volume arbiter count(exclude candidate arbiter))
   *
   * @param volumeType volumeType
   */
  public Map<Long, SimulateInstanceInfo> removeCanNotUsedInstance(VolumeType volumeType) {
    Map<Long, SimulateInstanceInfo> canUsedInstanceId2SimulateInstanceMap = new HashMap<>(
        instanceId2SimulateInstanceMap);

    Set<Long> removeInstanceSet = new HashSet<>();
    for (long instanceId : canUsedInstanceId2SimulateInstanceMap.keySet()) {
      SimulateInstanceInfo simulateInstanceInfo = canUsedInstanceId2SimulateInstanceMap
          .get(instanceId);

      /*
       * remove all normal datanode of mix group,
       * when (mix group + simple group <= volume arbiter count(exclude candidate arbiter))
       */
      if (mixGroupIdSet.size() + simpleGroupIdSet.size() <= volumeType.getNumArbiters()) {
        if (mixGroupIdSet.contains(simulateInstanceInfo.getGroupId())) {
          removeInstanceSet.add(instanceId);
          continue;
        }
      }

      /*
       * remove simple datanode
       */
      if (simulateInstanceInfo.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE) {
        removeInstanceSet.add(instanceId);
      }
    }
    for (long removeInsId : removeInstanceSet) {
      canUsedInstanceId2SimulateInstanceMap.remove(removeInsId);
    }
    return canUsedInstanceId2SimulateInstanceMap;
  }

  /**
   * calculate the max reserve count of instance.
   *
   * @param instanceWeight         weight of instance
   * @param totalWeight            all instance weight
   * @param allReserveSegmentCount all segment count will be reserved
   * @return expired max reserve count
   */
  private long calcMaxReserveCountForInstance(int instanceWeight, int totalWeight,
      long allReserveSegmentCount) {
    double percentage = (double) instanceWeight / totalWeight;
    long maxCountForInstance = Math.round(percentage * allReserveSegmentCount);



    if (instanceWeight != 0 && maxCountForInstance == 0) {
      maxCountForInstance = 1;
    }

    return maxCountForInstance;
  }


  private double calcReservedRatio(long alreadyDistributeCount, long expiredMaxCount) {
    return (double) (alreadyDistributeCount) / (double) expiredMaxCount * 10000;
  }


  public ObjectCounter<Long> getExpectReserveCount(Collection<Long> instanceIdList,
      Set<Long> excludeInstanceIdSet,
      long volumeSegmentCount, long expiredReserveCount,
      boolean isModShuffleDistribute, boolean isCalcByWeight) {
    ObjectCounter<Long> expectReserveCounter = new TreeSetObjectCounter<>();   //<instanceId, 
    // expiredCount>

    List<Long> canUsedInstanceIdList = new LinkedList<>(instanceIdList);
    if (excludeInstanceIdSet != null) {
      canUsedInstanceIdList.removeIf(excludeInstanceIdSet::contains);
    }

    if (canUsedInstanceIdList.isEmpty()) {
      return expectReserveCounter;
    } else if (canUsedInstanceIdList.size() == 1) {
      expectReserveCounter
          .set(canUsedInstanceIdList.get(0), Math.min(volumeSegmentCount, expiredReserveCount));
      return expectReserveCounter;
    }

    Multimap<Integer, Long> weight2InstanceIdMap = HashMultimap.create();
    Map<Long, Integer> instance2WeightMap = new HashMap<>();
    //calculate total weight
    int totalWeight = 0;
    for (long instanceId : canUsedInstanceIdList) {
      SimulateInstanceInfo simulateInstanceInfo = instanceId2SimulateInstanceMap.get(instanceId);
      int weightTemp = simulateInstanceInfo.getWeight();
      totalWeight += simulateInstanceInfo.getWeight();
      instance2WeightMap.put(instanceId, weightTemp);
      weight2InstanceIdMap.put(weightTemp, instanceId);
    }

    if (canUsedInstanceIdList.isEmpty()) {
      return expectReserveCounter;
    }

    List<Long> canUsedInstanceIdBySortList = new LinkedList<>();
    if (isModShuffleDistribute) {


      if (isCalcByWeight) {
        //sort by weight(from bigger to litter)
        List<Integer> weightList = new LinkedList<>(weight2InstanceIdMap.keySet());
        Collections.sort(weightList);
        Collections.reverse(weightList);

        for (int weight : weightList) {
          //shuffle
          List<Long> weightInsList = new LinkedList<>(weight2InstanceIdMap.get(weight));
          Collections.shuffle(weightInsList);

          canUsedInstanceIdBySortList.addAll(weightInsList);
        }
      } else {
        canUsedInstanceIdBySortList.addAll(canUsedInstanceIdList);
        Collections.shuffle(canUsedInstanceIdBySortList);
      }
    } else {
      canUsedInstanceIdBySortList.addAll(canUsedInstanceIdList);

      //in order of instance by weight(from bigger to litter)
      Collections.sort(canUsedInstanceIdBySortList);
      Collections.reverse(canUsedInstanceIdBySortList);
    }

    ObjectCounter<Long> groupInstanceExpectCounter = calcExpectReserveCount(expiredReserveCount,
        volumeSegmentCount,
        new LinkedList<>(canUsedInstanceIdBySortList), instance2WeightMap, totalWeight,
        isCalcByWeight);

    for (long instanceId : groupInstanceExpectCounter.getAll()) {
      expectReserveCounter.set(instanceId, groupInstanceExpectCounter.get(instanceId));
    }

    return expectReserveCounter;
  }


  public ObjectCounter<Long> getExpectReserveCountByGroup(Collection<Long> instanceIdList,
      Set<Long> excludeInstanceIdSet,
      long volumeSegmentCount, long expectReserveCount,
      boolean isModShuffleDistribute, boolean isCalcByWeight) {
    ObjectCounter<Long> expectReserveCounter = new TreeSetObjectCounter<>();   //<instanceId, 
    // expiredCount>

    List<Long> canUsedInstanceIdList = new LinkedList<>(instanceIdList);
    if (excludeInstanceIdSet != null) {
      canUsedInstanceIdList.removeIf(excludeInstanceIdSet::contains);
    }

    if (canUsedInstanceIdList.isEmpty()) {
      return expectReserveCounter;
    } else if (canUsedInstanceIdList.size() == 1) {
      expectReserveCounter
          .set(canUsedInstanceIdList.get(0), Math.min(volumeSegmentCount, expectReserveCount));
      return expectReserveCounter;
    }

    Multimap<Integer, Long> group2InstanceIdMap = HashMultimap.create();
    Map<Integer, Integer> group2WeightMap = new HashMap<>();

    Map<Long, Integer> instance2WeightMap = new HashMap<>();

    //calculate total weight
    for (long instanceId : canUsedInstanceIdList) {
      SimulateInstanceInfo simulateInstanceInfo = instanceId2SimulateInstanceMap.get(instanceId);

      int groupId = simulateInstanceInfo.getGroupId();
      group2InstanceIdMap.put(groupId, instanceId);

      if (!group2WeightMap.containsKey(groupId)) {
        group2WeightMap.put(simulateInstanceInfo.getGroupId(), 0);
      }
      group2WeightMap.put(simulateInstanceInfo.getGroupId(),
          group2WeightMap.get(groupId) + simulateInstanceInfo.getWeight());

      instance2WeightMap.put(instanceId, simulateInstanceInfo.getWeight());
    }

    int totalGroupWeight = 0;
    for (int groupId : group2WeightMap.keySet()) {
      int groupWeight = group2WeightMap.get(groupId) / group2InstanceIdMap.get(groupId).size();
      group2WeightMap.put(groupId, groupWeight);
      totalGroupWeight += groupWeight;
    }

    //calculate by group
    LinkedList<Integer> groupIdList = new LinkedList<>(group2WeightMap.keySet());

    if (isModShuffleDistribute) {
      Collections.shuffle(groupIdList);
    } else {
      //in order of instance by weight(from bigger to litter)
      Collections.sort(groupIdList);
      Collections.reverse(groupIdList);
    }
    ObjectCounter<Integer> groupExpectReserveCounter = calcExpectReserveCount(expectReserveCount,
        volumeSegmentCount,
        groupIdList, group2WeightMap, totalGroupWeight, isCalcByWeight);

    for (int groupId : group2WeightMap.keySet()) {
      long groupExpectCount = groupExpectReserveCounter.get(groupId);

      int currentGroupTotalWeight = 0;
      LinkedList<Long> groupInstanceIdList = new LinkedList<>(group2InstanceIdMap.get(groupId));
      Collections.shuffle(groupInstanceIdList);

      Map<Long, Integer> groupInstanceId2WeightMap = new HashMap<>();
      for (long instanceId : groupInstanceIdList) {
        groupInstanceId2WeightMap.put(instanceId, instance2WeightMap.get(instanceId));
        currentGroupTotalWeight += instance2WeightMap.get(instanceId);
      }

      ObjectCounter<Long> groupInstanceExpectCounter = calcExpectReserveCount(groupExpectCount,
          groupExpectCount,
          groupInstanceIdList, groupInstanceId2WeightMap, currentGroupTotalWeight, isCalcByWeight);

      for (long instanceId : groupInstanceExpectCounter.getAll()) {
        expectReserveCounter.set(instanceId, groupInstanceExpectCounter.get(instanceId));
      }
    }

    return expectReserveCounter;
  }

  /**
   * calculate expect reserve count by weight.
   *
   * @param expectReserveCount expect reserve count
   * @param volumeSegmentCount volume total segment count
   * @param objectList         instance or group list
   * @param object2WeightMap   instance or group and weight map
   * @param totalWeight        total weight
   * @param <T>                instance or group
   * @return expect reserve counter
   */
  private <T> ObjectCounter<T> calcExpectReserveCount(long expectReserveCount,
      long volumeSegmentCount, LinkedList<T> objectList,
      Map<T, Integer> object2WeightMap, int totalWeight, boolean isCalcByWeight) {
    ObjectCounter<T> expectReserveCounter = new TreeSetObjectCounter<>();

    Validate.isTrue(objectList.size() == object2WeightMap.size(),
        "objectList size != object2WeightMap");

    if (totalWeight == 0) {
      isCalcByWeight = false;
    }

    long remainder = expectReserveCount;
    for (T objId : objectList) {
      long desiredCount;
      int objWeight = object2WeightMap.get(objId);

      if (isCalcByWeight) {
        desiredCount = calcMaxReserveCountForInstance(objWeight, totalWeight, expectReserveCount);


        if (expectReserveCount > volumeSegmentCount) {
          if (desiredCount > volumeSegmentCount) {
            desiredCount = volumeSegmentCount;
          }
          expectReserveCount -= desiredCount;
          totalWeight -= objWeight;
        }
        remainder -= desiredCount;
      } else {
        desiredCount = Math.round((double) expectReserveCount / object2WeightMap.size());
        remainder -= desiredCount;
      }

      expectReserveCounter.set(objId, desiredCount);
    }


    while (remainder > 0) {
      if (object2WeightMap.isEmpty()) {
        break;
      }
      T objId = objectList.pollFirst();
      if (objId == null) {
        break;
      }
      expectReserveCounter.increment(objId);
      remainder--;
    }

    LinkedList<T> objectListTemp = new LinkedList<T>(objectList);
    while (remainder < 0) {
      if (object2WeightMap.isEmpty()) {
        break;
      }

      T objId = objectListTemp.pollLast();
      if (objId == null) {

        objId = objectList.pollLast();
        if (expectReserveCounter.get(objId) <= 0) {
          continue;
        }

        if (objId == null) {
          logger.warn("may be have some error when calcExpectReserveCount");
          break;
        }
      } else if (expectReserveCounter.get(objId) <= 1) {
        continue;
      }

      objectList.remove(objId);
      expectReserveCounter.decrement(objId);
      remainder++;
    }

    return expectReserveCounter;
  }


  public LinkedList<Long> getPrimaryPriorityList(Collection<Long> instanceIdList,
      ObjectCounter<Long> primaryDistributeCounter,
      ObjectCounter<Long> primaryExpectCounter, int volumeSegmentCount) {
    LinkedList<Long> priorityList = new LinkedList<>();

    if (instanceIdList.size() <= 1) {
      priorityList.addAll(instanceIdList);
      return priorityList;
    }

    //get instance expired count
    ObjectCounter<Long> primaryId2DesiredCounter;
    if (primaryExpectCounter == null || primaryExpectCounter.size() == 0) {
      primaryId2DesiredCounter = getExpectReserveCount(instanceIdList, null,
          volumeSegmentCount, volumeSegmentCount, false, true);
    } else {
      primaryId2DesiredCounter = primaryExpectCounter;
    }


    Multimap<Double, Long> freeReserveWeightPercent2InsIdMap = HashMultimap.create();
    for (long instanceId : instanceIdList) {
      double reserveWeightPercent = calcReservedRatio(primaryDistributeCounter.get(instanceId),
          primaryId2DesiredCounter.get(instanceId));
      freeReserveWeightPercent2InsIdMap.put(reserveWeightPercent, instanceId);
    }

    LinkedList<Double> freeReserveWeightPercentList = new LinkedList<>(
        freeReserveWeightPercent2InsIdMap.keySet());
    Collections.sort(freeReserveWeightPercentList);


    for (double freeReserveWeightPercent : freeReserveWeightPercentList) {
      Collection<Long> freeReserveWeightPercentCollection = freeReserveWeightPercent2InsIdMap
          .get(freeReserveWeightPercent);
      if (freeReserveWeightPercentCollection.size() <= 1) {
        priorityList.addAll(freeReserveWeightPercentCollection);
        continue;
      }

      Multimap<Integer, Long> weight2InstanceIdMap = HashMultimap.create();
      for (long instanceId : freeReserveWeightPercentCollection) {
        weight2InstanceIdMap
            .put(instanceId2SimulateInstanceMap.get(instanceId).getWeight(), instanceId);
      }
      LinkedList<Integer> weightList = new LinkedList<>(weight2InstanceIdMap.keySet());
      Collections.sort(weightList);
      Collections.reverse(weightList);

      /*
       * 3.Random ordering of the same weight2InstanceIdMap
       */
      for (int weight : weightList) {
        LinkedList<Long> shuffledList = new LinkedList<>(weight2InstanceIdMap.get(weight));
        Collections.shuffle(shuffledList);
        priorityList.addAll(shuffledList);
      }
    }

    return priorityList;
  }

  /**
   * get secondary priority list The following conditions must be followed： 1. The datanode is
   * sorted according to the PS composite distribution with datanode wight, with the lowest
   * distribution in the front 2. In order of S distribution with datanode wight, the least comes
   * first 3. A random arrangement cause when all of the above conditions are equally
   *
   * @param instanceIdList                     all can used instance, contains primary instance
   * @param primaryId                          primary instance id
   * @param primaryCount                       primary count when instance is primary id
   * @param secondaryOfPrimaryCounter          the distribution of secondary when primary is
   *                                           primaryId
   * @param secondaryDistributeCounter         the distribution of secondary on all nodes
   * @param secondaryIdOfPrimaryExpiredCounter the expired distribution count of secondary on all
   *                                           nodes (Usually, expired time is calculated in real
   *                                           time, and modulus is distributed randomly on
   *                                           instance. But modulus cannot distributed randomly on
   *                                           instance when rebalance, so we take this param when
   *                                           rebalance)
   * @param volumeSegmentCount                 the volume segment count
   * @param secondaryCountPerSegment           secondary counter of segment(without redundency
   *                                           secondary)
   * @return the secondary select list
   */
  public LinkedList<Long> getSecondaryPriorityList(Collection<Long> instanceIdList, long primaryId,
      long primaryCount,
      ObjectCounter<Long> secondaryOfPrimaryCounter, ObjectCounter<Long> secondaryDistributeCounter,
      ObjectCounter<Long> secondaryIdOfPrimaryExpiredCounter,
      long volumeSegmentCount, long secondaryCountPerSegment) {
    LinkedList<Long> selSecondaryList = new LinkedList<>();

    if (instanceIdList.size() <= 1) {
      selSecondaryList.addAll(instanceIdList);
      return selSecondaryList;
    }

    //get <group, instance>
    Multimap<Integer, Long> groupId2InstanceId = HashMultimap.create();
    for (long instanceId : instanceIdList) {
      SimulateInstanceInfo simulateInstanceInfo = instanceId2SimulateInstanceMap.get(instanceId);
      groupId2InstanceId.put(simulateInstanceInfo.getGroupId(), instanceId);
    }

    SimulateInstanceInfo primarySimulateInstance = instanceId2SimulateInstanceMap.get(primaryId);

    // get S max count when primary is primaryId
    long maxSecondaryOfPrimaryReserveCount = primaryCount * secondaryCountPerSegment;

    Set<Long> excludeInstanceSet = new HashSet<>();
    excludeInstanceSet.addAll(groupId2InstanceId.get(primarySimulateInstance.getGroupId()));
    ObjectCounter<Long> secondaryIdOfPrimary2DesiredCounter;

    if (secondaryIdOfPrimaryExpiredCounter == null
        || secondaryIdOfPrimaryExpiredCounter.size() == 0) {
      secondaryIdOfPrimary2DesiredCounter = getExpectReserveCount(instanceIdList,
          excludeInstanceSet,
          volumeSegmentCount, maxSecondaryOfPrimaryReserveCount, true, true);
    } else {
      secondaryIdOfPrimary2DesiredCounter = secondaryIdOfPrimaryExpiredCounter;
    }

    Multimap<Double, Long> secondaryReservePercentOfPrimary2InstanceIdMap = HashMultimap.create();
    for (long instanceId : instanceIdList) {
      if (excludeInstanceSet.contains(instanceId)) {
        continue;
      }
      double reserveWeightPercent = calcReservedRatio(secondaryOfPrimaryCounter.get(instanceId),
          secondaryIdOfPrimary2DesiredCounter.get(instanceId));
      secondaryReservePercentOfPrimary2InstanceIdMap.put(reserveWeightPercent, instanceId);
    }
    LinkedList<Double> reserveWeightPercentList = new LinkedList<>(
        secondaryReservePercentOfPrimary2InstanceIdMap.keySet());
    Collections.sort(reserveWeightPercentList);

    // get S max count of volume
    long maxSecondaryReserveCount = volumeSegmentCount * secondaryCountPerSegment;
    ObjectCounter<Long> secondaryId2DesiredCounter = getExpectReserveCount(instanceIdList, null,
        volumeSegmentCount, maxSecondaryReserveCount, false, true);

    for (double reservePercent : reserveWeightPercentList) {
      Collection<Long> secondaryReservePercentOfPrimary =
          secondaryReservePercentOfPrimary2InstanceIdMap
              .get(reservePercent);
      if (secondaryReservePercentOfPrimary.size() <= 1) {
        selSecondaryList.addAll(secondaryReservePercentOfPrimary);
        continue;
      }

      Multimap<Double, Long> secondaryReservePercent2InstanceIdMap = HashMultimap.create();
      for (long instanceId : secondaryReservePercentOfPrimary) {
        double reserveWeightPercent = calcReservedRatio(secondaryDistributeCounter.get(instanceId),
            secondaryId2DesiredCounter.get(instanceId));
        secondaryReservePercent2InstanceIdMap.put(reserveWeightPercent, instanceId);
      }

      LinkedList<Double> sreservepercentlist = new LinkedList<>(
          secondaryReservePercent2InstanceIdMap.keySet());
      Collections.sort(sreservepercentlist);


      for (double reserveofspercent : sreservepercentlist) {
        Collection<Long> secondaryReservePercent = secondaryReservePercent2InstanceIdMap
            .get(reserveofspercent);
        if (secondaryReservePercent.size() <= 1) {
          selSecondaryList.addAll(secondaryReservePercent);
          continue;
        }

        Multimap<Integer, Long> weight2InstanceIdMap = HashMultimap.create();
        for (long instanceId : secondaryReservePercent) {
          weight2InstanceIdMap
              .put(instanceId2SimulateInstanceMap.get(instanceId).getWeight(), instanceId);
        }
        LinkedList<Integer> sortList = new LinkedList<>(weight2InstanceIdMap.keySet());
        Collections.sort(sortList);
        Collections.reverse(sortList);


        for (int weight : sortList) {
          LinkedList<Long> shuffledList = new LinkedList<>(weight2InstanceIdMap.get(weight));
          Collections.shuffle(shuffledList);
          selSecondaryList.addAll(shuffledList);
        }
      }
    }

    return selSecondaryList;
  }

}
