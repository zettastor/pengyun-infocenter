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

package py.infocenter.instance.manger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import py.common.struct.Pair;
import py.volume.VolumeMetadata;

public class InstanceVolumeInEquilibriumManger {

  private static final Logger logger = LoggerFactory
      .getLogger(InstanceVolumeInEquilibriumManger.class);
  /**
   * just for test.
   ***/
  boolean startTest = true;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  /**
   * for each HA instance volume size Equilibrium, just for master.
   ***/
  //volumeId  instanceId(from)  instanceId(to)
  private Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibriumBuildWithVolumeId;
  //rebuild the Equilibrium table
  //instanceId(from) volumeId instanceId(to)
  private Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibrium;
  //for master; the volume id, the instance from
  private Map<Long, Long> equilibriumOkVolume;
  //set the version, the the equilibrium table change, add the version,
  //if the version change, data node update the table from master
  private AtomicLong updateReportToInstancesVersion = new AtomicLong();
  /**
   * volumeId  instanceId(from, old)  instanceId(to, new) for data node update the report table,
   * when the Equilibrium ok; * the datanode save two report table 1. the table save the (volume id,
   * report instance id)   whichHAThisVolumeToReportMap 2. the Equilibrium table (volume id, report
   * instance id from, report instance id to) volumeReportToInstancesSameTimeMap if Equilibrium ok,
   * update the table 2, and chang the table 1 which instance to report(chang report instance).
   ***/
  private Map<Long, Map<Long, Long>> updateTheDatanodeReportTable;
  /**
   * the volumes count which will Equilibrium.
   **/
  private long countEquilibriumNumber = 0;
  //the all segment number which master or follower can save
  private long allSegmentNumberToSave = 10000;
  //the all segment number which master can save = all.segment.number.to.save * percent.number
  // .master.segment
  private double percentNumberSegmentNumberMasterSave = 0.6;
  private long segmentSize;

  public InstanceVolumeInEquilibriumManger() {
    volumeReportToInstanceEquilibrium = new ConcurrentHashMap<>();
    updateTheDatanodeReportTable = new ConcurrentHashMap<>();
    volumeReportToInstanceEquilibriumBuildWithVolumeId = new ConcurrentHashMap<>();
    equilibriumOkVolume = new ConcurrentHashMap<>();
  }

  //sort map by value
  private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
    Map<K, V> result = new LinkedHashMap<>();
    Stream<Map.Entry<K, V>> st = map.entrySet().stream();

    st.sorted(Comparator.comparing(e -> e.getValue()))
        .forEach(e -> result.put(e.getKey(), e.getValue()));

    return result;
  }

  /**
   * the datanode response update status.
   ****/
  public void removeVolumeWhenDatanodeUpdateOk(Set<Long> volumeUpdateReportTableOk) {
    if (volumeUpdateReportTableOk == null) {
      return;
    }

    for (Long volumeId : volumeUpdateReportTableOk) {
      if (updateTheDatanodeReportTable.containsKey(volumeId)) {
        updateTheDatanodeReportTable.remove(volumeId);
        logger.warn(
            "when Equilibrium, current volume :{} update to data node ok, so remove in update "
                + "table",
            volumeId);
      }
    }
  }

  /**
   * check the Equilibrium is ok or not.
   */
  public boolean equilibriumOk() {
    if (countEquilibriumNumber == equilibriumOkVolume.size() //the volume equilibrium Ok
        && updateTheDatanodeReportTable.isEmpty()

        && volumeReportToInstanceEquilibriumBuildWithVolumeId.isEmpty()) {

      //clean all value, for next time equilibrium
      volumeReportToInstanceEquilibriumBuildWithVolumeId.clear();
      volumeReportToInstanceEquilibrium.clear();
      equilibriumOkVolume.clear();
      countEquilibriumNumber = 0;
      updateReportToInstancesVersion.set(0);

      return true;
    } else {
      return false;
    }
  }

  public void clearAllEquilibriumInfo() {

    //clean all value, for next time equilibrium
    volumeReportToInstanceEquilibriumBuildWithVolumeId.clear();
    volumeReportToInstanceEquilibrium.clear();
    equilibriumOkVolume.clear();
    updateTheDatanodeReportTable.clear();
    countEquilibriumNumber = 0;
    updateReportToInstancesVersion.set(0);
  }

  public void beginBalanceVolume(long masterInstanceId) {
    //begin
    //* instance id, volume id <-> volume size ***/
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = instanceIncludeVolumeInfoManger
        .getEachHaVolumeSizeInfo();
    logger.warn(
        "when init, the config value, number is :{}, percent is :{}, segmentSize :{} , the "
            + "volumeSizeInfo :{}",
        allSegmentNumberToSave, percentNumberSegmentNumberMasterSave, segmentSize, volumeSizeInfo);

    //init a tmp instanceId, volumeId, volumeSize
    //*instanceId,     volumeInfo **/
    MultiValueMap<Long, VolumesForEquilibrium> afterEquilibriumVolumeSizeInfo =
        new LinkedMultiValueMap<>();
    MultiValueMap<Long, VolumesForEquilibrium> beforeEquilibriumVolumeSizeInfo =
        new LinkedMultiValueMap<>();
    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : volumeSizeInfo.entrySet()) {
      List<VolumesForEquilibrium> volumesForEquilibriumList = entry.getValue();
      long instanceId = entry.getKey();

      afterEquilibriumVolumeSizeInfo.put(instanceId, new ArrayList<>(volumesForEquilibriumList));
      beforeEquilibriumVolumeSizeInfo.put(instanceId, new ArrayList<>(volumesForEquilibriumList));

    }

    logger.warn("when begin to calc Equilibrium volume for each instance, the begin table :{}",
        beforeEquilibriumVolumeSizeInfo);

    //** get the master segment number when begin ***/
    int masterSegmentNumberBegin = 0;
    int masterSegmentNumberAfter = 0;
    if (beforeEquilibriumVolumeSizeInfo.containsKey(masterInstanceId)) {
      List<VolumesForEquilibrium> masterVolumeInfoBegin = beforeEquilibriumVolumeSizeInfo
          .get(masterInstanceId);
      for (VolumesForEquilibrium volumesForEquilibrium : masterVolumeInfoBegin) {
        masterSegmentNumberBegin += (int) (Math
            .ceil(volumesForEquilibrium.getVolumeSize() * 1.0 / segmentSize));
      }

      logger.warn("when begin, get the master :{} have the segment number is :{}", masterInstanceId,
          masterSegmentNumberBegin);
    } else {
      logger.warn("when Equilibrium volume, can not find the master id :{} in table",
          masterInstanceId);
      return;
    }

    //get the re balance value
    volumeEquilibriumForEachInstance(afterEquilibriumVolumeSizeInfo);

    //same, so no to move
    if (afterEquilibriumVolumeSizeInfo.equals(beforeEquilibriumVolumeSizeInfo)) {
      logger.warn(
          "when begin to balance volume for each instance, the result is same, so not need do it");
      return;
    }

    //** get the master segment number when after ***/
    if (afterEquilibriumVolumeSizeInfo.containsKey(masterInstanceId)) {
      List<VolumesForEquilibrium> instanceVolumeInfoAfter = afterEquilibriumVolumeSizeInfo
          .get(masterInstanceId);
      for (VolumesForEquilibrium volumesForEquilibrium : instanceVolumeInfoAfter) {
        masterSegmentNumberAfter += (int) (Math
            .ceil(volumesForEquilibrium.getVolumeSize() * 1.0 / segmentSize));
      }

      logger.warn("when after, get the master :{} have the segment number is :{}", masterInstanceId,
          masterSegmentNumberAfter);
    } else {
      logger.warn("when Equilibrium volume, can not find the master id :{} in table",
          masterInstanceId);
      return;
    }

    /*
     * after balance the volume, check the master segment number is in limit or not if the master
     * have more segment, so not need to move, 60%.
     */
    if (masterSegmentNumberAfter > masterSegmentNumberBegin
        && masterSegmentNumberAfter
        > allSegmentNumberToSave * percentNumberSegmentNumberMasterSave) {

      logger.warn(
          "when balance volume for each instance, find the master :{} segment number :{}, is "
              + "large :{}, so remove master and begin again",
          masterInstanceId, masterSegmentNumberAfter,
          allSegmentNumberToSave * percentNumberSegmentNumberMasterSave);

      afterEquilibriumVolumeSizeInfo.clear();
      beforeEquilibriumVolumeSizeInfo.remove(masterInstanceId);

      for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : beforeEquilibriumVolumeSizeInfo
          .entrySet()) {
        List<VolumesForEquilibrium> volumesForEquilibriumList = entry.getValue();
        long instanceId = entry.getKey();
        afterEquilibriumVolumeSizeInfo.put(instanceId, new ArrayList<>(volumesForEquilibriumList));
      }

      //get the re balance value second, not include the master
      volumeEquilibriumForEachInstance(afterEquilibriumVolumeSizeInfo);

      //same, so no to move. second, not include the master
      if (afterEquilibriumVolumeSizeInfo.equals(beforeEquilibriumVolumeSizeInfo)) {
        logger.warn(
            "when begin to balance volume for each instance second, the result is same, so not "
                + "need do it");
        return;
      }
    }

    //save the result
    //compare the before and after, get the different, find which volume will be Equilibrium
    volumeReportToInstanceEquilibrium = findVolumeInWhichInstance(beforeEquilibriumVolumeSizeInfo,
        afterEquilibriumVolumeSizeInfo);

    /* get the all volumes number which will Equilibrium, just for check the Equilibrium is
     * finish or not ***/
    for (Map.Entry<Long, Map<Long, Long>> entry : volumeReportToInstanceEquilibrium.entrySet()) {
      countEquilibriumNumber += entry.getValue().size();
    }
    logger.warn("calc Equilibrium, get the count number:{} volumes which will to Equilibrium",
        countEquilibriumNumber);

    //rebuild the table style, just for easy to do it
    rebuildTheEquilibriumTable();
  }

  //volumeId  instanceId(from)  instanceId(to)
  //123451    111111              111114
  private void rebuildTheEquilibriumTable() {
    //instanceId(from) volumeId instanceId(to)
    for (Map.Entry<Long, Map<Long, Long>> entry : volumeReportToInstanceEquilibrium.entrySet()) {
      long instanceFrom = entry.getKey();
      Map<Long, Long> volumeAndInstanceTo = entry.getValue();

      for (Map.Entry<Long, Long> entry1 : volumeAndInstanceTo.entrySet()) {
        long volumeId = entry1.getKey();
        long instanceTo = entry1.getValue();
        Map<Long, Long> instanceIdsTemp = new HashMap<>();
        instanceIdsTemp.put(instanceFrom, instanceTo);
        volumeReportToInstanceEquilibriumBuildWithVolumeId.put(volumeId, instanceIdsTemp);
      }
    }
    logger.warn("for calc Equilibrium, the rebuild table :{} ",
        volumeReportToInstanceEquilibriumBuildWithVolumeId);
  }

  /**
   * check there volume which form instance is Equilibrium or not Equilibrium status, instance id
   * (form or to), form and to same report one volume.
   */
  public Pair<Boolean, Long> checkVolumeFormNewInstanceEquilibrium(long volumeId, long instanceId) {

    //  result  , old instanceId
    Pair<Boolean, Long> resultValue = new Pair<>();
    resultValue.setFirst(false);
    resultValue.setSecond(0L);
    if (!volumeReportToInstanceEquilibriumBuildWithVolumeId.containsKey(volumeId)) {

      //this volume no Equilibrium
      return resultValue;
    }

    //return  the volume and instance(from)
    Map<Long, Long> instanceIdInfo = volumeReportToInstanceEquilibriumBuildWithVolumeId
        .get(volumeId);

    for (Map.Entry<Long, Long> entry : instanceIdInfo.entrySet()) {
      long instanceIdFrom = entry.getKey();
      long instanceIdTo = entry.getValue();
      if (instanceIdTo == instanceId) {  //return  the volume and instance(from)
        resultValue.setFirst(true);
        resultValue.setSecond(instanceIdFrom);
      } else if (instanceIdFrom == instanceId) {
        resultValue.setFirst(false);
        resultValue.setSecond(instanceIdFrom);
      } else {
        logger.error(
            "find same error in checkVolumeFormNewInstanceEquilibrium, the input value :{} {}, "
                + "and the"

                + "table is :{}", volumeId, instanceId,
            volumeReportToInstanceEquilibriumBuildWithVolumeId);
      }
    }

    return resultValue;
  }


  public long checkVolumeEquilibriumOk(long volumeId) {
    if (equilibriumOkVolume.containsKey(volumeId)) {
      return equilibriumOkVolume.get(volumeId);
    } else {
      return 0;
    }
  }

  /**
   * when the volume Equilibrium ok, remove it in old instance.
   ****/
  public void removeVolumeInEquilibriumTable(long oldInstanceId, long volumeId) {
    //*1.  update the version ***/
    updateReportToInstancesVersion.incrementAndGet();

    //** 2. notify data node update the report volume table ***/
    Map<Long, Long> instanceInfo = volumeReportToInstanceEquilibriumBuildWithVolumeId.get(volumeId);
    if (instanceInfo != null) {
      updateTheDatanodeReportTable.put(volumeId, instanceInfo);
    }

    //*3. remove in  Equilibrium table ***/
    volumeReportToInstanceEquilibriumBuildWithVolumeId.remove(volumeId);

    logger.warn(
        "the volume:{} is Equilibrium ok, old instance id :{}, set some info and remove it in "
            + "table, "

            + "the updateReportToInstancesVersion :{}, the updateTheDatanodeReportTable :{}, the"

            + "volumeReportToInstanceEquilibriumBuildWithVolumeId :{}, the cout :{}, the ok table "
            + ":{}",
        volumeId, oldInstanceId, updateReportToInstancesVersion, updateTheDatanodeReportTable,
        volumeReportToInstanceEquilibriumBuildWithVolumeId, countEquilibriumNumber,
        equilibriumOkVolume);
  }

  //  {111111={123451=111114, 123452=111114}, 111112={223451=111114}} 111114
  //get the current volume move to which instance
  //current instanceId, volumeId, this volume to report new InstanceId
  private Map<Long, Map<Long, Long>> findVolumeInWhichInstance(
      MultiValueMap<Long, VolumesForEquilibrium> beforeVolumesToMove,
      MultiValueMap<Long, VolumesForEquilibrium> afterVolumesToMove) {

    //current instanceId, volumeId, this volume to report new InstanceId
    Map<Long, Map<Long, Long>> setVolumeReportToInstance = new ConcurrentHashMap<>();

    for (Map.Entry<Long, List<VolumesForEquilibrium>> beforeEntry : beforeVolumesToMove
        .entrySet()) {
      long instanceIdBefore = beforeEntry.getKey();
      List<VolumesForEquilibrium> beforeVolumes = beforeEntry.getValue();

      Map<Long, Long> mapForVolumeReportInstance = new ConcurrentHashMap<>();
      //check current volume in which instance
      for (VolumesForEquilibrium volumesForEquilibriumBefore : beforeVolumes) {

        long volumeId = volumesForEquilibriumBefore.getVolumeId();
        for (Map.Entry<Long, List<VolumesForEquilibrium>> afterEntry : afterVolumesToMove
            .entrySet()) {
          long instanceIdAfter = afterEntry.getKey();
          List<VolumesForEquilibrium> afterVolumes = afterEntry.getValue();

          List<Long> afterIncludeVolumes = new ArrayList<>();
          for (VolumesForEquilibrium volumesForEquilibrium : afterVolumes) {
            afterIncludeVolumes.add(volumesForEquilibrium.getVolumeId());
          }

          logger.debug("get the value :{} {} {} {}", afterIncludeVolumes.contains(volumeId),
              instanceIdBefore,
              instanceIdAfter, instanceIdBefore != instanceIdAfter);
          //in different instance
          if (afterIncludeVolumes.contains(volumeId) && instanceIdBefore != instanceIdAfter) {
            mapForVolumeReportInstance.put(volumeId, instanceIdAfter);
          }
        }
      }

      if (!mapForVolumeReportInstance.isEmpty()) {
        setVolumeReportToInstance.put(instanceIdBefore, mapForVolumeReportInstance);
      }
    }

    logger.warn("findVolumeInWhichInstance, get the end value :{}", setVolumeReportToInstance);

    return setVolumeReportToInstance;
  }

  private Map<Long, Map<Long, Long>> findVolumeInWhichInstance_bak(
      Map<Long, List<Long>> beforeVolumesToMove, Map<Long, List<Long>> afterVolumesToMove) {

    //current instanceId, volumeId, this volume to report new InstanceId
    Map<Long, Map<Long, Long>> setVolumeReportToInstance = new ConcurrentHashMap<>();

    for (Map.Entry<Long, List<Long>> beforeEntry : beforeVolumesToMove.entrySet()) {
      long instanceIdBefore = beforeEntry.getKey();
      List<Long> beforeVolumes = beforeEntry.getValue();

      Map<Long, Long> mapForVolumeReportInstance = new ConcurrentHashMap<>();
      //check current volume in which instance
      for (Long volumeId : beforeVolumes) {

        for (Map.Entry<Long, List<Long>> afterEntry : afterVolumesToMove.entrySet()) {
          long instanceIdAfter = afterEntry.getKey();
          List<Long> afterVolumes = afterEntry.getValue();

          //in different instance
          if (afterVolumes.contains(volumeId) && instanceIdBefore != instanceIdAfter) {
            mapForVolumeReportInstance.put(volumeId, instanceIdAfter);
          }
        }
      }

      if (!mapForVolumeReportInstance.isEmpty()) {
        setVolumeReportToInstance.put(instanceIdBefore, mapForVolumeReportInstance);
      }
    }

    logger.warn("findVolumeInWhichInstance, get the end value :{}", setVolumeReportToInstance);

    return setVolumeReportToInstance;
  }

  /**
   * if one instance which have more volume(the total volume size is large) we can move some volume
   * to the instance which not more volume.
   */
  //                     instanceId  volumeId, volumeSize
  public void volumeEquilibriumForEachInstance(
      MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo) {
    Set<Long> instanceWhichNotNeedMove = new HashSet<>();
    long beginTime = System.currentTimeMillis();
    while (true) {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - beginTime > 10 * 1000) { //10s,may be in
        logger.error(
            "find error in volumeEquilibriumForEachInstance, may be current function in loop, so "
                + "break");
        break;
      }

      //there is only one instance to Equilibrium, so not need to move
      if (theEndVolumeSizeInfo.size() - instanceWhichNotNeedMove.size() == 1) {
        logger.warn("calc the Equilibrium table is ok, the end map is :{}", theEndVolumeSizeInfo);
        break;
      }

      //first set true
      long averageSize = 0;
      VolumeInfoForEquilibrium volumeInfoForRebalance = new VolumeInfoForEquilibrium();
      volumeInfoForRebalance.setResetAverageSize(true);

      //when one instance is not need move, so get Average not include this instance
      while (volumeInfoForRebalance.isResetAverageSize()) {
        averageSize = getAverageSize(theEndVolumeSizeInfo, instanceWhichNotNeedMove);
        if (averageSize == 0) {
          return;
        }

        /* 1. get the max instance which to moved ***/
        volumeInfoForRebalance = getTheMaxSizeInstanceToMoved(theEndVolumeSizeInfo, averageSize,
            instanceWhichNotNeedMove);
      }

      boolean rebalanceOk = false;
      long theMaxSizeInstance = 0;
      long volumeIdWhichToMove = 0;
      long volumeSizeWhichToMove = 0;

      theMaxSizeInstance = volumeInfoForRebalance.getTheMaxSizeInstance();
      if (theMaxSizeInstance == 0) {
        rebalanceOk = true;
      } else {
        volumeIdWhichToMove = volumeInfoForRebalance.getVolumeIdWhichToMove();
        volumeSizeWhichToMove = volumeInfoForRebalance.getVolumeSizeWhichToMove();
      }

      if (rebalanceOk) {
        logger.warn("the rebalance is ok 2, the end map is :{}", theEndVolumeSizeInfo);
        break;
      }

      /* 2. get the instance which to move, the small instance **/
      Pair<Long, Long> theSmallInstanceIdAndSize = getWhichInstanceToMove(theEndVolumeSizeInfo,
          averageSize, instanceWhichNotNeedMove);
      long instanceIdToMove = theSmallInstanceIdAndSize.getFirst();

      if (instanceIdToMove == 0) {
        logger.warn("the rebalance is ok 3, the end map is :{}", theEndVolumeSizeInfo);
        break;
      }

      /* 4. put the volume id which move from max instance
       *   and remove it in max instance
       ***/
      logger.warn("get the current to move volume id :{} size: {} from :{} to :{}",
          volumeIdWhichToMove, volumeSizeWhichToMove, theMaxSizeInstance, instanceIdToMove);

      //** 5. move in the from instance and put to instance ***/
      List<VolumesForEquilibrium> volumesForEquilibriumListRemove = theEndVolumeSizeInfo
          .get(theMaxSizeInstance);
      Iterator<VolumesForEquilibrium> iterator = volumesForEquilibriumListRemove.iterator();

      //remove in form instance and add to instance
      VolumesForEquilibrium volumesForEquilibriumAdd;
      while (iterator.hasNext()) {
        VolumesForEquilibrium volumesForEquilibriumTemp = iterator.next();
        if (volumesForEquilibriumTemp.getVolumeId() == volumeIdWhichToMove) {
          try {
            //remove
            volumesForEquilibriumAdd = (VolumesForEquilibrium) volumesForEquilibriumTemp.clone();
            iterator.remove();

            //add
            theEndVolumeSizeInfo.get(instanceIdToMove).add(volumesForEquilibriumAdd);
            break;
          } catch (CloneNotSupportedException e) {
            logger.error("when Equilibrium volume, clone object error");
          }

        }
      }

      logger.warn("get the end map :{}", theEndVolumeSizeInfo);
    }

    return;
  }

  //get the instance which to move the volume
  private VolumeInfoForEquilibrium getTheMaxSizeInstanceToMoved(
      MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo, long averageSize,
      Set<Long> instanceWhichNotNeedMove) {

    long theMaxSizeInstanceId = 0;
    long theMaxSize = 0;
    boolean findInstance = true;
    VolumeInfoForEquilibrium volumeInfoForRebalance = new VolumeInfoForEquilibrium();
    volumeInfoForRebalance.setResetAverageSize(false);
    long beginTime = System.currentTimeMillis();

    while (findInstance) {
      //just for debug
      long currentTime = System.currentTimeMillis();

      //there is only one instance, no need move
      if (theEndVolumeSizeInfo.size() - instanceWhichNotNeedMove.size() == 1) {
        break;
      }

      //get the total volume size is large instance
      for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : theEndVolumeSizeInfo.entrySet()) {
        long currentInstanceVolumeSize = 0;

        long instanceId = entry.getKey();
        List<VolumesForEquilibrium> volumeInfoTemp = entry.getValue();

        //not need
        if (instanceWhichNotNeedMove.contains(instanceId)) {
          continue;
        }

        //get total size
        for (VolumesForEquilibrium volumesForEquilibrium : volumeInfoTemp) {
          currentInstanceVolumeSize += volumesForEquilibrium.getVolumeSize();
        }

        if (currentInstanceVolumeSize > theMaxSize) {
          theMaxSize = currentInstanceVolumeSize;
          theMaxSizeInstanceId = instanceId;
        }
      }

      /*check value with averageSize, if the currentInstanceVolumeSize <= averageSize,
       *  mean not need to move volume
       */

      //not get
      if (theMaxSizeInstanceId == 0) {
        break;
      }

      //only one volume in this instance, no need to move
      if (theEndVolumeSizeInfo.get(theMaxSizeInstanceId).size() == 1) {
        instanceWhichNotNeedMove.add(theMaxSizeInstanceId);
        volumeInfoForRebalance.setResetAverageSize(true);
        logger.warn("the current instance :{} no need to move any volume", theMaxSizeInstanceId);
        break;
      }

      List<VolumesForEquilibrium> theMaxSizeInstanceMapTemp = theEndVolumeSizeInfo
          .get(theMaxSizeInstanceId);
      Pair<Long, Long> volumeToMoveTemp = sortTheMapAndReturnTheSmallVolume(
          theMaxSizeInstanceMapTemp);

      //check current instance can move volume or not

      if (theMaxSize - volumeToMoveTemp.getSecond() >= averageSize) {
        //get the volume for move
        volumeInfoForRebalance.setVolumeIdWhichToMove(volumeToMoveTemp.getFirst());
        volumeInfoForRebalance.setVolumeSizeWhichToMove(volumeToMoveTemp.getSecond());
        volumeInfoForRebalance.setTheMaxSizeInstance(theMaxSizeInstanceId);

        break;
      } else {
        instanceWhichNotNeedMove.add(theMaxSizeInstanceId);
        logger.warn("the current instance :{} no need to move any volume", theMaxSizeInstanceId);
        volumeInfoForRebalance.setResetAverageSize(true);
        break;
      }
    }

    return volumeInfoForRebalance;
  }

  //each move, must get the left size is largest instance to moved
  private Pair<Long, Long> getWhichInstanceToMove(
      MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo, long averageSize,
      Set<Long> instanceWhichNotNeedMove) {

    Pair<Long, Long> theSmallInstanceIdAndSize = new Pair<>();
    long theCurrentSmallSizeInstanceId = 0;
    long theMaxLeftSize = -10000000; //init
    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : theEndVolumeSizeInfo.entrySet()) {
      long currentInstanceVolumeSize = 0;
      long currentInstanceLeftSize = 0;
      long currentInstanceId = entry.getKey();
      if (instanceWhichNotNeedMove.contains(currentInstanceId)) {
        continue;
      }

      for (VolumesForEquilibrium volumesForEquilibrium : entry.getValue()) {
        currentInstanceVolumeSize += volumesForEquilibrium.getVolumeSize();
      }

      //get current instance left size,
      currentInstanceLeftSize = averageSize - currentInstanceVolumeSize;
      if (currentInstanceLeftSize >= theMaxLeftSize) {
        theMaxLeftSize = currentInstanceLeftSize;
        theCurrentSmallSizeInstanceId = currentInstanceId;
      }
    }
    //id ,      totalVolumeSizeCurrentInstance
    theSmallInstanceIdAndSize.setFirst(theCurrentSmallSizeInstanceId);
    // theSmallInstanceIdAndSize.setSecond((theMaxLeftSize + averageSize));
    theSmallInstanceIdAndSize.setSecond((averageSize - theMaxLeftSize));

    return theSmallInstanceIdAndSize;
  }

  //get the Average = totalSize/instanceSize
  private long getAverageSize(MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo,
      Set<Long> instanceWhichNotNeedMove) {
    long getAllVolumeSize = 0;
    long instanceSize = theEndVolumeSizeInfo.size() - instanceWhichNotNeedMove.size();

    if (instanceSize == 0 || instanceSize == 1) {
      logger.warn("there is no any HA instance to move, the size :{}", instanceSize);
      return 0;
    }

    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : theEndVolumeSizeInfo.entrySet()) {
      long instanceId = entry.getKey();

      //not calculate this instance
      if (instanceWhichNotNeedMove.contains(instanceId)) {
        continue;
      }
      for (VolumesForEquilibrium volumesForEquilibrium : entry.getValue()) {
        getAllVolumeSize += volumesForEquilibrium.getVolumeSize();
      }
    }

    if (getAllVolumeSize == 0) {
      logger.warn("there is no any volume in each HA instance");
      return 0;
    }

    long averageSize = getAllVolumeSize / instanceSize;
    return averageSize;
  }

  // volume id   volume size
  private Pair<Long, Long> sortTheMapAndReturnTheSmallVolume(
      List<VolumesForEquilibrium> theMaxSizeInstanceMapTemp) {
    //** 3. sort the max instance ***/

    Collections.sort(theMaxSizeInstanceMapTemp, new Comparator<VolumesForEquilibrium>() {
      @Override
      public int compare(VolumesForEquilibrium o1, VolumesForEquilibrium o2) {
        return (int) (o1.getVolumeSize() - o2.getVolumeSize());
      }
    });

    //* 3.1 get the move volume id which the size is smallest **/
    long volumeIdWhichToMove = 0;
    long volumeSizeWhichToMove = 0;
    int listSize = theMaxSizeInstanceMapTemp.size();

    for (int i = 0; i < listSize; i++) {
      //the first value is the small value, and volume is Available
      if (theMaxSizeInstanceMapTemp.get(i).isAvailable) {
        volumeIdWhichToMove = theMaxSizeInstanceMapTemp.get(i).getVolumeId();
        volumeSizeWhichToMove = theMaxSizeInstanceMapTemp.get(i).getVolumeSize();
        break;
      }
    }

    Pair<Long, Long> volumeToMove = new Pair<>();
    volumeToMove.setFirst(volumeIdWhichToMove);
    volumeToMove.setSecond(volumeSizeWhichToMove);
    return volumeToMove;
  }


  public void saveEquilibriumOkVolume(long volumeId, long instanceIdFrom) {
    if (equilibriumOkVolume.containsKey(volumeId)) {
      return;
    } else {
      equilibriumOkVolume.put(volumeId, instanceIdFrom);
    }
  }


  public void removeInstanceInEachTable(long instanceId) {
    //* move in volumeReportToInstanceEquilibriumBuildWithVolumeId ***/
    Iterator iteratorEquilibrium = volumeReportToInstanceEquilibriumBuildWithVolumeId.keySet()
        .iterator();
    while (iteratorEquilibrium.hasNext()) {
      Long volumeId = (Long) iteratorEquilibrium.next();
      Map<Long, Long> instances = volumeReportToInstanceEquilibriumBuildWithVolumeId.get(volumeId);
      if (instances != null && (instances.containsKey(instanceId) || instances
          .containsValue(instanceId))) {
        //find the down instance, so move it
        logger.warn(
            "begin to move the instance: {} in Equilibrium table, and the Equilibrium volume: {}",
            instanceId, volumeId);
        //remove in table
        iteratorEquilibrium.remove();
      }
    }

    //* move in volumeReportToInstanceEquilibrium ***/
    Iterator iterator = volumeReportToInstanceEquilibrium.keySet().iterator();
    while (iterator.hasNext()) {
      long instanceIdForm = (Long) iterator.next();
      if (instanceIdForm == instanceId) {
        logger.warn(
            "begin to move the instance: {} in Equilibrium table, and the Equilibrium volume: {}",
            instanceId, volumeReportToInstanceEquilibrium.get(instanceId));
        iterator.remove();
        continue;
      }

      Map<Long, Long> volumeInInstance = volumeReportToInstanceEquilibrium.get(instanceIdForm);
      if (volumeInInstance != null && volumeInInstance.containsValue(instanceId)) {
        //find the down instance, so move it
        logger.warn(
            "begin to move the instance: {} in Equilibrium table, and the Equilibrium volume "
                + "info: {}", instanceId, volumeInInstance);
        //remove in table
        iterator.remove();
      }
    }
  }

  /**
   * return the volume which in Equilibrium.
   */
  public List<Long> getVolumeWhichEquilibriumInCurrentInstance(long repotInstanceId,
      List<VolumeMetadata> reportVolumes) {
    //current instanceId, volumeId, this volume to report new InstanceId
    List<Long> reportVolumesList = new ArrayList<>();
    List<Long> returnEquilibriumReportVolumes = new ArrayList<>();

    for (VolumeMetadata volumeMetadata : reportVolumes) {
      reportVolumesList.add(volumeMetadata.getVolumeId());
    }

    if (reportVolumesList.isEmpty()) {
      return returnEquilibriumReportVolumes;
    }

    //get the not need report volume id which is Equilibrium
    for (Map.Entry<Long, Map<Long, Long>> entry : volumeReportToInstanceEquilibrium.entrySet()) {
      for (Map.Entry<Long, Long> entry1 : entry.getValue().entrySet()) {
        long volumeId = entry1.getKey();
        long newInstanceId = entry1.getValue();

        if (repotInstanceId == newInstanceId && reportVolumesList.contains(volumeId)) {
          returnEquilibriumReportVolumes.add(volumeId);
        }
      }
    }

    return returnEquilibriumReportVolumes;
  }



  public Map<Long, Map<Long, Long>> getVolumeReportToInstanceEquilibrium() {
    return volumeReportToInstanceEquilibrium;
  }

  public void setVolumeReportToInstanceEquilibrium(
      Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibrium) {
    this.volumeReportToInstanceEquilibrium = volumeReportToInstanceEquilibrium;
  }

  public Map<Long, Map<Long, Long>> getVolumeReportToInstanceEquilibriumBuildWithVolumeId() {
    return volumeReportToInstanceEquilibriumBuildWithVolumeId;
  }

  public void setVolumeReportToInstanceEquilibriumBuildWithVolumeId(
      Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibriumBuildWithVolumeId) {
    this.volumeReportToInstanceEquilibriumBuildWithVolumeId =
        volumeReportToInstanceEquilibriumBuildWithVolumeId;
  }

  public AtomicLong getUpdateReportToInstancesVersion() {
    return updateReportToInstancesVersion;
  }

  public void setUpdateReportToInstancesVersion(AtomicLong updateReportToInstancesVersion) {
    this.updateReportToInstancesVersion = updateReportToInstancesVersion;
  }

  public Map<Long, Map<Long, Long>> getUpdateTheDatanodeReportTable() {
    return updateTheDatanodeReportTable;
  }

  public void setUpdateTheDatanodeReportTable(
      Map<Long, Map<Long, Long>> updateTheDatanodeReportTable) {
    this.updateTheDatanodeReportTable = updateTheDatanodeReportTable;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public Map<Long, Long> getEquilibriumOkVolume() {
    return equilibriumOkVolume;
  }

  public long getCountEquilibriumNumber() {
    return countEquilibriumNumber;
  }

  public long getAllSegmentNumberToSave() {
    return allSegmentNumberToSave;
  }

  public void setAllSegmentNumberToSave(long allSegmentNumberToSave) {
    this.allSegmentNumberToSave = allSegmentNumberToSave;
  }

  public double getPercentNumberSegmentNumberMasterSave() {
    return percentNumberSegmentNumberMasterSave;
  }

  public void setPercentNumberSegmentNumberMasterSave(double percentNumberSegmentNumberMasterSave) {
    this.percentNumberSegmentNumberMasterSave = percentNumberSegmentNumberMasterSave;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  //for test
  public void setTheSegmentInfoToDefault() {
    allSegmentNumberToSave = 10000;
    percentNumberSegmentNumberMasterSave = 0.6;
  }

  public boolean isStartTest() {
    return startTest;
  }

  public void setStartTest(boolean startTest) {
    this.startTest = startTest;
  }

  @Override
  public String toString() {
    return "InstanceVolumeInEquilibriumManger{"

        + "instanceIncludeVolumeInfoManger=" + instanceIncludeVolumeInfoManger

        + ", volumeReportToInstanceEquilibriumBuildWithVolumeId="
        + volumeReportToInstanceEquilibriumBuildWithVolumeId

        + ", volumeReportToInstanceEquilibrium=" + volumeReportToInstanceEquilibrium

        + ", equilibriumOkVolume=" + equilibriumOkVolume

        + ", updateReportToInstancesVersion=" + updateReportToInstancesVersion

        + ", updateTheDatanodeReportTable=" + updateTheDatanodeReportTable

        + ", countEquilibriumNumber=" + countEquilibriumNumber

        + ", allSegmentNumberToSave=" + allSegmentNumberToSave

        + ", percentNumberSegmentNumberMasterSave=" + percentNumberSegmentNumberMasterSave

        + ", startTest=" + startTest

        + '}';
  }


  public class VolumeInfoForEquilibrium {

    long theMaxSizeInstance = 0;
    long volumeIdWhichToMove = 0;
    long volumeSizeWhichToMove = 0;
    boolean resetAverageSize = false;

    public VolumeInfoForEquilibrium() {
    }

    public long getTheMaxSizeInstance() {
      return theMaxSizeInstance;
    }

    public void setTheMaxSizeInstance(long theMaxSizeInstance) {
      this.theMaxSizeInstance = theMaxSizeInstance;
    }

    public long getVolumeIdWhichToMove() {
      return volumeIdWhichToMove;
    }

    public void setVolumeIdWhichToMove(long volumeIdWhichToMove) {
      this.volumeIdWhichToMove = volumeIdWhichToMove;
    }

    public long getVolumeSizeWhichToMove() {
      return volumeSizeWhichToMove;
    }

    public void setVolumeSizeWhichToMove(long volumeSizeWhichToMove) {
      this.volumeSizeWhichToMove = volumeSizeWhichToMove;
    }

    public boolean isResetAverageSize() {
      return resetAverageSize;
    }

    public void setResetAverageSize(boolean resetAverageSize) {
      this.resetAverageSize = resetAverageSize;
    }
  }
}
