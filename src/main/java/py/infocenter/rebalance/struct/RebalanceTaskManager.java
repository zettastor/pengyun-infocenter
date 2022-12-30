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
import com.google.common.collect.Multimaps;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.NamedThreadFactory;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.selector.VolumeRebalanceSelector;
import py.infocenter.rebalance.thread.RebalanceTaskCalcThread;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePool;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;

/**
 * the rebalance task, instance and archives backup, if instance and archives not changed, the
 * rebalance task not be changed.
 */
public class RebalanceTaskManager {

  private static final Logger logger = LoggerFactory.getLogger(SimulateInstanceBuilder.class);

  private final double rebalanceBrokenRatioSegmentAbnormal = 10;     //(0-100)if member ship
  // become abnormal when rebalance is doing, record it. if abnormal segment count is over this, 
  // broken rebalance
  private final long rebalanceWaitTimeAfterArchiveLost =
      30 * 1000;    //rebalance recalculate wait times when archive lost
  private final RebalanceConfiguration config = RebalanceConfiguration.getInstance();
  private ExecutorService rebalanceCalcExecutor;
  private ConcurrentHashMap<Long, ReentrantLock> volumeId2ReentrantLock = new ConcurrentHashMap<>();

  private ConcurrentHashMap<Long, RebalanceCalcStatus> volumeId2RebalanceCalcStatusMap =
      new ConcurrentHashMap<>();  //volume rebalance calculate thread
  private ConcurrentHashMap<Long, VolumeRebalanceStatus> volumeId2RebalanceStatusMap =
      new ConcurrentHashMap<>();  //volume rebalance status

  private ConcurrentHashMap<Long, SimulatePool> volumeId2SimulatePoolMap =
      new ConcurrentHashMap<>(); //pool information backup of volume at last rebalance
  private ConcurrentHashMap<Long, Long> volumeId2PoolEnvChgTimestampMap =
      new ConcurrentHashMap<>();  //volume pool environment changed times
  private ConcurrentHashMap<Long, Boolean> volumeId2ForceCalcTaskStepMap =
      new ConcurrentHashMap<>();  //force calculate rebalance task steps, whether volume pool 
  // environment is or not changed

  private ConcurrentHashMap<Long, List<InternalRebalanceTask>> volumeId2InternalRebalanceTaskMap
      = new ConcurrentHashMap<>();     //rebalance task backup, if instance and archives not 
  // changed, it not be changed
  private Multimap<Long, SendRebalanceTask> volumeId2SendTaskMap = Multimaps
      .synchronizedSetMultimap(HashMultimap.create());     //already send rebalance task backup
  private Multimap<Long, Integer> volumeId2AbnormalSegmentIndexMap = Multimaps
      .synchronizedSetMultimap(HashMultimap
          .create());     //if member ship become abnormal when rebalance is doing, record it.
  private ConcurrentHashMap<Long, Long> volumeId2StepCount = new ConcurrentHashMap<>();

  //just for test
  private ConcurrentHashMap<Long, SimulateVolume> volumeId2ResultOfRebalanceMap =
      new ConcurrentHashMap<>();        //simulate volume after rebalance

  public RebalanceTaskManager() {
    rebalanceCalcExecutor = new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), new NamedThreadFactory("rebalance-calculate-pool-"));
  }



  public void volumeLock(long volumeId) {
    ReentrantLock volumeLock = volumeId2ReentrantLock.get(volumeId);
    if (volumeLock == null) {
      synchronized (this) {
        volumeLock = volumeId2ReentrantLock.computeIfAbsent(volumeId, k -> new ReentrantLock());
      }
    }
    volumeLock.lock();
  }



  public void volumeUnlock(long volumeId) {
    ReentrantLock volumeLock = volumeId2ReentrantLock.get(volumeId);
    if (volumeLock == null) {
      return;
    }
    volumeLock.unlock();
  }

  //just for test
  public void setPoolEnvChgTimestamp(long volumeId, long timestamp) {
    this.volumeId2PoolEnvChgTimestampMap.put(volumeId, timestamp);
  }

  //just for test
  public SimulateVolume getResultOfRebalance(long volumeId) {
    return volumeId2ResultOfRebalanceMap.get(volumeId);
  }

  //just for test
  public void setResultOfRebalance(long volumeId, SimulateVolume resultSimulateVolume) {
    this.volumeId2ResultOfRebalanceMap.put(volumeId, resultSimulateVolume);
  }

  /**
   * update volume environment and clear all steps.
   *
   * @param volumeId    volume id
   * @param storagePool pool info
   */
  public void updateEnvironment(long volumeId, StoragePool storagePool) {
    logger.warn(
        "volume:{} in pool:{} environment will be updated and rebalance steps will be cleared",
        volumeId, storagePool.getPoolId());
    try {
      volumeLock(volumeId);

      //clear old tasks, old sending tasks and old volume environment
      volumeId2InternalRebalanceTaskMap.remove(volumeId);
      volumeId2SimulatePoolMap.remove(volumeId);
      volumeId2SendTaskMap.removeAll(volumeId);
      volumeId2ResultOfRebalanceMap.remove(volumeId);
      volumeId2StepCount.remove(volumeId);
      volumeId2PoolEnvChgTimestampMap.remove(volumeId);
      volumeId2AbnormalSegmentIndexMap.removeAll(volumeId);

      //update environment
      volumeId2SimulatePoolMap.put(volumeId, new SimulatePool(storagePool));

      //must after environment update
      setForceCalcTaskStep(volumeId, false);
    } finally {
      volumeUnlock(volumeId);
    }
  }


  public boolean setRebalanceCalcStatus(long volumeId, RebalanceCalcStatus status) {
    try {
      volumeLock(volumeId);
      RebalanceCalcStatus oldStatus = volumeId2RebalanceCalcStatusMap
          .computeIfAbsent(volumeId, val -> RebalanceCalcStatus.STABLE);

      if (status == RebalanceCalcStatus.STOP_CALCULATE) {
        //if oldStatus is not in CALCULATING, nothing to do, else stop calc
        if (oldStatus != RebalanceCalcStatus.CALCULATING) {
          return false;
        }
      }

      logger.warn("rebalanceCalcStatus: {} --> {}", oldStatus, status);
      volumeId2RebalanceCalcStatusMap.put(volumeId, status);
      return true;
    } finally {
      volumeUnlock(volumeId);
    }
  }



  public RebalanceCalcStatus getRebalanceCalcStatus(long volumeId) {
    RebalanceCalcStatus calcStatus = volumeId2RebalanceCalcStatusMap.get(volumeId);
    if (calcStatus == null) {
      return RebalanceCalcStatus.STABLE;
    }

    return calcStatus;
  }

  /**
   * when volume in CHECKING status, check combination is or not over threshold. if not, no need to
   * rebalance exception cause
   *
   * @param volumeId volume id
   * @return status
   */
  public VolumeRebalanceStatus getRebalanceStatus(long volumeId) {
    return volumeId2RebalanceStatusMap
        .computeIfAbsent(volumeId, value -> VolumeRebalanceStatus.CHECKING);
  }

  /**
   * set volume rebalance status.
   *
   * @param volumeId        volume id
   * @param rebalanceStatus rebalance status that will be set
   */
  public void setRebalanceStatus(long volumeId, VolumeRebalanceStatus rebalanceStatus) {
    logger.warn("volume:{} rebalance status changed: {} --> {}", volumeId,
        volumeId2RebalanceStatusMap.get(volumeId), rebalanceStatus);
    //logger
    if (rebalanceStatus == VolumeRebalanceStatus.BALANCED) {
      int abnormalSegmentCount = 0;
      Collection<Integer> abnormalSegment = volumeId2AbnormalSegmentIndexMap.get(volumeId);
      if (abnormalSegment != null) {
        abnormalSegmentCount = abnormalSegment.size();
      }

      logger.warn("volume:{} rebalance over! abnormal segment count:{}", volumeId,
          abnormalSegmentCount);
    }
    volumeId2RebalanceStatusMap.put(volumeId, rebalanceStatus);
  }

  /**
   * get sending tasks of volume.
   *
   * @param volumeId volume id
   * @return sending tasks
   */
  private List<SendRebalanceTask> getSendingTasks(long volumeId) {
    return new LinkedList<>(volumeId2SendTaskMap.get(volumeId));
  }

  /**
   * get volumes which has rebalance task.
   *
   * @return volume list
   */
  public Set<Long> getHasTaskVolumes() {
    return volumeId2InternalRebalanceTaskMap.keySet();
  }

  /**
   * set the volume is or not force calculate rebalance task step, whether pool environment is or
   * not changed.
   *
   * @param volumeId     volume id
   * @param isPoolEnvChg if want force calculate, set it true; else set it false
   */
  public void setForceCalcTaskStep(long volumeId, boolean isPoolEnvChg) {
    logger.warn("setForceCalcTaskStep, volumeId:{}, isPoolEnvChg:{}", volumeId, isPoolEnvChg);
    volumeId2ForceCalcTaskStepMap.put(volumeId, isPoolEnvChg);
  }

  public boolean isForceCalcTaskStep(long volumeId) {
    return volumeId2ForceCalcTaskStepMap.computeIfAbsent(volumeId, v -> false);
  }

  public long getRebalanceStepCount(long volumeId) {
    if (!volumeId2StepCount.containsKey(volumeId)) {
      return 0;
    }
    return volumeId2StepCount.get(volumeId);
  }

  public void setRebalanceStepCount(long volumeId, long stepCount) {
    volumeId2StepCount.put(volumeId, stepCount);
  }

  /**
   * get rebalance ratio information.
   *
   * @param volumeId volumeId
   * @return volume rebalance information
   */
  public VolumeRebalanceInfo getRebalanceRatioInfo(long volumeId) {
    long totalStepCount = getRebalanceStepCount(volumeId);
    long remainStepCount = 0;

    VolumeRebalanceInfo rebalanceInfo = new VolumeRebalanceInfo();
    try {
      volumeLock(volumeId);

      if (getRebalanceCalcStatus(volumeId) != RebalanceCalcStatus.STABLE) {
        //means rebalance step is calculating
        remainStepCount = totalStepCount;
        rebalanceInfo.setRebalanceCalculating(true);
      } else {
        Collection<InternalRebalanceTask> internalRebalanceTasks = volumeId2InternalRebalanceTaskMap
            .get(volumeId);
        if (internalRebalanceTasks != null && !internalRebalanceTasks.isEmpty()) {
          remainStepCount += internalRebalanceTasks.size();
        }

        Collection<SendRebalanceTask> sendingTasks = volumeId2SendTaskMap.get(volumeId);
        if (sendingTasks != null && !sendingTasks.isEmpty()) {
          remainStepCount += sendingTasks.size();
        }

        rebalanceInfo.setRebalanceCalculating(false);
      }

      rebalanceInfo.setRebalanceRemainTaskCount(remainStepCount);
      rebalanceInfo.setRebalanceTotalTaskCount(totalStepCount);
      rebalanceInfo.calcRatio();
    } finally {
      volumeUnlock(volumeId);
    }

    //logger
    if ((long) rebalanceInfo.getRebalanceRatio() != 1) {
      int abnormalSegmentCount = 0;
      Collection<Integer> abnormalSegment = volumeId2AbnormalSegmentIndexMap.get(volumeId);
      if (abnormalSegment != null) {
        abnormalSegmentCount = abnormalSegment.size();
      }
      logger.warn("volume:{} get rebalance ratio: {} abnormal segment count:{}", volumeId,
          rebalanceInfo, abnormalSegmentCount);
    }

    return rebalanceInfo;
  }

  /**
   * is volume has task not over.
   *
   * @param volumeId volume id
   * @return if has task not over, return true
   */
  public boolean isVolumeHasTask(long volumeId) {
    if (getRebalanceCalcStatus(volumeId) != RebalanceCalcStatus.STABLE) {
      return true;
    }

    Collection<InternalRebalanceTask> internalRebalanceTasks = volumeId2InternalRebalanceTaskMap
        .get(volumeId);
    if (internalRebalanceTasks != null && !internalRebalanceTasks.isEmpty()) {
      return true;
    }
    Collection<SendRebalanceTask> sendingTasks = getSendingTasks(volumeId);
    if (!sendingTasks.isEmpty()) {
      return true;
    } else {
      for (SendRebalanceTask task : sendingTasks) {
        //if have task not expired and now is sending
        if (!task.expired()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * does any volume has rebalance tasks in task list or send task list.
   *
   * @return if has, return true
   */
  public boolean hasAnyTasks() {
    for (long volumeId : volumeId2InternalRebalanceTaskMap.keySet()) {
      if (hasInternalTasks(volumeId)) {
        return true;
      }
    }

    for (long volumeId : volumeId2SendTaskMap.keySet()) {
      if (hasSendingTasks(volumeId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * does volume has not send rebalance tasks.
   *
   * @return if has, return true
   */
  private boolean hasInternalTasks(long volumeId) {
    try {
      volumeLock(volumeId);

      if (getRebalanceCalcStatus(volumeId) != RebalanceCalcStatus.STABLE) {
        return true;
      }

      Collection<InternalRebalanceTask> internalRebalanceTasks = volumeId2InternalRebalanceTaskMap
          .get(volumeId);
      return (internalRebalanceTasks != null && !internalRebalanceTasks.isEmpty());
    } finally {
      volumeUnlock(volumeId);
    }
  }

  /**
   * does volume has sending tasks.
   *
   * @return if has, return true
   */
  private boolean hasSendingTasks(long volumeId) {
    try {
      volumeLock(volumeId);

      if (getRebalanceCalcStatus(volumeId) != RebalanceCalcStatus.STABLE) {
        return true;
      }

      Collection<SendRebalanceTask> sendingTasks = volumeId2SendTaskMap.get(volumeId);
      return (sendingTasks != null && !sendingTasks.isEmpty());
    } finally {
      volumeUnlock(volumeId);
    }
  }

  /**
   * update rebalance Steps, include volume environment and tasks.
   *
   * @param volumeId         volume id
   * @param internalTaskList task list
   */
  public void addRebalanceTasks(long volumeId, List<InternalRebalanceTask> internalTaskList) {
    try {
      volumeLock(volumeId);

      //save new tasks and new volume environment
      List<InternalRebalanceTask> oldInternalTaskList = volumeId2InternalRebalanceTaskMap
          .computeIfAbsent(volumeId,
              value -> new LinkedList<>());
      oldInternalTaskList.addAll(internalTaskList);
    } finally {
      volumeUnlock(volumeId);
    }
  }

  /**
   * remove all information of volume.
   *
   * @param volumeId volumeId
   */
  public void clearVolume(long volumeId) {
    try {
      volumeLock(volumeId);

      volumeId2RebalanceCalcStatusMap.remove(volumeId);
      volumeId2RebalanceStatusMap.remove(volumeId);
      volumeId2SimulatePoolMap.remove(volumeId);
      volumeId2InternalRebalanceTaskMap.remove(volumeId);
      volumeId2SendTaskMap.removeAll(volumeId);
      volumeId2ResultOfRebalanceMap.remove(volumeId);
      volumeId2StepCount.remove(volumeId);
      volumeId2PoolEnvChgTimestampMap.remove(volumeId);
      volumeId2ForceCalcTaskStepMap.remove(volumeId);
    } finally {
      volumeUnlock(volumeId);
    }
    volumeId2ReentrantLock.remove(volumeId);
  }

  /**
   * remove task from task list.
   *
   * @param volumeId    volume id
   * @param removeTasks remove tasks
   */
  public void removePartOfInternalTasks(long volumeId,
      Collection<InternalRebalanceTask> removeTasks) {
    if (removeTasks == null || removeTasks.isEmpty()) {
      return;
    }

    try {
      volumeLock(volumeId);

      List<InternalRebalanceTask> allTaskList = volumeId2InternalRebalanceTaskMap.get(volumeId);
      if (allTaskList == null || allTaskList.isEmpty()) {
        return;
      }

      logger.warn("remove ({}) internal task from internal task list", removeTasks.size());
      allTaskList.removeAll(removeTasks);
      volumeId2InternalRebalanceTaskMap.put(volumeId, allTaskList);
    } finally {
      volumeUnlock(volumeId);
    }
  }

  /**
   * remove task from task list.
   *
   * @param volumeId   volume id
   * @param removeTask remove tasks
   */
  public void removeSendTasks(long volumeId, SendRebalanceTask removeTask) {
    if (removeTask == null) {
      return;
    }
    volumeId2SendTaskMap.remove(volumeId, removeTask);
  }

  /**
   * save sending tasks of volume.
   *
   * @param volumeId             volume id
   * @param sendInternalTaskList sending tasks(internal)
   * @param sendRebalanceTask    sending tasks(rebalance task)
   */
  public void saveSendTask(long volumeId, List<InternalRebalanceTask> sendInternalTaskList,
      List<SendRebalanceTask> sendRebalanceTask) {
    if (sendInternalTaskList == null || sendInternalTaskList.isEmpty()) {
      return;
    }

    try {
      volumeLock(volumeId);

      //save it to sending task list
      volumeId2SendTaskMap.putAll(volumeId, sendRebalanceTask);

      //remove it from need rebalance task list
      List<InternalRebalanceTask> internalRebalanceTasks = volumeId2InternalRebalanceTaskMap
          .get(volumeId);
      internalRebalanceTasks.removeAll(sendInternalTaskList);

      logger
          .warn("volume:{} send ({}) task steps to datanode now, still have ({}) rebalance steps.",
              volumeId, sendInternalTaskList.size(),
              volumeId2InternalRebalanceTaskMap.get(volumeId).size());
    } finally {
      volumeUnlock(volumeId);
    }
  }

  /**
   * add a abnormal segment index.
   *
   * @param volumeId volume id
   * @param segIndex segment index
   */
  private void addAbnormalSegmentIndex(long volumeId, int segIndex) {
    volumeId2AbnormalSegmentIndexMap.put(volumeId, segIndex);
  }

  /**
   * calculate and judge is illegal task is overmuch.
   *
   * @param volumeId           volumeId
   * @param volumeSegmentCount volume segment count
   * @return if overmuch, return true
   */
  public boolean isIllegalTaskOvermuch(long volumeId, long volumeSegmentCount) {
    long illegalTaskCount = volumeId2AbnormalSegmentIndexMap.get(volumeId).size();
    if ((double) illegalTaskCount / volumeSegmentCount * 100
        > rebalanceBrokenRatioSegmentAbnormal) {
      logger.warn(
          "illegal rebalance task is overmuch threshold. illegal task count:{}, total count:{}, "
              + "threshold:{}",
          illegalTaskCount, volumeSegmentCount, rebalanceBrokenRatioSegmentAbnormal);
      return true;
    }
    return false;
  }


  public List<SendRebalanceTask> selectNoDependTask(VolumeMetadata volumeMetadata,
      InstanceId instanceId,
      boolean isNeedSendNewTask) {
    long volumeId = volumeMetadata.getVolumeId();
    /*
     * select no depend task(contains last send tasks and new internal tasks)
     */
    List<SendRebalanceTask> lastSendTaskList = new LinkedList<>();
    List<InternalRebalanceTask> internalTaskList = selectNoDependTask(volumeMetadata, instanceId,
        isNeedSendNewTask, lastSendTaskList);
    logger.debug("got internal tasks of instance:{} tasks:{}", instanceId, internalTaskList);

    /*
     * change internal task to send task, and filtrate illegal task then remove them
     */
    List<SendRebalanceTask> newSendTaskList = chgInternalTask2RebalanceTask(volumeMetadata,
        internalTaskList);
    logger.debug("change internal tasks to send tasks of instance:{} tasks:{}", instanceId,
        internalTaskList);

    /*
     * if illegal task of segment is more than 10%, delete all internal task and wait for sending
     * task run over
     */
    if (isIllegalTaskOvermuch(volumeId, volumeMetadata.getSegmentCount())) {
      logger.warn(
          "illegal rebalance task is overmuch threshold. Now give up all unSend task. give up "
              + "count:{}",
          volumeId2InternalRebalanceTaskMap.get(volumeId).size());
      volumeId2InternalRebalanceTaskMap.remove(volumeId);
      return lastSendTaskList;
    }

    /*
     * save send task steps this time
     */
    lastSendTaskList.addAll(newSendTaskList);
    saveSendTask(volumeId, internalTaskList, lastSendTaskList);
    return lastSendTaskList;
  }


  public List<InternalRebalanceTask> selectNoDependTask(VolumeMetadata volumeMetadata,
      InstanceId instanceId,
      boolean isNeedSendNewTask, List<SendRebalanceTask> lastSendTaskList) {
    List<InternalRebalanceTask> selectTaskList = new LinkedList<>();
    long volumeId = volumeMetadata.getVolumeId();
    try {
      volumeLock(volumeId);

      Multimap<Integer, Long> segIndex2MigrateIdMap = HashMultimap
          .create();  //select migrate information
      /*
       * get task from last sending list
       */
      Collection<SendRebalanceTask> sendingTasks = getSendingTasks(volumeId);
      logger.debug("got all sending tasks of volume:{} tasks:{}", volumeId, sendingTasks);
      for (SendRebalanceTask task : sendingTasks) {
        SimulateSegmentUnit srcSegmentUnit = task.getSourceSegmentUnit();

        InstanceId srcId = srcSegmentUnit.getInstanceId();
        InstanceId destId = task.getDestInstanceId();
        SegId segId = srcSegmentUnit.getSegId();

        Set<Long> allMigrateIdSet = new HashSet<>(segIndex2MigrateIdMap.get(segId.getIndex()));
        //if src instance id or dest instance id is exist in task before
        if (allMigrateIdSet.contains(srcId.getId()) || allMigrateIdSet.contains(destId.getId())) {

          segIndex2MigrateIdMap.put(segId.getIndex(), srcId.getId());
          segIndex2MigrateIdMap.put(segId.getIndex(), destId.getId());
          continue;
        }

        segIndex2MigrateIdMap.put(segId.getIndex(), srcId.getId());
        segIndex2MigrateIdMap.put(segId.getIndex(), destId.getId());

        SendRebalanceTask.RebalanceTaskType taskType = task.getTaskType();
        if (taskType == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {
          //not current instance
          if (srcId.getId() != instanceId.getId()) {
            continue;
          }
        } else if (taskType == BaseRebalanceTask.RebalanceTaskType.PSRebalance
            || taskType == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance) {
          if (destId.getId() != instanceId.getId()) {
            continue;
          }
        } else {
          continue;
        }

        lastSendTaskList.add(task);
      }

      if (lastSendTaskList.size() >= config.getMaxRebalanceTaskCountPerVolumeOfDatanode()) {
        logger.warn(
            "volume:{} already have sending tasks about instance:{} now, cannot send new tasks "
                + "now.",
            volumeId, instanceId);
        logger.debug("we cannot send new tasks now. sending tasks:{}", lastSendTaskList);
        return selectTaskList;
      }

      //when environment is changed, will not send new task
      if (!isNeedSendNewTask) {
        logger.warn("environment has changed, cannot do rebalance now. volume:{} instance:{}",
            volumeId, instanceId);
        return selectTaskList;
      }

      int remainTaskSize =
          config.getMaxRebalanceTaskCountPerVolumeOfDatanode() - lastSendTaskList.size();

      /*
       * get task from task step list
       */
      Collection<InternalRebalanceTask> allTasks = volumeId2InternalRebalanceTaskMap.get(volumeId);
      if (allTasks == null) {
        logger.warn("cannot get any tasks. volume:{} instance:{}", volumeId, instanceId);
        return selectTaskList;
      }
      for (InternalRebalanceTask task : allTasks) {
        InstanceId srcId = task.getSrcInstanceId();
        InstanceId destId = task.getDestInstanceId();
        SegId segId = task.getSegId();

        Set<Long> allMigrateIdSet = new HashSet<>(segIndex2MigrateIdMap.get(segId.getIndex()));
        //if src instance id or dest instance id is exist in task before
        if (allMigrateIdSet.contains(srcId.getId()) || allMigrateIdSet.contains(destId.getId())) {

          segIndex2MigrateIdMap.put(segId.getIndex(), srcId.getId());
          segIndex2MigrateIdMap.put(segId.getIndex(), destId.getId());
          continue;
        }

        segIndex2MigrateIdMap.put(segId.getIndex(), srcId.getId());
        segIndex2MigrateIdMap.put(segId.getIndex(), destId.getId());

        SendRebalanceTask.RebalanceTaskType taskType = task.getTaskType();
        if (taskType == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {
          if (srcId.getId() != instanceId.getId()) {
            continue;
          }
        } else if (taskType == BaseRebalanceTask.RebalanceTaskType.PSRebalance
            || taskType == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance) {
          if (destId.getId() != instanceId.getId()) {
            continue;
          }
        } else {
          continue;
        }

        selectTaskList.add(task);

        if (selectTaskList.size() >= remainTaskSize) {
          break;
        }
      }
    } finally {
      volumeUnlock(volumeId);
    }

    return selectTaskList;
  }

  /**
   * change internal task to rebalance task if task if illegal, remove them from internal task
   * list.
   *
   * @param volumeMetadata volume info
   * @param srcTaskList    source task list
   * @return rebalance task which can send
   */
  public List<SendRebalanceTask> chgInternalTask2RebalanceTask(VolumeMetadata volumeMetadata,
      Collection<InternalRebalanceTask> srcTaskList) {
    List<InternalRebalanceTask> illegalInternalTaskList = new LinkedList<>();
    List<SendRebalanceTask> taskList = new LinkedList<>();
    for (InternalRebalanceTask task : srcTaskList) {
      InstanceId srcId = task.getSrcInstanceId();
      InstanceId destId = task.getDestInstanceId();
      SegId segId = task.getSegId();

      SegmentMetadata realSegment = volumeMetadata.getSegmentByIndex(segId.getIndex());
      SegmentMembership realMembership = realSegment.getLatestMembership();
      if (task.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {
        if (!realMembership.isPrimary(srcId)) {
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          logger.warn(
              "primary migration: source is not primary, task will be removed from internal task "
                  + "list! seg index:{}, {} ---> {}. mark abnormal segment",
              task.getSegId(), srcId, destId);
        } else if (!realMembership.isSecondary(destId)) {
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          logger.warn(
              "primary migration: source is not secondary, task will be removed from internal "
                  + "task list! seg index:{}, {} ---> {}. mark abnormal segment",
              task.getSegId(), srcId, destId);
        }
      } else if (task.getTaskType() == BaseRebalanceTask.RebalanceTaskType.PSRebalance) {
        if (!realMembership.isSecondary(srcId)) {
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          logger.warn(
              "secondary migration: source is not secondary, task will be removed from internal "
                  + "task list! , seg index:{}, {} ---> {}. mark abnormal segment",
              task.getSegId(), srcId, destId);
        } else if (realMembership.contain(destId)) {
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          logger.warn(
              "secondary migration: source is not empty, task will be removed from internal task "
                  + "list! , seg index:{}, {} ---> {}. mark abnormal segment",
              task.getSegId(), srcId, destId);
        }
      } else if (task.getTaskType() == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance) {
        if (!realMembership.isArbiter(srcId)) {
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          logger.warn(
              "arbiter migration: source is not arbiter, task will be removed from internal task "
                  + "list! , seg index:{}, {} ---> {}. mark abnormal segment",
              task.getSegId(), srcId, destId);
        } else if (realMembership.contain(destId)) {
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          logger.warn(
              "arbiter migration: source is not empty, task will be removed from internal task "
                  + "list! , seg index:{}, {} ---> {}. mark abnormal segment",
              task.getSegId(), srcId, destId);
        }
      } else {
        task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
        logger.warn(
            "task has not set rebalance task type, task will be removed from internal task list! "
                + ", seg index:{}, {} ---> {}. mark abnormal segment",
            task.getSegId(), srcId, destId);
      }

      SegmentUnitMetadata realSrcSegmentUnit = realSegment.getSegmentUnitMetadata(srcId);
      if (realSrcSegmentUnit == null) {
        task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
        logger.warn(
            "segment:{} in volume:{} have not segment unit on instance:{}, task will be removed "
                + "from internal task list. mark abnormal segment",
            task.getSegId(), segId.getVolumeId(), srcId);
      }

      if (task.getTaskStatus() == BaseRebalanceTask.TaskStatus.ILLEGAL) {
        illegalInternalTaskList.add(task);
        //record illegal task
        addAbnormalSegmentIndex(volumeMetadata.getVolumeId(), task.getSegId().getIndex());
      } else {
        SimulateSegmentUnit simulateSegmentUnit = new SimulateSegmentUnit(realSrcSegmentUnit);
        SendRebalanceTask rebalanceTask = new SendRebalanceTask(task.getTaskId(),
            simulateSegmentUnit,
            destId, config.getRebalanceTaskExpireTimeSeconds(), task.getTaskType());

        if (task.isBornTimeSet()) {
          rebalanceTask.setBornTime(task.getBornTime());
        }
        taskList.add(rebalanceTask);
      }
    }

    //remove illegal task
    if (illegalInternalTaskList.size() > 0) {
      logger.warn(
          "volume:{} found ({}) task abnormal when change internal task to send task, now remove "
              + "it from internal list",
          volumeMetadata.getVolumeId(), illegalInternalTaskList.size());
      removePartOfInternalTasks(volumeMetadata.getVolumeId(), illegalInternalTaskList);
    }

    return taskList;
  }

  /**
   * remove time out tasks from send list time out task is not removed until environment is
   * changed.
   *
   * @param volumeMetadata volume metadata
   */
  private void removeTimeoutTaskFromSendList(VolumeMetadata volumeMetadata) {
    try {
      volumeLock(volumeMetadata.getVolumeId());

      List<SendRebalanceTask> overTasks = getTimeoutTasks(volumeMetadata);

      //remove run over tasks
      for (SendRebalanceTask overTask : overTasks) {
        volumeId2SendTaskMap.remove(volumeMetadata.getVolumeId(), overTask);
      }
    } finally {
      volumeUnlock(volumeMetadata.getVolumeId());
    }
  }

  /**
   * update sending task list according to new volume metadata if task is run over, timeout or
   * task's datanode not exist, remove task from task list.
   *
   * @param volumeMetadata          new volume metadata
   * @param simulateInstanceBuilder simulate instance builder object
   * @param isRemoveFailedTask      is remove failed task from send task list (when volume
   *                                environment changed, we will remove it, then recalculate tasks)
   */
  public void removeRunOverTaskFromSendList(VolumeMetadata volumeMetadata,
      SimulateInstanceBuilder simulateInstanceBuilder, boolean isRemoveFailedTask) {
    try {
      volumeLock(volumeMetadata.getVolumeId());

      List<SendRebalanceTask> overTasks = getRunOverTasks(volumeMetadata, simulateInstanceBuilder,
          isRemoveFailedTask);

      //remove run over tasks
      for (SendRebalanceTask overTask : overTasks) {
        volumeId2SendTaskMap.remove(volumeMetadata.getVolumeId(), overTask);
        if (overTask.getTaskStatus() == BaseRebalanceTask.TaskStatus.ILLEGAL) {
          addAbnormalSegmentIndex(overTask.getSegId().getVolumeId().getId(),
              overTask.getSegId().getIndex());
        }
      }
    } finally {
      volumeUnlock(volumeMetadata.getVolumeId());
    }
  }

  /**
   * get run over tasks from sending task list according to volume metadata if task is run over or
   * task's datanode not exist, we think task run over.
   *
   * @param volumeMetadata          volume metadata
   * @param simulateInstanceBuilder simulation instance builder
   * @param isRemoveFailedTask      is remove failed task from send task list
   * @return run over tasks
   */
  private List<SendRebalanceTask> getRunOverTasks(VolumeMetadata volumeMetadata,
      SimulateInstanceBuilder simulateInstanceBuilder, boolean isRemoveFailedTask) {
    List<SendRebalanceTask> overTasks = new LinkedList<>();

    //get sending tasks
    long volumeId = volumeMetadata.getVolumeId();
    Collection<SendRebalanceTask> sendTasks = getSendingTasks(volumeId);
    if (sendTasks.isEmpty()) {
      return overTasks;
    }

    //get all instance
    Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap = simulateInstanceBuilder
        .getInstanceId2SimulateInstanceMap();

    Map<Integer, SegmentMetadata> segId2SegmentMap = volumeMetadata.getSegmentTable();

    for (SendRebalanceTask task : sendTasks) {
      SimulateSegmentUnit oldSrcSegmentUnit = task.getSourceSegmentUnit();
      InstanceId oldDestInstanceId = task.getDestInstanceId();

      //if source instance is not exist in current pool, it means task will never over
      if (!instanceId2SimulateInstanceMap.containsKey(oldSrcSegmentUnit.getInstanceId().getId())) {
        logger.warn(
            "instance:{} not exists now! take this task over. mark abnormal segment and remove it"
                + " from send list",
            oldSrcSegmentUnit.getInstanceId().getId());
        task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
        overTasks.add(task);
        continue;
      }

      //if dest instance is not exist in current pool, it means task will never over
      if (!instanceId2SimulateInstanceMap.containsKey(oldDestInstanceId.getId())) {
        logger.warn(
            "instance:{} not exists now! take this task over. mark abnormal segment and remove it"
                + " from send list",
            oldDestInstanceId.getId());
        task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
        overTasks.add(task);
        continue;
      }

      //if task has bean migrate, it means task run over
      BaseRebalanceTask.RebalanceTaskType taskType = task.getTaskType();
      SegId segId = oldSrcSegmentUnit.getSegId();
      SegmentMetadata segmentMetadata = segId2SegmentMap.get(segId.getIndex());

      SegmentMembership membership = segmentMetadata.getLatestMembership();
      if (taskType == BaseRebalanceTask.RebalanceTaskType.PrimaryRebalance) {

        if (membership.getPrimary().equals(oldDestInstanceId)) {
          int primaryCount = 0;
          int otherUnstableCount = 0;
          for (SegmentUnitMetadata segmentUnitMetadata : segmentMetadata.getSegmentUnits()) {
            if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Primary) {
              primaryCount++;
            } else if (segmentUnitMetadata.getStatus() != SegmentUnitStatus.Secondary
                && segmentUnitMetadata.getStatus() != SegmentUnitStatus.Arbiter) {
              otherUnstableCount++;
            }
          }

          if (primaryCount == 1 && otherUnstableCount == 0) {
            if (segmentMetadata.getSegmentUnitMetadata(oldDestInstanceId).getStatus()
                == SegmentUnitStatus.Primary) {
              // only when the destination is already primary, will we go to next phase
              overTasks.add(task);
            }
          }
        } else if (membership.getPrimary().equals(oldSrcSegmentUnit.getInstanceId())) {

          if (!membership.isPrimaryCandidate(oldDestInstanceId) && task.becomeCandidateExpired()) {

            if (isRemoveFailedTask) {
              logger.warn(
                  "task:(type:{} {} --> {} on seg:{}) become candidate timeout. mark abnormal "
                      + "segment and remove it from send list",
                  task.getTaskType(),
                  task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
                  task.getSourceSegmentUnit().getSegId());
              task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
              overTasks.add(task);
            } else {
              logger.warn(
                  "task:(type:{} {} --> {} on seg:{}) become candidate timeout, but cannot be "
                      + "removed",
                  task.getTaskType(),
                  task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
                  task.getSourceSegmentUnit().getSegId());
            }
          }
        } else {
          logger.warn(
              "primary is not source instance or dest instance in membership, task:(type:{} {} "
                  + "--> {} on seg:{}) will be removed. mark abnormal segment",
              task.getTaskType(),
              task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
              task.getSourceSegmentUnit().getSegId());
          task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
          overTasks.add(task);
        }
      } else if (taskType == BaseRebalanceTask.RebalanceTaskType.PSRebalance) {

        int secondaryCount = 0;
        int arbiterCount = 0;
        List<SegmentUnitMetadata> latestSegmentUnits = segmentMetadata.getSegmentUnits();
        for (SegmentUnitMetadata latestSegmentUnit : latestSegmentUnits) {
          if (latestSegmentUnit.getStatus() == SegmentUnitStatus.Secondary) {
            secondaryCount++;
          } else if (latestSegmentUnit.getStatus() == SegmentUnitStatus.Arbiter) {
            arbiterCount++;
          }
        }

        if (secondaryCount + arbiterCount == volumeMetadata.getVolumeType().getNumMembers() - 1) {
          // segment is stable
          if (/*!membership.contain(oldSrcSegmentUnit.getInstanceId()) &&*/
              membership.isSecondary(oldDestInstanceId)) {
            if (membership.contain(oldSrcSegmentUnit.getInstanceId())) {
              logger.warn(
                  "task:(type:{} {} --> {} on seg:{}) migrate have some exception. mark abnormal "
                      + "segment and remove it from send list",
                  task.getTaskType(),
                  task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
                  task.getSourceSegmentUnit().getSegId());
              task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
            }
            overTasks.add(task);
          } else if (membership.isPrimary(oldDestInstanceId)) {
            logger.warn(
                "task:(type:{} {} --> {} on seg:{}) migrate destination is primary now, remove "
                    + "this task. mark abnormal segment and remove it from send list",
                task.getTaskType(),
                task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
                task.getSourceSegmentUnit().getSegId());
            task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
            overTasks.add(task);
          } else if (membership.getSecondaryCandidate() == null && task.becomeCandidateExpired()) {

            if (isRemoveFailedTask) {
              logger.warn(
                  "task:(type:{} {} --> {} on seg:{}) become candidate timeout, mark abnormal "
                      + "segment and and remove it from send list",
                  task.getTaskType(),
                  task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
                  task.getSourceSegmentUnit().getSegId());
              task.setTaskStatus(BaseRebalanceTask.TaskStatus.ILLEGAL);
              overTasks.add(task);
            } else {
              logger.warn(
                  "task:(type:{} {} --> {} on seg:{}) become candidate timeout, but cannot be "
                      + "removed",
                  task.getTaskType(),
                  task.getSourceSegmentUnit().getInstanceId(), task.getDestInstanceId(),
                  task.getSourceSegmentUnit().getSegId());
            }
          }
        } else {


          logger
              .debug("membership is abnormal! secondary count:{} arbiter count:{}", secondaryCount,
                  arbiterCount);
        }
      } else if (taskType == BaseRebalanceTask.RebalanceTaskType.ArbiterRebalance
          && !membership.isArbiter(oldSrcSegmentUnit.getInstanceId())) {
        //TODO
        overTasks.add(task);
      }
    }

    return overTasks;
  }

  /**
   * get timeout tasks from sending task list according to volume metadata.
   *
   * @param volumeMetadata volume metadata
   * @return time out task list
   */
  private List<SendRebalanceTask> getTimeoutTasks(VolumeMetadata volumeMetadata) {
    List<SendRebalanceTask> overTasks = new LinkedList<>();

    Collection<SendRebalanceTask> sendTasks = getSendingTasks(volumeMetadata.getVolumeId());
    if (sendTasks.isEmpty()) {
      return overTasks;
    }

    for (SendRebalanceTask task : sendTasks) {
      //if timeout, it means task will never over
      if (task.expired()) {
        overTasks.add(task);
      }
    }

    return overTasks;
  }

  /**
   * if sending task is run over or time out, remove it, then judge has any sending task in sending
   * list.
   *
   * @param volumeMetadata          volume
   * @param simulateInstanceBuilder simulate instance info
   * @return if all send task run over or time out, return true
   */
  private boolean isSendTaskOver(VolumeMetadata volumeMetadata,
      SimulateInstanceBuilder simulateInstanceBuilder) {
    //remove task if run over
    removeRunOverTaskFromSendList(volumeMetadata, simulateInstanceBuilder, true);

    //remove task if timeout
    removeTimeoutTaskFromSendList(volumeMetadata);

    return !hasSendingTasks(volumeMetadata.getVolumeId());
  }

  public SimulatePool.PoolChangedStatus isPoolEnvChanged(StoragePool storagePool,
      VolumeMetadata volumeMetadata) {
    long volumeId = volumeMetadata.getVolumeId();
    try {
      volumeLock(volumeId);


      SimulatePool lastSimulatePool = volumeId2SimulatePoolMap.get(volumeId);
      if (lastSimulatePool == null) {
        logger.debug("last simulate pool is null, volume:{}", volumeId);
        return SimulatePool.PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME;
      }


      if (isForceCalcTaskStep(volumeId)) {
        logger.debug("pool changed during rebalance disabled, volume:{}", volumeId);
        return SimulatePool.PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME;
      }


      return lastSimulatePool.isPoolEnvChanged(storagePool, volumeMetadata);
    } finally {
      volumeUnlock(volumeId);
    }
  }

  /**
   * calculate rebalance tasks if volume pool environment changed, else not do anything.
   *
   * @param volumeStore             volume store DB
   * @param volumeMetadata          volume
   * @param storagePool             pool
   * @param simulateInstanceBuilder simulate instance info
   * @param selector                rebalance task selector
   * @return if need to send new task to datanode, return true; else if need resend not run over
   *          task to datanode, return false
   */
  public boolean calcRebalanceTasks(VolumeStore volumeStore, VolumeMetadata volumeMetadata,
      StoragePool storagePool,
      SimulateInstanceBuilder simulateInstanceBuilder, VolumeRebalanceSelector selector)
      throws NoNeedToRebalance {
    long volumeId = volumeMetadata.getVolumeId();

    try {
      volumeLock(volumeId);

      SimulatePool.PoolChangedStatus poolChangedStatus = isPoolEnvChanged(storagePool,
          volumeMetadata);
      ;


      if (poolChangedStatus == SimulatePool.PoolChangedStatus.NO_CHANGE) {
        logger.debug("volume:{} in pool env not changed", volumeId);
        return true;
      }

      // if pool has changed

      // clear step count, otherwise ratio will show a normal num
      setRebalanceStepCount(volumeId, 0);



      long poolChgTime = volumeId2PoolEnvChgTimestampMap
          .computeIfAbsent(volumeId, time -> time = System.currentTimeMillis());
      if (poolChgTime == 0) {
        poolChgTime = System.currentTimeMillis();
        volumeId2PoolEnvChgTimestampMap.put(volumeId, poolChgTime);
      }

      //if volume is calculating, stop it, then do new calculation
      if (setRebalanceCalcStatus(volumeId, RebalanceCalcStatus.STOP_CALCULATE)) {
        logger.warn(
            "volume:{} environment in pool:{} changed, but last rebalance is calculating, stop "
                + "it.",
            volumeId, storagePool.getPoolId());
        return false;
      }





      if (!isSendTaskOver(volumeMetadata, simulateInstanceBuilder)) {
        logger.warn("volume:{} environment in pool:{} changed, wait remain tasks run over.",
            volumeId, storagePool.getPoolId());
        return false;
      }


      if (poolChangedStatus == SimulatePool.PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME) {
        long curTime = System.currentTimeMillis();
        if (curTime - poolChgTime < rebalanceWaitTimeAfterArchiveLost) {
          logger.warn(
              "archive of volume:{} environment in pool:{} had lost, can not calculate migration "
                  + "steps now, must wait {}ms! current time:{} pool changed time:{}, remain "
                  + "time:{}",
              volumeId, storagePool.getPoolId(), rebalanceWaitTimeAfterArchiveLost, curTime,
              poolChgTime, curTime - poolChgTime);
          throw new NoNeedToRebalance();
        }
      }

      if (!volumeMetadata.isStable()) {
        logger.warn("volume:{} in pool:{} is not stable, can not calculate migration steps now",
            volumeId, storagePool.getPoolId());
        throw new NoNeedToRebalance();
      }

      updateEnvironment(volumeId, storagePool);

      //init
      volumeId2PoolEnvChgTimestampMap.put(volumeId, 0L);

      long rebalanceVersion = volumeStore.incrementRebalanceVersion(volumeId);

      logger.warn(
          "volume:{} environment in pool:{} changed, recalculating the migration steps. next "
              + "rebalanceVersion:{}",
          volumeId, storagePool.getPoolId(), rebalanceVersion);

      if (logger.isDebugEnabled()) {
        try {
          StringBuilder volumeSegInfoBuffer = new StringBuilder();
          for (int segIndex : volumeMetadata.getSegmentTable().keySet()) {
            SegmentMetadata segmentMetadata = volumeMetadata.getSegmentTable().get(segIndex);
            SegmentMembership lastMemberShip = segmentMetadata.getLatestMembership();
            volumeSegInfoBuffer.append("{").append(segIndex).append(", P:")
                .append(lastMemberShip.getPrimary())
                .append(", S:").append(lastMemberShip.getSecondaries())
                .append(", A:").append(lastMemberShip.getArbiters()).append("},\t");
          }
          logger.debug("volume before rebalance: {}", volumeSegInfoBuffer);
        } catch (Exception e) {
          logger.warn("get volume before rebalance failed,", e);
        }
      }
      //recalculate rebalance tasks
      rebalanceCalcExecutor.execute(new RebalanceTaskCalcThread(volumeId, selector, this)
          .setRebalanceTaskCalcStatus(RebalanceCalcStatus.CALCULATING));

      return true;
    } finally {
      volumeUnlock(volumeId);
    }
  }

  public enum RebalanceCalcStatus {
    CALCULATING,     //volume rebalance task is calculating
    STOP_CALCULATE,  //calculate will stop
    STABLE,         //volume rebalance task has calculated, and task stable
  }

  public enum VolumeRebalanceStatus {
    CHECKING,       //check volume is or not need to rebalance by threshold,if not, don't do 
    // rebalance
    WORKING,        //do not care threshold, and must be rebalance.
    BALANCED        //volume all rebalance task run over, and volume is balanced
  }
}
