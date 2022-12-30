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

package py.infocenter.rebalance.thread;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.selector.VolumeRebalanceSelector;
import py.infocenter.rebalance.struct.InternalRebalanceTask;
import py.infocenter.rebalance.struct.RebalanceTaskManager;
import py.thrift.infocenter.service.SegmentNotFoundExceptionThrift;

/**
 * rebalance calculate thread.
 */
public class RebalanceTaskCalcThread implements Runnable {

  private final long taskSaveTriggerCount = 10;
  private Logger logger = LoggerFactory.getLogger(RebalanceTaskCalcThread.class);
  private long volumeId;
  private VolumeRebalanceSelector selector;
  private RebalanceTaskManager rebalanceTaskManager;
  private boolean doArbiter = false;
  private Map<Integer, List<InternalRebalanceTask>> allTaskStepMap = new HashMap<>();


  public RebalanceTaskCalcThread(long volumeId, VolumeRebalanceSelector selector,
      RebalanceTaskManager rebalanceTaskManager) {
    this.volumeId = volumeId;
    this.selector = selector;
    this.rebalanceTaskManager = rebalanceTaskManager;

    //prevent no any task of volume in rebalanceTaskManager, other datanode cannot get task then 
    // may be shutdown(like rebalanceTaskManager::hasAnyTasks())
    rebalanceTaskManager.addRebalanceTasks(volumeId, new LinkedList<>());
  }

  public RebalanceTaskCalcThread setRebalanceTaskCalcStatus(
      RebalanceTaskManager.RebalanceCalcStatus rebalanceCalcStatus) {
    rebalanceTaskManager.setRebalanceCalcStatus(volumeId, rebalanceCalcStatus);
    return this;
  }

  @Override
  public void run() {
    int primaryMigrateCount = 0;
    int secondaryMigrateCount = 0;
    try {
      //update task list status, just for test
      rebalanceTaskManager.setResultOfRebalance(volumeId, selector.getSimulateVolume());

      logger.warn("volume:{} start to recalculate rebalance tasks", volumeId);
      //parse volume metadata, then get primary and secondary distribution
      selector.parseVolumeInfo();

      List<InternalRebalanceTask> internalTaskList = new LinkedList<>();
      int taskStepCount = 0;
      boolean doContinue = true;
      while (doContinue) {

        if (rebalanceTaskManager.getRebalanceCalcStatus(volumeId)
            == RebalanceTaskManager.RebalanceCalcStatus.STOP_CALCULATE) {
          logger.warn("rebalance tasks calculation is canceled. volume:{}", volumeId);
          return;
        }

        if (internalTaskList.size() >= taskSaveTriggerCount) {
          saveAllTasks(internalTaskList);

          rebalanceTaskManager.addRebalanceTasks(volumeId, internalTaskList);
          taskStepCount += internalTaskList.size();
          internalTaskList.clear();
        }

        try {
          logger.debug("try primary rebalance");
          internalTaskList.addAll(selector.selectPrimaryRebalanceTask());
          primaryMigrateCount++;
          continue;
        } catch (NoNeedToRebalance ne) {
          logger.debug("no need for primary rebalance", ne);
        } catch (Exception e) {
          logger.error("cause a exception: {}", e);
          doContinue = false;
        }

        //if environment changed, stop to calculate
        if (rebalanceTaskManager.getRebalanceCalcStatus(volumeId)
            == RebalanceTaskManager.RebalanceCalcStatus.STOP_CALCULATE) {
          logger.warn("rebalance tasks calculation is canceled. volume:{}", volumeId);
          return;
        }

        try {
          logger.debug("try PS rebalance");
          internalTaskList.addAll(selector.selectPsRebalanceTask());
          secondaryMigrateCount++;
        } catch (NoNeedToRebalance ne) {
          logger.debug("no need for PS rebalance", ne);
          doContinue = false;
        } catch (Exception e) {
          logger.error("cause a exception: {}", e);
          doContinue = false;
        }
      }
      logger.warn("after phase1: P migrate step:{}, S migrate step:{}", primaryMigrateCount,
          secondaryMigrateCount);

      if (doArbiter) {
        doContinue = true;
        while (doContinue) {

          if (rebalanceTaskManager.getRebalanceCalcStatus(volumeId)
              == RebalanceTaskManager.RebalanceCalcStatus.STOP_CALCULATE) {
            logger.warn("rebalance tasks calculation is canceled. volume:{}", volumeId);
            return;
          }

          if (internalTaskList.size() >= taskSaveTriggerCount) {
            saveAllTasks(internalTaskList);

            rebalanceTaskManager.addRebalanceTasks(volumeId, internalTaskList);
            taskStepCount += internalTaskList.size();
            internalTaskList.clear();
          }

          try {
            logger.debug("try arbiter rebalance");
            internalTaskList.addAll(selector.selectArbiterRebalanceTask());
          } catch (NoNeedToRebalance ne) {
            logger.debug("no need for A rebalance", ne);
            doContinue = false;
          } catch (Exception e) {
            logger.error("cause a exception: {}", e);
            doContinue = false;
          }
        }
      }

      if (internalTaskList.size() > 0) {
        saveAllTasks(internalTaskList);
        rebalanceTaskManager.addRebalanceTasks(volumeId, internalTaskList);
        taskStepCount += internalTaskList.size();
      }

      rebalanceTaskManager.setResultOfRebalance(volumeId, selector.getSimulateVolume());
      rebalanceTaskManager.setRebalanceStepCount(volumeId, taskStepCount);
      logger.warn("volume:{} recalculate rebalance tasks over! need ({}) steps to rebalance",
          volumeId, taskStepCount);
      logger.debug("all tasks: {}", allTaskStepMap);
      selector.printRebalanceResultLog();
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.error("volume:{} calculate rebalance task caught a exception", e);
    } catch (Exception e) {
      logger.error("volume:{} calculate rebalance task caught a exception", e);
    } finally {
      //update task list status
      setRebalanceTaskCalcStatus(RebalanceTaskManager.RebalanceCalcStatus.STABLE);
    }
  }

  /**
   * just for log.
   *
   * @param internalTaskList tasks
   */
  private void saveAllTasks(List<InternalRebalanceTask> internalTaskList) {
    if (logger.isDebugEnabled()) {
      for (InternalRebalanceTask taskTemp : internalTaskList) {
        List<InternalRebalanceTask> taskListTemp = allTaskStepMap
            .computeIfAbsent(taskTemp.getSegId().getIndex(),
                value -> new LinkedList<>());
        taskListTemp.add(taskTemp);
      }
    }
  }
}
