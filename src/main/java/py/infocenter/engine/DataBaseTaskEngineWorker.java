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

package py.infocenter.engine;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.NamedThreadFactory;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.job.DataBaseTaskProcessor;
import py.infocenter.job.LaunchScsiDriverProcessor;
import py.infocenter.job.TaskType;
import py.infocenter.job.UmountScsiDriverProcessor;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.TaskRequestInfo;
import py.infocenter.store.TaskStore;
import py.instance.InstanceStatus;
import py.periodic.Worker;

public class DataBaseTaskEngineWorker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(DataBaseTaskEngineWorker.class);
  private final ThreadPoolExecutor workerPool;
  private TaskStore taskRequestStore;
  private InfoCenterAppContext appContext;
  private int dbTaskMaxConcurrentSize;
  private InformationCenterImpl informationCenterImpl;

  private Semaphore semaphore;
  private Set<Long> taskIdRecordSet;
  private ReentrantLock taskLock;

  public DataBaseTaskEngineWorker(int corePoolSize, int maxPoolSize, int dbTaskMaxConcurrentSize,
      InformationCenterImpl informationCenterImpl) {
    this(corePoolSize, maxPoolSize, dbTaskMaxConcurrentSize, false, informationCenterImpl);
  }


  public DataBaseTaskEngineWorker(int corePoolSize, int maxPoolSize, int dbTaskMaxConcurrentSize,
      boolean allowCoreThreadTimeOut,
      InformationCenterImpl informationCenterImpl) {
    this.dbTaskMaxConcurrentSize = dbTaskMaxConcurrentSize;
    this.taskRequestStore = informationCenterImpl.getTaskStore();
    this.appContext = (InfoCenterAppContext) informationCenterImpl.getAppContext();
    this.taskIdRecordSet = new HashSet<>();
    this.informationCenterImpl = informationCenterImpl;

    semaphore = new Semaphore(maxPoolSize);
    taskLock = new ReentrantLock();

    workerPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 60, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new NamedThreadFactory("DB-task-engine-worker-pool"));
    workerPool.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
  }

  @Override
  public void doWork() throws Exception {
    if (InstanceStatus.SUSPEND == appContext.getStatus()) {
      return;
    }

    logger.info("database task engine start!");
    pullWork();
    logger.info("database task engine over!");
  }

  private void pullWork() throws Exception {
    List<TaskRequestInfo> taskRequestInfoList = null;

    //get all TaskRequestInfo from TaskRequestStore, and check task whether exists
    try {
      taskLock.lock();
      taskRequestInfoList = taskRequestStore.listAllTask(dbTaskMaxConcurrentSize);
      taskRequestInfoList
          .removeIf(taskRequestInfo -> taskIdRecordSet.contains(taskRequestInfo.getTaskId()));
    } finally {
      taskLock.unlock();
    }

    logger.info("got db request task from store:{}", taskRequestInfoList);

    //set all task to queue
    for (TaskRequestInfo taskRequestInfo : taskRequestInfoList) {
      //save and del task record
      taskIdRecordSet.add(taskRequestInfo.getTaskId());

      DataBaseRequestTask task = new DataBaseRequestTask(taskRequestInfo);

      //execute
      try {
        DataBaseTaskProcessor taskProcessor = newTaskProcessor(task);
        if (taskProcessor == null) {
          logger.error("execute task:{} failed", task);
          continue;
        }
        semaphore.acquire();
        workerPool.execute(new FutureTask<>(taskProcessor));
        logger.info("add task to execute. task{}", task);
      } catch (RejectedExecutionException re) {
        logger.error("Because of RejectedExecutionException, Can't submit a task to work threads");
      } catch (Exception e) {
        logger.error("submit task, caught an exception", e);
      }
    }
  }

  private DataBaseTaskProcessor newTaskProcessor(DataBaseRequestTask task) {
    DataBaseTaskProcessor taskProcessor = null;
    TaskRequestInfo taskRequestInfo = task.getTaskRequestInfo();
    if (taskRequestInfo == null) {
      logger.error("DB request task is null.");
      return taskProcessor;
    }

    //
    if (TaskType.LaunchDriver.name().equals(taskRequestInfo.getTaskType())) {
      taskProcessor = new LaunchScsiDriverProcessor(taskRequestInfo, informationCenterImpl,
          new DataBaseTaskEngineCallback());
      logger.warn("get launchDriver request task:{} ", taskRequestInfo);
    } else if (TaskType.UmountDriver.name().equals(taskRequestInfo.getTaskType())) {
      taskProcessor = new UmountScsiDriverProcessor(taskRequestInfo, informationCenterImpl,
          new DataBaseTaskEngineCallback());
      logger.warn("get umountDriver request task:{} ", taskRequestInfo);
    } else {
      logger.error("unknown db request task type, {}", task);
    }

    return taskProcessor;
  }


  public void stop() {
    logger.warn("stop db task engine worker");
    try {
      workerPool.shutdown();
      workerPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (Throwable t) {
      logger.warn("caught an exception", t);
    }
  }


  public class DataBaseTaskEngineCallback {


    public void release(long taskId, boolean taskTryAgain) {
      try {
        taskLock.lock();
        logger.warn("task:{} release!", taskId);
        if (!taskTryAgain) {
          //not remove in db, just remove in task Record
          taskRequestStore.deleteTaskById(taskId);
        }

        Validate.isTrue(taskIdRecordSet.remove(taskId));
      } finally {
        taskLock.unlock();
      }
      semaphore.release();
    }
  }
}
