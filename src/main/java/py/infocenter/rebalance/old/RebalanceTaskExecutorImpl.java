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

package py.infocenter.rebalance.old;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.infocenter.rebalance.old.processor.BasicRebalanceTaskContext;
import py.infocenter.rebalance.old.processor.RebalanceTaskContextFactoryImpl;
import py.infocenter.rebalance.old.processor.RebalanceTaskProcessorFactoryImpl;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStore;
import py.thrift.datanode.service.DataNodeService;

/**
 * This class works just like segment unit task executor :).
 *
 */
public class RebalanceTaskExecutorImpl implements RebalanceTaskExecutor {

  private static final Logger logger = LoggerFactory.getLogger(RebalanceTaskExecutorImpl.class);

  private final AppContext appContext;

  private final ThreadPoolExecutor threadPoolExecutor;

  private final DelayQueue<BasicRebalanceTaskContext> taskQueue;
  private final Thread taskPullerThread;

  private final RebalanceTaskContextFactory contextFactory;
  private final RebalanceTaskProcessorFactory processorFactory;

  private volatile boolean isInterrupted;
  private volatile boolean pause = true;



  public RebalanceTaskExecutorImpl(InformationCenterImpl informationCenter, AppContext appContext,
      InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFacotry,
      int corePoolSize, int maximumPoolSize, int keepAliveTimeSeconds, int rebalanceTaskCount) {
    this.appContext = appContext;
    this.taskQueue = new DelayQueue<>();
    this.contextFactory = new RebalanceTaskContextFactoryImpl();
    this.processorFactory = new RebalanceTaskProcessorFactoryImpl(instanceStore, informationCenter,
        dataNodeSyncClientFacotry);

    this.threadPoolExecutor = new ThreadPoolExecutorWithCallBack(corePoolSize, maximumPoolSize,
        keepAliveTimeSeconds, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new NamedThreadFactory("Rebalance-Task-Executor"));
    final TaskPuller puller = new TaskPuller();
    taskPullerThread = new Thread("Rebalance-Task-Puller") {
      @Override
      public void run() {
        puller.run();
      }
    };
    taskPullerThread.start();

    int initDelay = 10000; // start task 10 seconds later.
    for (int i = 0; i < rebalanceTaskCount; i++) {
      BasicRebalanceTaskContext initContext = new BasicRebalanceTaskContext(i * 5000 + initDelay,
          this.appContext);
      taskQueue.put(initContext);
    }

  }

  @Override
  public void start() {
    pause = false;
  }

  @Override
  public void pause() {
    pause = true;
  }

  @Override
  public boolean started() {
    return !pause;
  }

  public void processDone(RebalanceTaskExecutionResult result) {
    BasicRebalanceTaskContext newContext = contextFactory.generateContext(result);
    taskQueue.put(newContext);
  }

  @Override
  public void shutdown() {
    isInterrupted = true;
    threadPoolExecutor.shutdown();
    try {
      if (!threadPoolExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        logger.warn("executor still running");
      }
    } catch (InterruptedException e) {
      logger.warn("Opps, interrupted !");
    }
  }

  private class ThreadPoolExecutorWithCallBack extends ThreadPoolExecutor {

    public ThreadPoolExecutorWithCallBack(int corePoolSize, int maximumPoolSize, long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      // the execution of RebalanceTaskProcessor must not throw any exception,
      Validate.isTrue(t == null);

      if (r instanceof Future<?>) {
        try {
          Object resultObject = ((Future<?>) r).get();
          Validate.isTrue(resultObject instanceof RebalanceTaskExecutionResult);
          RebalanceTaskExecutionResult result = (RebalanceTaskExecutionResult) resultObject;
          processDone(result);
        } catch (ExecutionException ee) {
          logger.warn("No way we can get here", ee);
          Validate.isTrue(false);
        } catch (InterruptedException ie) {
          logger.warn("caught an interrupt exception", ie);
          Thread.currentThread().interrupt(); // ignore/reset
        }
      } else {
        logger.error("No way {} is not an instance of a Future class", r);
        Validate.isTrue(false);
      }

    }

  }

  private class TaskPuller implements Runnable {

    @Override
    public void run() {

      List<BasicRebalanceTaskContext> contextsRetrieved = new ArrayList<>();
      while (!isInterrupted) {
        try {
          if (pause) {
            try {
              Thread.sleep(2000);
            } catch (InterruptedException e) {
              isInterrupted = true;
            }
            continue;
          }

          // wait forever until an available task available to execute
          try {
            taskQueue.drainTo(contextsRetrieved);
          } catch (Exception e) {
            logger.error("Caught an exception when draining contexts from the delay queue. "
                + "Some contexts might miss because of this exception. "
                + "Let's exit the system and restart it again", e);
            isInterrupted = true;
            continue;
          }

          if (contextsRetrieved.size() == 0) {
            // nothing is available, Sleep 1 seconds
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              isInterrupted = true;
            }
            continue;
          }

          for (BasicRebalanceTaskContext context : contextsRetrieved) {
            submitContext(context);
          }

          contextsRetrieved.clear();
        } catch (Exception e) {
          logger.error("task puller caught an exception ", e);
        } finally {
          // stopTimer(pollTimerContext);
        }
      }

    }

    private void submitContext(BasicRebalanceTaskContext context) {
      try {
        RebalanceTaskProcessor processor = processorFactory.generateProcessor(context);
        threadPoolExecutor.execute(new FutureTask<>(processor));
      } catch (RejectedExecutionException re) {
        logger.debug("rejected, put it back to delay queue {}", context);
        context.updateDelay(5000);
        taskQueue.put(context);
      }
    }

  }

}
