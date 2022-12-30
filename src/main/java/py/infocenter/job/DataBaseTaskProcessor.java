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

package py.infocenter.job;

import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.engine.Result;
import py.infocenter.engine.DataBaseTaskEngineWorker;
import py.infocenter.store.TaskRequestInfo;

public abstract class DataBaseTaskProcessor implements Callable<Result> {

  private static final Logger logger = LoggerFactory.getLogger(DataBaseTaskProcessor.class);

  protected TaskRequestInfo taskRequestInfo;
  protected DataBaseTaskEngineWorker.DataBaseTaskEngineCallback callback;

  public DataBaseTaskProcessor(TaskRequestInfo taskRequestInfo,
      DataBaseTaskEngineWorker.DataBaseTaskEngineCallback callback) {
    this.taskRequestInfo = taskRequestInfo;
    this.callback = callback;
  }

  public TaskRequestInfo getTaskRequestInfo() {
    return taskRequestInfo;
  }

  @Override
  public Result call() throws Exception {
    boolean taskTryAgain = false;
    try {
      taskTryAgain = doWork();
    } catch (Exception e) {
      logger.error("Caught Exception when do DB task processor, ", e);
    } finally {
      callback.release(taskRequestInfo.getTaskId(), taskTryAgain);
    }
    return null;
  }

  public abstract boolean doWork();
}
