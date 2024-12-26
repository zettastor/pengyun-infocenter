/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
