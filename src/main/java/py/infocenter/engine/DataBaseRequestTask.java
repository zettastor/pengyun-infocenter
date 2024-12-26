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

package py.infocenter.engine;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import py.engine.Task;
import py.infocenter.store.TaskRequestInfo;

public class DataBaseRequestTask implements Task {

  private TaskRequestInfo taskRequestInfo;

  public DataBaseRequestTask(TaskRequestInfo taskRequestInfo) {
    this.taskRequestInfo = taskRequestInfo;
  }

  public TaskRequestInfo getTaskRequestInfo() {
    return taskRequestInfo;
  }

  @Override
  public String toString() {
    return "DataBaseRequestTask{"

        + "taskRequestInfo=" + taskRequestInfo

        + '}';
  }

  @Override
  public void destroy() {

  }

  @Override
  public void doWork() {

  }

  @Override
  public void cancel() {

  }

  @Override
  public boolean isCancel() {
    return false;
  }

  @Override
  public int getToken() {
    return 0;
  }

  @Override
  public void setToken(int token) {

  }

  @Override
  public long getDelay(TimeUnit unit) {
    return 0;
  }

  @Override
  public int compareTo(Delayed o) {
    return 0;
  }
}
