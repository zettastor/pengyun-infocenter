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

package py.infocenter.store;

import java.io.IOException;
import java.sql.Blob;
import org.apache.commons.lang.Validate;
import py.informationcenter.Utils;


public class TaskRequestInfo {

  private long taskId;
  private String taskType;

  private Object request;

  private long taskCreateTime;

  public long getTaskId() {
    return taskId;
  }

  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  public String getTaskType() {
    return taskType;
  }

  public void setTaskType(String taskType) {
    this.taskType = taskType;
  }

  public Object getRequest() {
    return request;
  }

  public void setRequest(Object request) {
    this.request = request;
  }

  public long getTaskCreateTime() {
    return taskCreateTime;
  }

  public void setTaskCreateTime(long taskCreateTime) {
    this.taskCreateTime = taskCreateTime;
  }


  
  public TaskRequestInfoDb toTaskRequestInfoDb(TaskStore taskStore) throws IOException {

    TaskRequestInfoDb taskRequestInfoDb = new TaskRequestInfoDb();
    Validate.notNull(taskId);
    taskRequestInfoDb.setTaskId(taskId);

    Validate.notNull(taskType);
    taskRequestInfoDb.setTaskType(taskType);

    String launchDriverRequestStr = Utils.serialize(request);
    Blob launchDriverBlob = taskStore.createBlob(launchDriverRequestStr.getBytes());
    taskRequestInfoDb.setTaskRequest(launchDriverBlob);

    Validate.notNull(taskCreateTime);
    taskRequestInfoDb.setTaskCreateTime(taskCreateTime);

    return taskRequestInfoDb;
  }

  @Override
  public String toString() {
    return "TaskRequestInfo{"

        + "taskId=" + taskId

        + ", taskType='" + taskType + '\''

        + ", request='" + request + '\''

        + ", taskCreateTime=" + taskCreateTime

        + '}';
  }
}
