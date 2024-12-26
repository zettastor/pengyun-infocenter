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
import java.sql.SQLException;
import javax.persistence.Lob;
import org.apache.commons.lang.Validate;
import org.hibernate.annotations.Type;
import py.informationcenter.Utils;

public class TaskRequestInfoDb {

  private long taskId;
  private String taskType;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob taskRequest;

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

  public Blob getTaskRequest() {
    return taskRequest;
  }

  public void setTaskRequest(Blob taskRequest) {
    this.taskRequest = taskRequest;
  }

  public long getTaskCreateTime() {
    return taskCreateTime;
  }

  public void setTaskCreateTime(long taskCreateTime) {
    this.taskCreateTime = taskCreateTime;
  }



  public TaskRequestInfo toTaskRequestInfo()
      throws IOException, SQLException, ClassNotFoundException {
    TaskRequestInfo taskRequestInfo = new TaskRequestInfo();
    Validate.notNull(taskId);
    taskRequestInfo.setTaskId(taskId);

    Validate.notNull(taskType);
    taskRequestInfo.setTaskType(taskType);

    String taskRequestStr = new String(py.license.Utils.readFrom(taskRequest));
    Object request = Utils.deserialize(taskRequestStr);
    taskRequestInfo.setRequest(request);

    Validate.notNull(taskCreateTime);
    taskRequestInfo.setTaskCreateTime(taskCreateTime);

    return taskRequestInfo;
  }

  @Override
  public String toString() {
    return "TaskRequestInfo{"

        + "taskId=" + taskId

        + ", taskType='" + taskType + '\''

        + ", taskRequest=" + taskRequest

        + ", taskCreateTime=" + taskCreateTime

        + '}';
  }
}
