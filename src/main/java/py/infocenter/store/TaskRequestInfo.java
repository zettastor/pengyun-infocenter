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
