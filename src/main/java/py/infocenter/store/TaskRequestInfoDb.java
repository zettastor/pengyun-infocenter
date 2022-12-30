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
