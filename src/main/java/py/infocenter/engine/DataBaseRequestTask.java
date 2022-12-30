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
