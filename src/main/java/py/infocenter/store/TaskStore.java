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
import java.util.List;

public interface TaskStore {

  public void saveTask(TaskRequestInfo taskRequestInfo) throws IOException;

  public List<TaskRequestInfo> listAllTask(int limit) throws Exception;

  public TaskRequestInfo getTaskById(long taskId)
      throws IOException, SQLException, ClassNotFoundException;

  public void deleteTaskById(long taskId);

  public int clearDb() throws Exception;

  public Blob createBlob(byte[] bytes);
}
