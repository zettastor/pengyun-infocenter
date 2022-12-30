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
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class TaskStoreImpl implements TaskStore {

  private static final Logger logger = LoggerFactory.getLogger(TaskStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void saveTask(TaskRequestInfo taskRequestInfo) throws IOException {
    if (taskRequestInfo == null) {
      logger.warn("Invalid param, please check them");
      return;
    }

    TaskRequestInfoDb taskRequestInfoDb = taskRequestInfo.toTaskRequestInfoDb(this);
    sessionFactory.getCurrentSession().saveOrUpdate(taskRequestInfoDb);

  }

  @Override
  public List<TaskRequestInfo> listAllTask(int limit) throws Exception {

    List<TaskRequestInfo> taskRequestInfoList = new ArrayList<>();
    StringBuilder hqlSequenceSb = new StringBuilder("from TaskRequestInfoDb ");

    String sortField = "taskCreateTime";
    String sortDirection = "ASC";
    hqlSequenceSb.append(String.format(" order by %s %s ", sortField, sortDirection));

    String hqlSequenceStr = hqlSequenceSb.toString();
    try {
      Query query = sessionFactory.getCurrentSession().createQuery(hqlSequenceStr);
      if (limit != 0) {
        query.setMaxResults(limit);
      }
      logger.debug("hqlSequenceStr: {}", hqlSequenceStr);
      List<TaskRequestInfoDb> taskRequestInfoDbList = query.list();

      if (taskRequestInfoDbList != null) {
        for (TaskRequestInfoDb taskRequestInfoDb : taskRequestInfoDbList) {
          TaskRequestInfo taskRequestInfo = taskRequestInfoDb.toTaskRequestInfo();
          taskRequestInfoList.add(taskRequestInfo);
        }
      }

      return taskRequestInfoList;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }
  }

  @Override
  public TaskRequestInfo getTaskById(long taskId)
      throws IOException, SQLException, ClassNotFoundException {
    TaskRequestInfoDb taskRequestInfoDb = (TaskRequestInfoDb) sessionFactory.getCurrentSession()
        .get(
            TaskRequestInfoDb.class, taskId);
    if (taskRequestInfoDb == null) {
      return null;
    }

    return taskRequestInfoDb.toTaskRequestInfo();
  }

  @Override
  public void deleteTaskById(long taskId) {
    org.hibernate.query.Query query = sessionFactory.getCurrentSession().createQuery(
        "delete TaskRequestInfoDb where taskId = :taskId");
    query.setLong("taskId", taskId);
    query.executeUpdate();

  }

  @Override
  public int clearDb() throws Exception {
    List<TaskRequestInfo> taskRequestInfoList = listAllTask(0);
    for (TaskRequestInfo taskRequestInfo : taskRequestInfoList) {
      deleteTaskById(taskRequestInfo.getTaskId());
    }
    return 1;
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}
