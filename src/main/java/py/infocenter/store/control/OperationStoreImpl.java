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

package py.infocenter.store.control;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.Operation;
import py.icshare.OperationInformation;
import py.icshare.OperationStatus;



@Transactional
public class OperationStoreImpl implements OperationStore, OperationDbStore {

  private static final Logger logger = LoggerFactory.getLogger(OperationStoreImpl.class);
  private static final long DAY_TO_MILLSECOND = 24 * 60 * 60 * 1000L; // ms
  private SessionFactory sessionFactory;
  private Map<Long, Operation> activeOperationMap = new ConcurrentHashMap<>();
  private Map<Long, Operation> endOperationMap = new ConcurrentHashMap<>();
  private int saveOperationDays;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void saveOperationToDb(Operation operation) {
    if (operation == null) {
      logger.warn("Invalid param. please check it");
      return;
    }
    OperationInformation operationInformation = operation.toOperationInformation();
    logger.info("save operation to db,the operation is {}", operation);
    sessionFactory.getCurrentSession().saveOrUpdate(operationInformation);
  }

  @Override
  public void deleteOperationFromDb(Long operationId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete OperationInformation where operationId= :operationId");
    query.setLong("operationId", operationId);
    query.executeUpdate();
  }

  @Override
  public void reloadAllOperationsFromDb() {
    @SuppressWarnings("unchecked")
    List<OperationInformation> allOperationsInDb = sessionFactory.getCurrentSession()
        .createQuery("from OperationInformation").list();
    for (OperationInformation operationInformation : allOperationsInDb) {
      Operation operation = operationInformation.toOperation();
      if (operation.getStatus() == OperationStatus.ACTIVITING) {
        activeOperationMap.put(operation.getOperationId(), operation);
      } else {
        endOperationMap.put(operation.getOperationId(), operation);
      }
    }
  }

  @Override
  public void saveOperation(Operation operation) {
    if (operation == null) {
      logger.warn("Invalid param. please check it");
      return;
    }
    // load operation form db, if memory-map is null
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      reloadAllOperationsFromDb();
    }
    // save to memory
    // if memory not change, will not save to db
    Long operationId = operation.getOperationId();
    if (operation.getStatus() == OperationStatus.ACTIVITING) {
      activeOperationMap.put(operationId, operation);
      endOperationMap.remove(operationId);
    } else {
      endOperationMap.put(operationId, operation);
      activeOperationMap.remove(operationId);
    }
    // save to DB
    logger.info("save operation, the operation is {}", operation);
    saveOperationToDb(operation);
  }

  @Override
  public void deleteOperation(Long operationId) {
    // delete from DB
    deleteOperationFromDb(operationId);
    // delete from memory, no matter the status
    endOperationMap.remove(operationId);
    activeOperationMap.remove(operationId);
  }

  @Override
  public Operation getOperation(Long operationId) {
    // memory-map is null
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      reloadAllOperationsFromDb();
    }
    // return end first
    if (endOperationMap.containsKey(operationId)) {
      return endOperationMap.get(operationId);
    } else if (activeOperationMap.containsKey(operationId)) {
      return activeOperationMap.get(operationId);
    }
    return null;
  }

  @Override
  public List<Operation> getAllOperation() {
    List<Operation> allOperationList = new ArrayList<>();
    // memory-map is null
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      reloadAllOperationsFromDb();
    }
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      return allOperationList;
    }
    allOperationList.addAll(activeOperationMap.values());
    allOperationList.addAll(endOperationMap.values());
    return allOperationList;
  }

  @Override
  public List<Operation> getActiveOperation() {
    List<Operation> activeOperationList = new ArrayList<>();
    // memory-map is null
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      reloadAllOperationsFromDb();
    }
    activeOperationList.addAll(activeOperationMap.values());
    return activeOperationList;
  }

  @Override
  public List<Operation> getEndOperation() {
    List<Operation> endOperationList = new ArrayList<>();
    // memory-map is null
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      reloadAllOperationsFromDb();
    }
    endOperationList.addAll(endOperationMap.values());
    return endOperationList;
  }

  @Override
  public void clearMemory() {
    activeOperationMap.clear();
    endOperationMap.clear();
   
  }

  @Override
  public void clearDb() {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete OperationInformation where 1=1");
    query.executeUpdate();
  }

  // delete end-operation that pass 30 days in db and memory.
  @Override
  public void deleteOldOperations() {
    // memory-map is null
    if ((activeOperationMap == null || activeOperationMap.isEmpty())
        && (endOperationMap == null || endOperationMap.isEmpty())) {
      reloadAllOperationsFromDb();
    }
    if (endOperationMap == null || endOperationMap.isEmpty()) {
      return;
    }
    Collection<Operation> operationCollection = endOperationMap.values();
    for (Operation operationEnd : operationCollection) {
      long endTime = operationEnd.getEndTime();
      long startTime = operationEnd.getStartTime();
      if (endTime - startTime < saveOperationDays * DAY_TO_MILLSECOND) {
        continue;
      } else {
        deleteOperation(operationEnd.getOperationId());
      }
    }
  }

  public void setSaveOperationDays(int saveOperationDays) {
    this.saveOperationDays = saveOperationDays;
  }

}
