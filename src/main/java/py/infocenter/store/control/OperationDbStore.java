
package py.infocenter.store.control;

import py.icshare.Operation;


public interface OperationDbStore {

  /**
   * save operation to database.
   */
  public void saveOperationToDb(Operation operation);

  /**
   * delete operation from database by domainId.
   */
  public void deleteOperationFromDb(Long operationId);

  /**
   * reload all operations from database.
   */
  public void reloadAllOperationsFromDb();

  /**
   * delete end-operations which pass 30 days.
   */
  public void deleteOldOperations();

  /**
   * clear all operations from DB.
   */
  public void clearDb();
}
