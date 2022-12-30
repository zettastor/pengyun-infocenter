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

import java.util.List;
import py.icshare.Operation;


public interface OperationStore {

  /**
   * save operation to memory map and DB.
   */
  public void saveOperation(Operation operation);

  /**
   * delete operation from memory map and DB.
   */
  public void deleteOperation(Long operationId);

  /**
   * get operation from memory map or DB.
   */
  public Operation getOperation(Long operationId);

  /**
   * delete operations that pass 30 days.
   */
  public void deleteOldOperations();

  /**
   * get all operation from memory map or DB.
   */
  public List<Operation> getAllOperation();

  /**
   * get active operation from memory map or DB.
   */
  public List<Operation> getActiveOperation();

  /**
   * get end operation from memory map or DB.
   */
  public List<Operation> getEndOperation();


  /**
   * clear all memory map to sync data from database again.
   */
  public void clearMemory();

  /**
   * set meximum days that end operation exits.
   */
  public void setSaveOperationDays(int saveOperationDays);

}
