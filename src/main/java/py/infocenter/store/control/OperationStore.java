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
