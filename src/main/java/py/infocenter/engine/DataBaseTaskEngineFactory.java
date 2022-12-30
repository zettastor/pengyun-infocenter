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

import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.TaskStore;
import py.periodic.WorkerFactory;

public class DataBaseTaskEngineFactory implements WorkerFactory {

  private static DataBaseTaskEngineWorker worker;
  private int dbTaskCorePoolSize;
  private int dbTaskMaxPoolSize;
  private int dbTaskMaxConcurrentSize;
  private InformationCenterImpl informationCenterImpl;

  
  public DataBaseTaskEngineFactory(int dbTaskCorePoolSize, int dbTaskMaxPoolSize,
      int dbTaskMaxConcurrentSize, TaskStore taskRequestStore,
      InfoCenterAppContext appContext) {
    this.dbTaskCorePoolSize = dbTaskCorePoolSize;
    this.dbTaskMaxPoolSize = dbTaskMaxPoolSize;
    this.dbTaskMaxConcurrentSize = dbTaskMaxConcurrentSize;
  }

  @Override
  public DataBaseTaskEngineWorker createWorker() {
    if (worker == null) {
      worker = new DataBaseTaskEngineWorker(dbTaskCorePoolSize, dbTaskMaxPoolSize,
          dbTaskMaxConcurrentSize, informationCenterImpl);

    }
    return worker;
  }

  public void setInformationCenterImpl(InformationCenterImpl informationCenterImpl) {
    this.informationCenterImpl = informationCenterImpl;
  }

  
  public void stop() {
    if (worker == null) {
      worker.stop();
    }
  }
}
