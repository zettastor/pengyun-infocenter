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
