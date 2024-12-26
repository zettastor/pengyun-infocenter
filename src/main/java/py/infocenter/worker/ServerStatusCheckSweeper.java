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

package py.infocenter.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.infocenter.service.ServerStatusCheck;
import py.instance.InstanceStatus;
import py.periodic.Worker;

public class ServerStatusCheckSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(ServerStatusCheckSweeper.class);

  private AppContext appContext;
  private ServerStatusCheck serverStatusCheck;

  @Override
  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      // delete the memory database
      logger.info("only the master can do check status");
      return;
    }

    logger.warn("begin to check server status");
    serverStatusCheck.doCheck();
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setServerStatusCheck(ServerStatusCheck serverStatusCheck) {
    this.serverStatusCheck = serverStatusCheck;
  }
}
