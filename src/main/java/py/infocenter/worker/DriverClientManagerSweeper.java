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
import py.infocenter.driver.client.manger.DriverClientManger;
import py.instance.InstanceStatus;
import py.periodic.Worker;


public class DriverClientManagerSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(DriverClientManagerSweeper.class);

  private AppContext appContext;
  private DriverClientManger driverClientManager;

  @Override
  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("DriverStoreSweeper, only the master do it");
      return;
    }
    logger.info("DriverClientManagerSweeper begin to work");
    driverClientManager.removeOldDriverClientInfo();
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setDriverClientManager(DriverClientManger driverClientManager) {
    this.driverClientManager = driverClientManager;
  }
}
