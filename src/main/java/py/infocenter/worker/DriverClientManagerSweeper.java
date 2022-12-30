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
