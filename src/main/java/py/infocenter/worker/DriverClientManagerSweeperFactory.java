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

import py.app.context.AppContext;
import py.infocenter.driver.client.manger.DriverClientManger;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class DriverClientManagerSweeperFactory implements WorkerFactory {

  private static DriverClientManagerSweeper worker;
  private AppContext appContext;
  private DriverClientManger driverClientManager;

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new DriverClientManagerSweeper();
      worker.setAppContext(appContext);
      worker.setDriverClientManager(driverClientManager);
    }
    return worker;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setDriverClientManager(DriverClientManger driverClientManager) {
    this.driverClientManager = driverClientManager;
  }
}
