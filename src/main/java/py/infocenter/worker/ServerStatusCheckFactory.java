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
import py.infocenter.service.ServerStatusCheck;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

public class ServerStatusCheckFactory implements WorkerFactory {

  private AppContext appContext;
  private ServerStatusCheck serverStatusCheck;
  private ServerStatusCheckSweeper serverStatusCheckSweeper;

  @Override
  public Worker createWorker() {

    if (serverStatusCheckSweeper == null) {
      serverStatusCheckSweeper = new ServerStatusCheckSweeper();
      serverStatusCheckSweeper.setAppContext(appContext);
      serverStatusCheckSweeper.setServerStatusCheck(serverStatusCheck);
    }
    return serverStatusCheckSweeper;
  }

  public void setServerStatusCheck(ServerStatusCheck serverStatusCheck) {
    this.serverStatusCheck = serverStatusCheck;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }
}
