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
