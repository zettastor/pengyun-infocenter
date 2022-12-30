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

import java.util.Map;
import py.app.context.AppContext;
import py.icshare.InstanceMaintenanceDbStore;
import py.infocenter.store.ServerNodeStore;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class ServerNodeAlertCheckerFactory implements WorkerFactory {

  private Map<String, Long> serverNodeReportTimeMap;
  private int serverNodeReportOverTimeSecond;
  private InstanceStore instanceStore;
  private ServerNodeStore serverNodeStore;
  private InstanceMaintenanceDbStore instanceMaintenanceDbStore;
  private AppContext appContext;
  private ServerNodeAlertChecker serverNodeAlertChecker;

  @Override
  public Worker createWorker() {

    if (serverNodeAlertChecker == null) {
      serverNodeAlertChecker = new ServerNodeAlertChecker(serverNodeReportTimeMap,
          serverNodeReportOverTimeSecond, instanceStore, serverNodeStore,
          instanceMaintenanceDbStore, appContext);
    }
    return serverNodeAlertChecker;
  }

  public void setServerNodeReportTimeMap(Map<String, Long> serverNodeReportTimeMap) {
    this.serverNodeReportTimeMap = serverNodeReportTimeMap;
  }

  public void setServerNodeReportOverTimeSecond(int serverNodeReportOverTimeSecond) {
    this.serverNodeReportOverTimeSecond = serverNodeReportOverTimeSecond;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public void setServerNodeStore(ServerNodeStore serverNodeStore) {
    this.serverNodeStore = serverNodeStore;
  }

  public void setInstanceMaintenanceDbStore(InstanceMaintenanceDbStore instanceMaintenanceDbStore) {
    this.instanceMaintenanceDbStore = instanceMaintenanceDbStore;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }
}
