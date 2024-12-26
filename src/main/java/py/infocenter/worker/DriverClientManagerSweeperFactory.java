
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
