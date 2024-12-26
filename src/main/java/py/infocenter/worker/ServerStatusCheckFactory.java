

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
