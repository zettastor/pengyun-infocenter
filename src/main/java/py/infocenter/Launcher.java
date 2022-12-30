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

package py.infocenter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import py.monitor.jmx.server.JmxAgent;


public class Launcher extends py.app.Launcher {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

  public Launcher(String beansHolder, String serviceRunningPath) {
    super(beansHolder, serviceRunningPath);
  }



  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      logger.error("Usage: error args");
      System.exit(0);
    }

    Launcher launcher = new Launcher(InformationCenterAppConfig.class.getName() + ".class",
        args[0]);
    launcher.launch();
  }

  @Override
  public void startAppEngine(ApplicationContext appContext) {
    try {
      InformationCenterAppEngine engine = appContext.getBean(InformationCenterAppEngine.class);
      logger.info("info center get Max network Frame size is {}", engine.getMaxNetworkFrameSize());
      engine.start();
    } catch (Exception e) {
      logger.error("Caught an exception when start infocenter service", e);
      System.exit(1);
    }
  }

  @Override
  protected void startMonitorAgent(ApplicationContext appContext) throws Exception {
    try {
      JmxAgent jmxAgent = appContext.getBean(JmxAgent.class);
      jmxAgent.start();
    } catch (Exception e) {
      logger.error("Caught an exception when start dih service", e);
      System.exit(1);
    }
  }
}