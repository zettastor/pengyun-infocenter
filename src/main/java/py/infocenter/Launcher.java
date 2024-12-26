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