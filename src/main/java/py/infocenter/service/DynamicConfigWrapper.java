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

package py.infocenter.service;

import java.util.Map;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import py.infocenter.common.InfoCenterConstants;


@Deprecated
public class DynamicConfigWrapper {

  private static final Logger logger = Logger.getLogger(DynamicConfigWrapper.class);
  private Map<String, String> dynamicParameters;

  private DynamicConfigWrapper() {
    dynamicParameters = new ConcurrentHashMap<String, String>();
    // initialize this class by spring configuration files
    dynamicParameters.put("log.level", "DEBUG");
    dynamicParameters.put("volumeToBeCreatedTimeout",
        String.valueOf(InfoCenterConstants.getVolumeToBeCreatedTimeout()));
    dynamicParameters.put("segmentUnitReportTimeout",
        String.valueOf(InfoCenterConstants.getSegmentUnitReportTimeout()));
    dynamicParameters.put("timeOfdeadVolumeToRemove",
        String.valueOf(InfoCenterConstants.getTimeOfdeadVolumeToRemove()));

  }

  public static DynamicConfigWrapper getInstance() {
    return LazyHolder.singletonInstance;
  }

  public Map<String, String> getDynamicParameters() {
    return dynamicParameters;
  }


  
  public void setParammeter(String name, String value) {
    boolean matchKey = false;

    // apply the change
    if (name.equalsIgnoreCase("log.level")) {
      logger.debug("leve change to " + value);
      LogManager.getRootLogger().setLevel(Level.toLevel(value));
      matchKey = true;
    } else if (name.equalsIgnoreCase("volumeToBeCreatedTimeout")) {
      int volumeToBeCreatedTimeout = Integer.parseInt(value);
      InfoCenterConstants.setVolumeToBeCreatedTimeout(volumeToBeCreatedTimeout);
      matchKey = true;
    } else if (name.equalsIgnoreCase("segmentUnitReportTimeout")) {
      int segmentUnitReportTimeout = Integer.parseInt(value);
      InfoCenterConstants.setSegmentUnitReportTimeout(segmentUnitReportTimeout);
      matchKey = true;
    } else if (name.equalsIgnoreCase("timeOfdeadVolumeToRemove")) {
      int timeOfdeadVolumeToRemove = Integer.parseInt(value);
      InfoCenterConstants.setTimeOfdeadVolumeToRemove(timeOfdeadVolumeToRemove);
      matchKey = true;
    } else {
      logger.debug("can't set this parameter " + name);
    }

    if (matchKey) {
      // store the change of this parameter.
      dynamicParameters.put(name, value);
    }
  }

  private static class LazyHolder {

    private static final DynamicConfigWrapper singletonInstance = new DynamicConfigWrapper();
  }
}
