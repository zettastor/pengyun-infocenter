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

package py.infocenter.reportvolume;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AllDriverClearAccessRulesManager {

  private final Map<Long, Set<Long>> volumeIdToDriverIds = new ConcurrentHashMap<>();

  public AllDriverClearAccessRulesManager() {
  }


  public boolean isAllDriverClearAccessRules(long volumeId, int driverCount) {
    Set<Long> driverIds = volumeIdToDriverIds.get(volumeId);

    if (Objects.nonNull(driverIds)) {
      return driverIds.size() == driverCount;
    } else {
      return true;
    }
  }


  public boolean isVolumeNeedClearAccessRules(long volumeId) {
    Set<Long> driverIds = volumeIdToDriverIds.get(volumeId);
    return Objects.nonNull(driverIds);
  }

  public void removeVolume(long volumeId) {
    volumeIdToDriverIds.remove(volumeId);
  }


  
  public void initVolume(long volumeId) throws Exception {
    Set<Long> driverIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    if (Objects.nonNull(volumeIdToDriverIds.putIfAbsent(volumeId, driverIds))) {
      throw new Exception();
    }

  }


  
  public void addDriverHasClearAccessRules(long volumeId, long driverId) {
    Set<Long> driverIds = volumeIdToDriverIds.get(volumeId);

    if (Objects.nonNull(driverIds)) {
      driverIds.add(driverId);
    }
  }
}
