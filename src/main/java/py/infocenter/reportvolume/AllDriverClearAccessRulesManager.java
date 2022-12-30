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

package py.infocenter.reportvolume;

import edu.emory.mathcs.backport.java.util.Collections;
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
