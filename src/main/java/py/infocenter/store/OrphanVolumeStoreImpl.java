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

package py.infocenter.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


public class OrphanVolumeStoreImpl implements OrphanVolumeStore {

  private static final Logger logger = Logger.getLogger(OrphanVolumeStoreImpl.class);

  private Map<Long, Long> orphanVolumeStore = new ConcurrentHashMap<Long, Long>();
  private long volumeToBeOrphanTime;

  @Override
  public void addOrphanVolume(long volumeId) {
    if (!orphanVolumeStore.containsKey(volumeId)) {
      orphanVolumeStore.put(volumeId, System.currentTimeMillis());
    }
  }

  @Override
  public void removeOrphanVolume(long volumeId) {
    orphanVolumeStore.remove(volumeId);
  }

  @Override
  public List<Long> getOrphanVolume() {

    List<Long> allOrphanVolumes = new ArrayList<Long>();
    Iterator<Map.Entry<Long, Long>> iterator = orphanVolumeStore.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Long> entry = iterator.next();
      if (System.currentTimeMillis() - entry.getValue() > volumeToBeOrphanTime) {
        allOrphanVolumes.add(entry.getKey());
      }
    }
    return allOrphanVolumes;
  }

  public long getVolumeToBeOrphanTime() {
    return volumeToBeOrphanTime;
  }

  public void setVolumeToBeOrphanTime(long volumeToBeOrphanTime) {
    this.volumeToBeOrphanTime = volumeToBeOrphanTime;
  }

}
