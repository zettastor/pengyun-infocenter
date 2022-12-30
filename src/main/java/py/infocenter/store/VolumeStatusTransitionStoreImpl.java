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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import py.volume.VolumeMetadata;


public class VolumeStatusTransitionStoreImpl implements VolumeStatusTransitionStore {

  private Map<Long, VolumeMetadata> volumeSweeperMap;

  public VolumeStatusTransitionStoreImpl() {
    volumeSweeperMap = new ConcurrentHashMap<>();
  }

  /**
   * Add the volume need to process to the store.
   */
  @Override
  public void addVolumeToStore(VolumeMetadata volume) {
    volumeSweeperMap.put(volume.getVolumeId(), volume);
  }

  /**
   * Pop all the volumes which need to process After this method, the map will be empty;.
   */
  @Override
  public int drainTo(Collection<VolumeMetadata> volumes) {
    int num = 0;
    for (Object obj : volumeSweeperMap.values()) {
      VolumeMetadata volume = (VolumeMetadata) obj;
      volumes.add(volume);
      num++;
    }
    volumeSweeperMap.clear();
    return num;
  }

  /**
   * Clear all the volume need to process.
   */
  @Override
  public void clear() {
    volumeSweeperMap.clear();
  }
}
