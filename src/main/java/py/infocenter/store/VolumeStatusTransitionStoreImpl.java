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
