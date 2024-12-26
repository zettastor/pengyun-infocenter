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
