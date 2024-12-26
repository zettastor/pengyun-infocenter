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

package py.infocenter.test.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import py.icshare.InstanceMetadata;
import py.infocenter.store.StorageStore;
import py.infocenter.store.StorageStoreImpl;

/**
 * {@link StorageStoreImpl} is a two level storage one of which is memory store, and another one is
 * db store. The instance of this class provide one-level store which is just a memory store. We use
 * this store to do unit test.
 *
 */
public class StorageMemStore implements StorageStore {

  private Map<Long, InstanceMetadata> instanceMap = new ConcurrentHashMap<Long, InstanceMetadata>();

  @Override
  public void save(InstanceMetadata instanceMetadata) {
    instanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
  }

  @Override
  public InstanceMetadata get(long instanceId) {
    return instanceMap.get(instanceId);
  }

  @Override
  public synchronized List<InstanceMetadata> list() {
    List<InstanceMetadata> instanceList = new ArrayList<InstanceMetadata>();
    if (instanceMap.values() == null || instanceMap.values().isEmpty()) {
      return instanceList;
    }

    for (InstanceMetadata instance : instanceMap.values()) {
      instanceList.add(instance);
    }

    return instanceList;
  }

  @Override
  public void delete(long instanceId) {
    instanceMap.remove(instanceId);
  }

  @Override
  public int size() {
    return instanceMap.size();
  }

  @Override
  public void clearMemoryData() {
    instanceMap.clear();
  }

  @Override
  public void saveAll(List<InstanceMetadata> instanceMetadatas) {

  }

}
