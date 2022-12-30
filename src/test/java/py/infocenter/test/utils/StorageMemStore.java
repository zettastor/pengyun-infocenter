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
