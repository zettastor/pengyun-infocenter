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

import java.util.List;
import py.icshare.InstanceMetadata;


public interface StorageStore {

  public void save(InstanceMetadata instanceMetadata);

  public InstanceMetadata get(long instanceId);

  public List<InstanceMetadata> list();

  public void delete(long instanceId);

  public int size();

  public void clearMemoryData();

  public void saveAll(List<InstanceMetadata> instanceMetadatas);
}
