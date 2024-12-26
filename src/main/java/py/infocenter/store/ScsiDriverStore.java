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
import py.icshare.DriverKeyForScsi;
import py.icshare.ScsiDriverMetadata;


public interface ScsiDriverStore {

  public List<ScsiDriverMetadata> get(long volumeId);

  public List<ScsiDriverMetadata> get(long volumeId, int snapshotId);

  public ScsiDriverMetadata get(DriverKeyForScsi driverKeyForScsi);

  List<ScsiDriverMetadata> getByDriverKeyFromDb(long drivercontainerId, long volumeId,
      int snapshotId);

  public List<ScsiDriverMetadata> list();

  List<ScsiDriverMetadata> getByDriverContainerId(long drivercontainerId);

  public void delete(long volumeId);

  public void delete(long volumeId, int snapshotId);

  public void delete(long drivercontainerId, long volumeId, int snapshotId);

  public int deleteFromDb(long drivercontainerId, long volumeId, int snapshotId);

  public void save(ScsiDriverMetadata driverMetadata);

  public void clearMemoryData();

  List<ScsiDriverMetadata> getByDriverContainerIdFromDb(long drivercontainerId);


}