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


import java.sql.Blob;
import java.util.Set;
import py.informationcenter.Utils;


public class InstanceVolumesInformation {

  private long instanceId;
  private Set<Long> volumeIds;

  public InstanceVolumesInformation() {
  }


  
  public InstanceVolumesInformationDb toInstanceVolumesInformationDb(
      InstanceVolumesInformationStore instanceVolumesInformationStore) {
    InstanceVolumesInformationDb instanceVolumesInformationDb = new InstanceVolumesInformationDb();
    instanceVolumesInformationDb.setInstanceId(instanceId);

    if (volumeIds != null && !volumeIds.isEmpty()) {
      String volumeIdsStr = Utils.bulidJsonStrFromObjectLong(volumeIds);
      Blob volumeIdsStrBlob = instanceVolumesInformationStore.createBlob(volumeIdsStr.getBytes());
      instanceVolumesInformationDb.setVolumeIds(volumeIdsStrBlob);
    }

    return instanceVolumesInformationDb;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public Set<Long> getVolumeIds() {
    return volumeIds;
  }

  public void setVolumeIds(Set<Long> volumeIds) {
    this.volumeIds = volumeIds;
  }
}
