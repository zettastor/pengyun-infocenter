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
