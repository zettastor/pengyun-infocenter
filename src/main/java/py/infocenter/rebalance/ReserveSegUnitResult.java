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

package py.infocenter.rebalance;

import java.util.List;
import py.thrift.share.InstanceMetadataThrift;

/**
 * for ReserveSegUnits,get the result.
 */
public class ReserveSegUnitResult {

  public List<InstanceMetadataThrift> instances; // required
  public long storagePoolId; // required


  public ReserveSegUnitResult(List<InstanceMetadataThrift> instances, long storagePoolId) {
    this.instances = instances;
    this.storagePoolId = storagePoolId;
  }

  public List<InstanceMetadataThrift> getInstances() {
    return instances;
  }

  public void setInstances(List<InstanceMetadataThrift> instances) {
    this.instances = instances;
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }
}
