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