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
import py.driver.DriverType;
import py.icshare.DriverInformation;


public interface DriverDbStore {

  public void updateToDb(DriverInformation driverInformation);

  public void saveToDb(DriverInformation driverInformation);

  public List<DriverInformation> getByVolumeIdFromDb(long volumeId);

  public List<DriverInformation> getByDriverKeyFromDb(long volumeId, DriverType driverType,
      int snapshotId);

  List<DriverInformation> getByDriverContainerIdFromDb(long driverContainerId);

  public List<DriverInformation> listFromDb();

  public int deleteFromDb(long volumeId);

  public int deleteFromDb(long volumeId, DriverType driverType, int snapshotId);

  public int updateStatusToDb(long volumeId, DriverType driverType, int snapshotId, String status);

  public int updateMakeUnmountDriverForCsiToDb(long driverContainerId, long volumeId,
      DriverType driverType, int snapshotId,
      boolean makeUnmountForCsi);
}
