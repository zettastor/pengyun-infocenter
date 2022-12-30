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
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;


public interface DriverClientDbStore {

  public void updateToDb(DriverClientInformation driverClientInformation);

  public void saveToDb(DriverClientInformation driverClientInformation);

  public List<DriverClientInformation> getByVolumeIdFromDb(long volumeId);

  public List<DriverClientInformation> getByDriverKeyFromDb(DriverClientKey driverClientKey);

  List<DriverClientInformation> getByDriverContainerIdFromDb(long driverContainerId);

  public List<DriverClientInformation> listFromDb();

  public int deleteFromDb(long volumeId);

  public int deleteFromDb(long volumeId, DriverType driverType, int snapshotId,
      long driverContainerId);

  public int deleteFromDb(DriverClientKey driverClientKey);

  public int deleteFromDb(DriverClientInformation driverClientInformation);
}
