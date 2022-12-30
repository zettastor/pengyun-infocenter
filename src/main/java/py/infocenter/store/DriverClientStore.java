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

import com.google.common.collect.Multimap;
import java.util.List;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;


public interface DriverClientStore {

  public DriverClientInformation getLastTimeValue(DriverClientKey driverClientKey);

  public List<DriverClientInformation> list();

  public Multimap<DriverClientKey, DriverClientInformation> listDriverKey();

  public void loadToMemory();

  public void delete(DriverClientKey driverClientKey);

  public void deleteValue(DriverClientInformation driverClientInformation);

  public void save(DriverClientInformation driverClientInformation);

  public void clearMemoryData();
}