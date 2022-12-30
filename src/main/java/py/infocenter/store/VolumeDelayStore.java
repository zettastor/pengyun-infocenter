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
import py.icshare.VolumeDeleteDelayInformation;


public interface VolumeDelayStore {

  public int deleteVolumeDelayInfo(long volumeId);

  public void saveVolumeDelayInfo(VolumeDeleteDelayInformation volumeDeleteDelayInformation);

  public List<VolumeDeleteDelayInformation> listVolumesDelayInfo();

  public VolumeDeleteDelayInformation getVolumeDelayInfo(long volumeId);

  long updateVolumeDelayTimeInfo(long volumeId, long timeForDelay);

  public boolean updateVolumeDelayStatusInfo(long volumeId, boolean stopDelay);

  public void clearAll();
}

