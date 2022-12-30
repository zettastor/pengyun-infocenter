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

package py.infocenter.service;


/**
 * for create volume and reserveVolume, about the lastRefreshTime and FreeSpace.
 */
public class RefreshTimeAndFreeSpace {

  private static RefreshTimeAndFreeSpace RefreshTimeAndFreeSpace;
  private long lastRefreshTime;
  private long actualFreeSpace;



  public static RefreshTimeAndFreeSpace getInstance() {
    if (RefreshTimeAndFreeSpace == null) {
      synchronized (RefreshTimeAndFreeSpace.class) {
        if (RefreshTimeAndFreeSpace == null) {
          RefreshTimeAndFreeSpace = new RefreshTimeAndFreeSpace();
        }
      }
    }
    return RefreshTimeAndFreeSpace;
  }

  public long getLastRefreshTime() {
    return lastRefreshTime;
  }

  public void setLastRefreshTime(long lastRefreshTime) {
    this.lastRefreshTime = lastRefreshTime;
  }

  public long getActualFreeSpace() {
    return actualFreeSpace;
  }

  public void setActualFreeSpace(long actualFreeSpace) {
    this.actualFreeSpace = actualFreeSpace;
  }

}
