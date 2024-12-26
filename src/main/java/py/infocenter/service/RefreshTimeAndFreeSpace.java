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
