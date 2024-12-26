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
