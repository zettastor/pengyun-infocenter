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
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.io.qos.IoLimitation;
import py.thrift.share.AlreadyExistStaticLimitationExceptionThrift;
import py.thrift.share.DynamicIoLimitationTimeInterleavingExceptionThrift;


public interface DriverStore {

  public List<DriverMetadata> get(long volumeId);

  public List<DriverMetadata> get(long volumeId, int snapshotId);

  public DriverMetadata get(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId);

  List<DriverMetadata> getByDriverContainerId(long driverContainerId);

  public List<DriverMetadata> list();

  public void delete(long volumeId);

  // if isAttached is not 0, means this volume has launched

  public void delete(long driverContainerId, long volumeId, DriverType driverType, int snapshotId);

  public void save(DriverMetadata driverMetadata);

  public void clearMemoryData();

  public int updateIoLimit(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId,
      IoLimitation ioLimitation) throws AlreadyExistStaticLimitationExceptionThrift,
      DynamicIoLimitationTimeInterleavingExceptionThrift;

  public int deleteIoLimit(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId, long limitId);

  public int changeLimitType(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId,
      long limitId, boolean staticLimit);

  public int updateMakeUnmountDriverForCsi(long driverContainerId, long volumeId,
      DriverType driverType, int snapshotId,
      boolean makeUnmountForCsi);
}