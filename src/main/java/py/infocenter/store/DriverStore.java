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