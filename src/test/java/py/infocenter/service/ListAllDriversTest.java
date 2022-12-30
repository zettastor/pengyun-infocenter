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

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.store.DriverStoreImpl;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.icshare.ListAllDriversRequest;
import py.thrift.icshare.ListAllDriversResponse;
import py.thrift.share.DriverTypeThrift;

/**
 * The class use to test get drivers from driverStore by different request parameters. Created by
 * wangjing on 17-12-12.
 */
public class ListAllDriversTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ListAllDriversTest.class);
  private InformationCenterImpl icImpl;
  @Mock
  private DriverStoreImpl driverStore;
  @Mock
  private SessionFactory sessionFactory;
  @Mock
  private VolumeStore volumeStore;
  @Mock
  private InfoCenterAppContext appContext;


  @Before
  public void init() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    icImpl = new InformationCenterImpl();
    icImpl.setAppContext(appContext);
    icImpl.setVolumeStore(volumeStore);
    driverStore.setSessionFactory(sessionFactory);
    List<DriverMetadata> driverMetadataList = new ArrayList<>();
    driverMetadataList
        .add(buildUniqueDriver(11111, 1, "192.168.2.101", "172.16.2.101", DriverType.ISCSI));
    driverMetadataList
        .add(buildUniqueDriver(11111, 2, "192.168.2.101", "172.16.2.101", DriverType.NBD));
    driverMetadataList
        .add(buildUniqueDriver(33333, 2, "192.168.2.103", "172.16.2.103", DriverType.ISCSI));
    driverMetadataList
        .add(buildUniqueDriver(44444, 2, "192.168.2.104", "172.16.2.104", DriverType.NBD));
    driverMetadataList
        .add(buildUniqueDriver(55555, 3, "192.168.2.105", "172.16.2.105", DriverType.ISCSI));
    driverMetadataList
        .add(buildUniqueDriver(66666, 3, "192.168.2.105", "172.16.2.105", DriverType.ISCSI));
    icImpl.setDriverStore(driverStore);
    when(driverStore.list()).thenReturn(driverMetadataList);
    super.init();
  }

  /**
   * If request not set any condition ,it will get all drivers from driverstore.
   */
  @Test
  public void listAllDriverWith() throws TException {
    ListAllDriversRequest request = new ListAllDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    ListAllDriversResponse response = icImpl.listAllDrivers(request);
    Assert.assertTrue(response.getDriverMetadatasthrift().size() == 6);
  }

  /**
   * Set volumeId for request.
   */
  @Test
  public void volumeSetTest() throws TException {
    ListAllDriversRequest request = new ListAllDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(11111);
    ListAllDriversResponse response = icImpl.listAllDrivers(request);
    Assert.assertTrue(response.getDriverMetadatasthrift().size() == 2);

  }

  /**
   * Set volumeId and snapshotId fro request.
   */
  @Test
  public void volumeAndSnapshotIdTest() throws TException {
    ListAllDriversRequest request = new ListAllDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(11111);
    request.setSnapshotId(2);
    ListAllDriversResponse response = icImpl.listAllDrivers(request);
    Assert.assertTrue(response.getDriverMetadatasthrift().size() == 1);

  }


  @Test
  public void snapshotIdAndDriverTypeSetTest() throws TException {
    ListAllDriversRequest request = new ListAllDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setSnapshotId(2);
    request.setDriverType(DriverTypeThrift.NBD);
    ListAllDriversResponse response = icImpl.listAllDrivers(request);
    Assert.assertTrue(response.getDriverMetadatasthrift().size() == 2);
  }


  @Test
  public void snapshotIdAndDriverHostAndDriverTypeSet() throws TException {
    ListAllDriversRequest request = new ListAllDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setSnapshotId(3);
    request.setDriverHost("172.16.2.105");
    request.setDriverType(DriverTypeThrift.NBD);
    Assert.assertTrue(icImpl.listAllDrivers(request).getDriverMetadatasthrift().size() == 0);

    request.setDriverType(DriverTypeThrift.ISCSI);
    Assert.assertTrue(icImpl.listAllDrivers(request).getDriverMetadatasthrift().size() == 2);
  }


  private DriverMetadata buildUniqueDriver(long volumeId, int snapshotId, String dcHost,
      String driverHost, DriverType driverType) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverStatus(DriverStatus.LAUNCHED);
    driver.setProcessId(Integer.MAX_VALUE);
    driver.setVolumeId(volumeId);
    driver.setSnapshotId(snapshotId);
    driver.setQueryServerIp(dcHost);
    driver.setHostName(driverHost);
    driver.setDriverType(driverType);
    return driver;
  }
}
