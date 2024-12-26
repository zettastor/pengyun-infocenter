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

package py.infocenter.authorization;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.driver.ScsiDriverDescription;
import py.icshare.ScsiClientStore;
import py.infocenter.job.TaskType;
import py.informationcenter.ScsiClient;
import py.informationcenter.ScsiClientInfo;
import py.test.TestBase;
import py.thrift.share.ScsiDeviceStatusThrift;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ScsiClientTestConfig.class})
public class ScsiClientTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ScsiClientTest.class);

  @Autowired
  ScsiClientStore scsiClientStore;

  @Before
  public void init() throws IOException, SQLException {
    scsiClientStore.clearDb();
  }

  @Test
  public void testCheck() {
    List<ScsiClient> scsiClientList = scsiClientStore.getScsiClientByIp("10.0.0.80");
    logger.warn("get the value :{}", scsiClientList);
  }

  @Test
  public void testScsiClientInfo() throws IOException, SQLException {
    //save
    ScsiClient scsiClient = new ScsiClient();

    ScsiClientInfo scsiClientInfo = new ScsiClientInfo();
    scsiClientInfo.setIpName("10.0.0.80");
    scsiClient.setScsiClientInfo(scsiClientInfo);
    scsiClientStore.saveOrUpdateScsiClientInfo(scsiClient);

    //get
    List<ScsiClient> scsiClientGet = scsiClientStore.getScsiClientByIp("10.0.0.80");

    //check
    assertTrue(scsiClientGet.size() == 1);
    for (ScsiClient value : scsiClientGet) {
      assertTrue(value.getScsiClientInfo().getIpName().equals("10.0.0.80"));
      assertTrue(value.getScsiClientInfo().getVolumeId() == 0);
    }

    //save volume in client
    ScsiClientInfo scsiClientInfo2 = new ScsiClientInfo();
    scsiClientInfo2.setVolumeId(123456);
    scsiClientInfo2.setIpName("10.0.0.80");
    scsiClientInfo2.setSnapshotId(0);
    scsiClient.setScsiClientInfo(scsiClientInfo2);
    scsiClient.setDriverContainerId(1111);
    scsiClient.setScsiDriverStatus(ScsiDeviceStatusThrift.UNKNOWN.name());

    scsiClientStore.saveOrUpdateScsiClientInfo(scsiClient);

    //list
    List<ScsiClient> scsiClientList = scsiClientStore.listAllScsiClients();
    assertTrue(scsiClientList.size() == 2);
    logger.warn("get the value :{}", scsiClientList);

    //update Description
    scsiClientStore
        .updateScsiDriverDescription("10.0.0.80", 123456, 0, ScsiDriverDescription.Normal.name());

    //update DriverStatus
    scsiClientStore
        .updateScsiDriverStatus("10.0.0.80", 123456, 0, ScsiDeviceStatusThrift.LAUNCHING.name());

    //update driverContainerId
    scsiClientStore.updateScsiPydDriverContainerId("10.0.0.80", 123456, 0, 2222);

    //get
    ScsiClient get = scsiClientStore
        .getScsiClientInfoByVolumeIdAndSnapshotId("10.0.0.80", 123456, 0);
    assertTrue(get.getDriverContainerId() == 2222);
    assertTrue(get.getStatusDescription().equals(ScsiDriverDescription.Normal.name()));
    assertTrue(get.getScsiDriverStatus().equals(ScsiDeviceStatusThrift.LAUNCHING.name()));

    //update all
    scsiClientStore.updateScsiDriverStatusAndDescription("10.0.0.80", 123456, 0,
        ScsiDeviceStatusThrift.UMOUNT.name(), ScsiDriverDescription.Error.name(),
        TaskType.LaunchDriver.name());

    //get again
    ScsiClient get2 = scsiClientStore
        .getScsiClientInfoByVolumeIdAndSnapshotId("10.0.0.80", 123456, 0);
    assertTrue(get2.getDriverContainerId() == 2222);
    assertTrue(get2.getStatusDescription().equals(ScsiDriverDescription.Error.name()));
    assertTrue(get2.getScsiDriverStatus().equals(ScsiDeviceStatusThrift.UMOUNT.name()));
    assertTrue(get2.getDescriptionType().equals(TaskType.LaunchDriver.name()));

    //delete
    scsiClientStore.deleteScsiClientInfoByVolumeIdAndSnapshotId("10.0.0.80", 123456, 0);

    //list
    List<ScsiClient> scsiClientList2 = scsiClientStore.listAllScsiClients();
    assertTrue(scsiClientList2.size() == 1);

    //delete
    scsiClientStore.deleteScsiClientByIp("10.0.0.80");
    //list
    List<ScsiClient> scsiClientList3 = scsiClientStore.listAllScsiClients();
    assertTrue(scsiClientList3.isEmpty());
  }

  @After
  public void clean() throws IOException, SQLException {
    scsiClientStore.clearDb();
  }

}
