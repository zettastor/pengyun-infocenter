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

package py.infocenter.authorization;

import static org.junit.Assert.assertTrue;

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
import py.infocenter.job.TaskType;
import py.infocenter.store.TaskRequestInfo;
import py.infocenter.store.TaskStore;
import py.informationcenter.LaunchDriverRequest;
import py.test.TestBase;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TaskStoreTestConfig.class})
public class TaskStoreTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(TaskStoreTest.class);

  @Autowired
  TaskStore taskStore;

  @Before
  public void init() throws Exception {
    taskStore.clearDb();
  }

  @Test
  public void testTaskStore() throws Exception {

    LaunchDriverRequest launchDriverRequest = new LaunchDriverRequest();
    launchDriverRequest.setVolumeId(123);
    launchDriverRequest.setSnapshotId(0);
    launchDriverRequest.setScsiIp("10.0.0.80");

    TaskRequestInfo taskRequestInfo = new TaskRequestInfo();
    taskRequestInfo.setTaskId(1111);
    taskRequestInfo.setTaskType(TaskType.LaunchDriver.name());
    taskRequestInfo.setRequest(launchDriverRequest);
    taskRequestInfo.setTaskCreateTime(System.currentTimeMillis());

    //save
    taskStore.saveTask(taskRequestInfo);

    //get
    TaskRequestInfo taskRequestInfoGet = taskStore.getTaskById(1111);

    //check type
    assertTrue(taskRequestInfoGet.getTaskType().equals(TaskType.LaunchDriver.name()));

    //save two in client
    LaunchDriverRequest launchDriverRequest2 = new LaunchDriverRequest();
    launchDriverRequest2.setVolumeId(223);
    launchDriverRequest2.setSnapshotId(0);
    launchDriverRequest2.setScsiIp("10.0.0.81");

    TaskRequestInfo taskRequestInfo2 = new TaskRequestInfo();
    taskRequestInfo2.setTaskId(2222);
    taskRequestInfo2.setTaskType(TaskType.LaunchDriver.name());
    taskRequestInfo2.setRequest(launchDriverRequest2);
    taskRequestInfo2.setTaskCreateTime(System.currentTimeMillis());
    taskStore.saveTask(taskRequestInfo2);

    //list not set limit
    List<TaskRequestInfo> taskRequestInfoList = taskStore.listAllTask(0);
    assertTrue(taskRequestInfoList.size() == 2);
    logger.warn("get the value :{}", taskRequestInfoList);

    //list  set limit
    List<TaskRequestInfo> taskRequestInfoList2 = taskStore.listAllTask(1);
    assertTrue(taskRequestInfoList2.size() == 1);
    TaskRequestInfo taskRequestInfoGet2 = taskRequestInfoList2.get(0);
    LaunchDriverRequest launchDriverRequestCheck = (LaunchDriverRequest) taskRequestInfoGet2
        .getRequest();
    assertTrue(launchDriverRequestCheck.getScsiIp().equals("10.0.0.80"));
    assertTrue(launchDriverRequestCheck.getVolumeId() == 123);
    logger.warn("get the value :{}", taskRequestInfoList2);

    //delete
    taskStore.deleteTaskById(2222);

    //list
    List<TaskRequestInfo> taskRequestInfoList3 = taskStore.listAllTask(0);
    assertTrue(taskRequestInfoList3.size() == 1);
  }

  @After
  public void clean() throws Exception {
    taskStore.clearDb();
  }

}
