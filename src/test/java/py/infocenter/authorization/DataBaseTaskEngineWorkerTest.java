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

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mockito.Mock;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.engine.DataBaseTaskEngineWorker;
import py.infocenter.job.TaskType;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.TaskRequestInfo;
import py.infocenter.store.TaskStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.LaunchDriverRequest;
import py.instance.InstanceStatus;
import py.test.TestBase;

public class DataBaseTaskEngineWorkerTest extends TestBase {


  @Mock
  TaskStore taskStore;
  @Mock
  VolumeStore volumeStore;
  private InformationCenterImpl informationCenter;
  @Mock
  private InfoCenterAppContext appContext;

  @Test
  public void test() throws Exception {
    informationCenter = new InformationCenterImpl();
    informationCenter.setVolumeStore(volumeStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setTaskStore(taskStore);

    final List<TaskRequestInfo> taskRequestInfoList = new ArrayList<>();
    LaunchDriverRequest launchDriverRequest = new LaunchDriverRequest();
    launchDriverRequest.setVolumeId(123);
    launchDriverRequest.setSnapshotId(0);
    launchDriverRequest.setScsiIp("10.0.0.80");

    TaskRequestInfo taskRequestInfo = new TaskRequestInfo();
    taskRequestInfo.setTaskId(1111);
    taskRequestInfo.setTaskType(TaskType.LaunchDriver.name());
    taskRequestInfo.setRequest(launchDriverRequest);
    taskRequestInfo.setTaskCreateTime(System.currentTimeMillis());
    taskRequestInfoList.add(taskRequestInfo);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(taskStore.listAllTask(0)).thenReturn(taskRequestInfoList);

    DataBaseTaskEngineWorker dataBaseTaskEngineWorker = new DataBaseTaskEngineWorker(5, 10,
        1, informationCenter);
    dataBaseTaskEngineWorker.doWork();
  }
}
