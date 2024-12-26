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
