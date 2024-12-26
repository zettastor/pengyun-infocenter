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

package py.infocenter.worker;

import py.infocenter.InformationCenterAppConfig;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class GetRebalanceTaskSweeperFactory implements WorkerFactory {

  private static GetRebalanceTaskSweeper worker;
  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private VolumeInformationManger volumeInformationManger;
  private VolumeStore volumeStore;
  private InstanceStore instanceStore;
  private InformationCenterAppConfig informationCenterAppConfig;



  public GetRebalanceTaskSweeperFactory(
      SegmentUnitsDistributionManager segmentUnitsDistributionManager,
      VolumeInformationManger volumeInformationManger, VolumeStore volumeStore,
      InstanceStore instanceStore, InformationCenterAppConfig informationCenterAppConfig) {
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
    this.volumeInformationManger = volumeInformationManger;
    this.volumeStore = volumeStore;
    this.instanceStore = instanceStore;
    this.informationCenterAppConfig = informationCenterAppConfig;
  }

  public static GetRebalanceTaskSweeper getWorker() {
    return worker;
  }

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new GetRebalanceTaskSweeper(segmentUnitsDistributionManager, volumeInformationManger,
          volumeStore, instanceStore, informationCenterAppConfig);
    }
    return worker;
  }

}
