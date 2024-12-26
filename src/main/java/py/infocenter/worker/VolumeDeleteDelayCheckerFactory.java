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

import py.app.context.AppContext;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.volume.recycle.VolumeRecycleManager;
import py.periodic.Worker;
import py.periodic.WorkerFactory;


public class VolumeDeleteDelayCheckerFactory implements WorkerFactory {

  private long recycleDeleteTimeSecond;
  private VolumeDelayStore volumeDelayStore;
  private VolumeRecycleStore volumeRecycleStore;
  private AppContext appContext;
  private VolumeDeleteDelayChecker volumeDeleteDelayChecker;
  private VolumeRecycleManager volumeRecycleManager;
  private InformationCenterImpl informationCenter;
  private long recycleKeepTimeSecond;

  @Override
  public Worker createWorker() {

    if (volumeDeleteDelayChecker == null) {
      volumeDeleteDelayChecker = new VolumeDeleteDelayChecker(recycleKeepTimeSecond,
          recycleDeleteTimeSecond,
          volumeDelayStore, volumeRecycleStore, appContext, volumeRecycleManager);
      volumeDeleteDelayChecker.setInformationCenter(informationCenter);
    }
    return volumeDeleteDelayChecker;
  }

  public void setRecycleDeleteTimeSecond(long recycleDeleteTimeSecond) {
    this.recycleDeleteTimeSecond = recycleDeleteTimeSecond;
  }

  public void setVolumeDelayStore(VolumeDelayStore volumeDelayStore) {
    this.volumeDelayStore = volumeDelayStore;
  }

  public void setVolumeRecycleStore(VolumeRecycleStore volumeRecycleStore) {
    this.volumeRecycleStore = volumeRecycleStore;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setVolumeRecycleManager(VolumeRecycleManager volumeRecycleManager) {
    this.volumeRecycleManager = volumeRecycleManager;
  }

  public void setInformationCenter(InformationCenterImpl informationCenter) {
    this.informationCenter = informationCenter;
  }

  public void setRecycleKeepTimeSecond(long recycleKeepTimeSecond) {
    this.recycleKeepTimeSecond = recycleKeepTimeSecond;
  }
}
