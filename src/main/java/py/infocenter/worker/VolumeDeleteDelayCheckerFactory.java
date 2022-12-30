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
