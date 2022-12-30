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
