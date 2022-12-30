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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.instance.InstanceStatus;
import py.periodic.Worker;

/**
 * check the ha instance is still live or not by check ha instance last report time.
 */
public class HaInstanceCheckSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(HaInstanceCheckSweeper.class);
  private VolumeInformationManger volumeInformationManger;
  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;
  private int instanceTimeOutCheck;
  private AppContext appContext;
  //    instanceId  down number count
  private Map<Long, Integer> countInstanceNotReportNumber = new ConcurrentHashMap<>();


  @Override
  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("check instance, only the master do it");
      return;
    }

    /* check the instance ok or not ***/
    Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap =
        instanceIncludeVolumeInfoManger.getInstanceToVolumeInfoMap();

    logger.info("check instance, i am working, the instance table :{}", instanceToVolumeInfoMap);
    //get the down instance
    Set<Long> downInstance = new HashSet<>();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      long instanceId = entry.getKey();
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();

      long lastReportTime = instanceToVolumeInfo.getLastReportedTime();

      /* which load for db, if the value still is "0", mean this instance not report
       * if current instance because to master, find some instance(which for db) 10s not report
       * volume info
       * so in think this instance is down
       * eg:master down, one follower become to master,load instance info from db,
       * the instance info also include the old master,bu it down, so i remove it
       */
      if (lastReportTime == 0) {
        if (countInstanceNotReportNumber.containsKey(instanceId)) {
          int count = countInstanceNotReportNumber.get(instanceId);
          count++;
          countInstanceNotReportNumber.put(instanceId, count);
        } else {
          //init
          countInstanceNotReportNumber.put(instanceId, 1);
        }

        // check the instance id down or not, the instance which form db
        if (countInstanceNotReportNumber.get(instanceId) > instanceTimeOutCheck) {
          countInstanceNotReportNumber.remove(instanceId);
          downInstance.add(instanceId);
          logger.warn("find a instance :{} which form db, but it is down", instanceId);
        }

      } else {
        /* this instance report, but next time is down, so checkout the instance is down or not
         * in instanceTimeOutCheck time
         *  time out
         *****/

        long currentTime = System.currentTimeMillis();
        if (currentTime - instanceToVolumeInfo.getLastReportedTime() > TimeUnit.SECONDS
            .toMillis(instanceTimeOutCheck)) {
          downInstance.add(instanceId);
        }
      }
    }

    //can not find the down instance
    boolean findDownInstance = false;
    if (downInstance.isEmpty()) {
      return;
    } else {
      logger.warn("find some instance:{} is down, so i remove it in haInstanceManger table",
          downInstance);
      Iterator<Long> iterator = instanceToVolumeInfoMap.keySet().iterator();
      while (iterator.hasNext()) {
        long instanceId = iterator.next();
        if (downInstance.contains(instanceId)) {
          //delete the down instance
          logger.warn("begin to move the instance :{} in haInstanceManger table", instanceId);
          instanceIncludeVolumeInfoManger.removeInstance(instanceId);

          //remove the instance in Equilibrium table
          Map<Long, Map<Long, Long>> volumeReportToInstanceEquilibrium =
              instanceVolumeInEquilibriumManger.getVolumeReportToInstanceEquilibrium();
          if (!volumeReportToInstanceEquilibrium.isEmpty()) {
            instanceVolumeInEquilibriumManger.removeInstanceInEachTable(instanceId);
          }

          findDownInstance = true;
        }
      }
    }

    //update the all instance in ha Instance Manger
    if (findDownInstance) {
      volumeInformationManger.updateToHaInstanceManger();
    }

    
   

  }

  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }

  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }

  public int getInstanceTimeOutCheck() {
    return instanceTimeOutCheck;
  }

  public void setInstanceTimeOutCheck(int instanceTimeOutCheck) {
    this.instanceTimeOutCheck = instanceTimeOutCheck;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public InstanceIncludeVolumeInfoManger getInstanceIncludeVolumeInfoManger() {
    return instanceIncludeVolumeInfoManger;
  }

  public void setInstanceIncludeVolumeInfoManger(
      InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger) {
    this.instanceIncludeVolumeInfoManger = instanceIncludeVolumeInfoManger;
  }

  public void setInstanceVolumeInEquilibriumManger(
      InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger) {
    this.instanceVolumeInEquilibriumManger = instanceVolumeInEquilibriumManger;
  }
}
