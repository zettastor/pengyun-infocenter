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

package py.infocenter.instance.manger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import py.infocenter.store.InstanceVolumesInformation;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.VolumeStore;
import py.volume.VolumeMetadata;

public class InstanceIncludeVolumeInfoManger {

  private static final Logger logger = LoggerFactory
      .getLogger(InstanceIncludeVolumeInfoManger.class);
  //      instanceId  allVolumeInfo
  private Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap;

  private InstanceVolumesInformationStore instanceVolumesInformationStore;

  private VolumeStore volumeStore;

  public InstanceIncludeVolumeInfoManger() {
    instanceToVolumeInfoMap = new ConcurrentHashMap<>();
  }

  
  public void initVolumeInfo() {
    try {
      List<InstanceVolumesInformation> instanceVolumesInformationList =
          instanceVolumesInformationStore.reloadAllInstanceVolumesInformationFromDb();
      //clear the old value
      instanceToVolumeInfoMap.clear();
      logger.warn("get the value in instanceVolumesInformation size :{}",
          instanceVolumesInformationList.size());

      for (InstanceVolumesInformation value : instanceVolumesInformationList) {

        long instanceId = value.getInstanceId();
        Set<Long> volumeIds = value.getVolumeIds();
        if (volumeIds == null || volumeIds.isEmpty()) {

          //if this instance not include any volume, remove it in db
          instanceVolumesInformationStore.deleteInstanceVolumesInformationFromDb(instanceId);
          continue;
        }

        InstanceToVolumeInfo instanceToVolumeInfo = new InstanceToVolumeInfo();
        instanceToVolumeInfo.setVolumeInfo(volumeIds);
        instanceToVolumeInfoMap.put(instanceId, instanceToVolumeInfo);
      }

      logger.warn("after the init initVolumeInfo, the value is :{}", instanceToVolumeInfoMap);
    } catch (SQLException | IOException e) {
      logger.error("when init the VolumeInfoMap, find a error:", e);
    }
  }

  public void clearVolumeInfoMap() {
    instanceToVolumeInfoMap.clear();
  }

  public Map<Long, InstanceToVolumeInfo> getInstanceToVolumeInfoMap() {
    return instanceToVolumeInfoMap;
  }

  public void setInstanceToVolumeInfoMap(Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap) {
    this.instanceToVolumeInfoMap = instanceToVolumeInfoMap;
  }

  public void setInstanceVolumesInformationStore(
      InstanceVolumesInformationStore instanceVolumesInformationStore) {
    this.instanceVolumesInformationStore = instanceVolumesInformationStore;
  }

  /**
   * the return value is: this is have one volume in table,but other HA report this volume again so
   * i notify this HA instance remove this volume in memory, just for the Equilibrium error.
   *
   * @param instanceIdWhichReport                    report instance id
   * @param volumeIdList                             the volume from the report instance
   * @param removeVolumeInInfoTableWhenEquilibriumOk the volume which is Equilibrium from the report
   *                                                 instance. if Equilibrium ok,
   */
  public synchronized Set<Long> saveVolumeInfo(long instanceIdWhichReport, List<Long> volumeIdList,
      Map<Long, List<Long>> removeVolumeInInfoTableWhenEquilibriumOk,
      Map<Long, Long> totalSegmentUnitMetadataNumber) {

    //this volume where be remove by report instance
    Set<Long> volumeExistInTable = new HashSet<>();

    if (!removeVolumeInInfoTableWhenEquilibriumOk.isEmpty()) {
      logger
          .warn("this volume in table will be move :{}", removeVolumeInInfoTableWhenEquilibriumOk);
    }

    //delete volume in this instance
    for (Map.Entry<Long, List<Long>> entry : removeVolumeInInfoTableWhenEquilibriumOk.entrySet()) {
      long oldInstanceId = entry.getKey();
      List<Long> volumeIds = entry.getValue();

      for (Long volumeId : volumeIds) {
        removeVolumeInfo(oldInstanceId, volumeId);
      }
    }

    boolean needUpdateToDb = false;
    if (!instanceToVolumeInfoMap.containsKey(instanceIdWhichReport)) {
      InstanceToVolumeInfo instanceToVolumeInfoNew = new InstanceToVolumeInfo();

      instanceToVolumeInfoMap.put(instanceIdWhichReport, instanceToVolumeInfoNew);
      needUpdateToDb = true;
    }

    InstanceToVolumeInfo instanceToVolumeInfo = instanceToVolumeInfoMap.get(instanceIdWhichReport);

    //set last report time
    instanceToVolumeInfo.setLastReportedTime(System.currentTimeMillis());

    /* check current instance, the report volume has in other instance or not, choose by the
     * segmentUnitNumber ***/
    for (Long volumeId : volumeIdList) {
      long instanceIdForChoose = 0;
      long maxSegmentUnitMetadataNumber = 0;
      long volumeInEachInstanceNumber = 0;

      for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
        long currentInstanceId = entry.getKey();
        InstanceToVolumeInfo instanceToVolumeInfoGet = entry.getValue();

        if (instanceToVolumeInfoGet.containsValue(volumeId)) {
          volumeInEachInstanceNumber++;
          long segmentUnitMetadataNumber = instanceToVolumeInfoGet
              .getVolumesegmentUnitNumber(volumeId);

          if (segmentUnitMetadataNumber >= maxSegmentUnitMetadataNumber) {
            maxSegmentUnitMetadataNumber = segmentUnitMetadataNumber;
            instanceIdForChoose = currentInstanceId;
          }
        }
      }

      /* volume first report form instance  **/
      if (volumeInEachInstanceNumber == 0) {
        instanceToVolumeInfo.addVolume(volumeId, totalSegmentUnitMetadataNumber.get(volumeId));
        needUpdateToDb = true;
        continue;
      }

      /* volume normal report form instance ***/
      if (volumeInEachInstanceNumber == 1) {
        if (instanceIdForChoose == instanceIdWhichReport) {
          //normal report, report instance and save instance is ok
        } else {
          /* the volume report instance is not the save instance **/

          //check which instance have more segmentUnit,
          if (maxSegmentUnitMetadataNumber >= totalSegmentUnitMetadataNumber.get(volumeId)) {
            /* remove volume in old instance **/
            InstanceToVolumeInfo instanceToVolumeInfoGet = instanceToVolumeInfoMap
                .get(instanceIdWhichReport);
            if (instanceToVolumeInfoGet != null) {
              instanceToVolumeInfoGet.moveVolume(volumeId);
              logger.warn(
                  "after move, current report instance :{} and it InstanceToVolumeInfo is:{}, the"
                      + " remove volume is :{}",
                  instanceIdWhichReport, instanceToVolumeInfoGet, volumeId);
            }

            //set volume not report next time
            volumeExistInTable.add(volumeId);
          } else {
           
            
            InstanceToVolumeInfo instanceToVolumeInfoGet = instanceToVolumeInfoMap
                .get(instanceIdForChoose);
            if (instanceToVolumeInfoGet != null) {
              instanceToVolumeInfoGet.moveVolume(volumeId);
              logger.warn(
                  "after move, current report instance :{} and i move volume in instance :{}, "

                      + "after move,the old InstanceToVolumeInfo is:{}",
                  instanceIdWhichReport, instanceIdForChoose, instanceToVolumeInfoGet);
            }

            /* save in my table ***/
            instanceToVolumeInfoGet = instanceToVolumeInfoMap.get(instanceIdWhichReport);
            instanceToVolumeInfoGet
                .addVolume(volumeId, totalSegmentUnitMetadataNumber.get(volumeId));
            logger.warn("after add, the report instance :{} and it InstanceToVolumeInfo is:{}",
                instanceIdWhichReport, instanceToVolumeInfoGet);
          }
        }
        continue;
      }

      
      if (volumeInEachInstanceNumber > 1) {
        if (instanceIdForChoose == instanceIdWhichReport) {
          logger.warn(
              "when saveVolumeInfo, find current volume:{},report form :{} has in other instance"

                  + "after choose, set myself to report it", volumeId, instanceIdForChoose);

          
          for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
            long instanceId = entry.getKey();
            InstanceToVolumeInfo instanceToVolumeInfoChange = entry.getValue();

            //remove the volume in table, only hold myself instance
            if (instanceToVolumeInfoChange.containsValue(volumeId)
                && instanceId != instanceIdWhichReport) {
              instanceToVolumeInfoChange.moveVolume(volumeId);
              logger.warn("begin to move the volume :{} in instance :{}", volumeId, instanceId);
            }
          }
        } else {

          logger.warn(
              "when saveVolumeInfo, find current volume:{},report form :{} has in other instance"

                  + "after choose, set :{} to report it, and i not report it next time", volumeId,
              instanceIdWhichReport, instanceIdForChoose);

          
          for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
            long instanceId = entry.getKey();
            InstanceToVolumeInfo instanceToVolumeInfoChange = entry.getValue();

            if (instanceId == instanceIdWhichReport) {
              //remove volume for old instance info
              instanceToVolumeInfoChange.moveVolume(volumeId);
              logger.warn("begin to move the volume :{} in my instance :{}", volumeId, instanceId);
              continue;
            }

            if (instanceId == instanceIdForChoose) {
              //save in new instance info
              instanceToVolumeInfoChange
                  .addVolume(volumeId, totalSegmentUnitMetadataNumber.get(volumeId));
              logger
                  .warn("begin to save the volume :{} in other instance :{}", volumeId, instanceId);
              continue;
            }
          }

          //set volume not report next time
          volumeExistInTable.add(volumeId);
        }

      }
    }

    //update to DB
    if (needUpdateToDb) {
      logger
          .warn("begin to update the instance volume info, the instance id :{}, and the value :{}",
              instanceIdWhichReport, instanceToVolumeInfo);
      saveinstancevolumeinfotodb(instanceIdWhichReport, instanceToVolumeInfo);
    }

    return volumeExistInTable;
  }

  
  public void saveVolumeInfo(long instanceId, Long volumeId) {
    if (!instanceToVolumeInfoMap.containsKey(instanceId)) {
      InstanceToVolumeInfo instanceToVolumeInfoNew = new InstanceToVolumeInfo();
      instanceToVolumeInfoMap.put(instanceId, instanceToVolumeInfoNew);
    }

    InstanceToVolumeInfo instanceToVolumeInfoGet = instanceToVolumeInfoMap.get(instanceId);
    if (instanceToVolumeInfoGet.containsValue(volumeId)) {
      return;
    }

    //init 0
    instanceToVolumeInfoGet.addVolume(volumeId, 0L);
    //update to db
    logger.warn("save Volume Info, the instance id :{}, and the value :{}", instanceId,
        instanceToVolumeInfoGet);
    saveinstancevolumeinfotodb(instanceId, instanceToVolumeInfoGet);

    
    if (instanceToVolumeInfoGet.getLastReportedTime() == 0) {
      instanceToVolumeInfoGet.setLastReportedTime(System.currentTimeMillis());
    }

  }

  
  public boolean isInTable(long volumeId) {
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();
      if (instanceToVolumeInfo.containsValue(volumeId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * instance id, volume id <-> volume size.
   ***/
  public MultiValueMap<Long, VolumesForEquilibrium> getEachHaVolumeSizeInfo() {
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      long instanceId = entry.getKey();
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();

      if (instanceToVolumeInfo == null) {
        continue;
      }

      for (Long volumeId : instanceToVolumeInfo.getVolumeInfo()) {
        VolumesForEquilibrium volumeInfoForEquilibrium = new VolumesForEquilibrium();
        VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
        //just move the volume which status is Available
        if (volumeMetadata == null) {
          continue;
        } else {

          volumeInfoForEquilibrium.setVolumeId(volumeId);
          volumeInfoForEquilibrium.setVolumeSize(volumeMetadata.getVolumeSize());

          //the extend volume is not Equilibrium, just set the status is false,not the 
          // VolumeStatus is Unavailable
          if (volumeMetadata.getExtendingSize() > 0) {
            volumeInfoForEquilibrium.setAvailable(false);
          } else {
            volumeInfoForEquilibrium.setAvailable(volumeMetadata.isVolumeAvailable());
          }

          volumeSizeInfo.add(instanceId, volumeInfoForEquilibrium);
        }
      }

      //if the instance is empty, set default volume
      if (!volumeSizeInfo.containsKey(instanceId)) {
        volumeSizeInfo.add(instanceId, new VolumesForEquilibrium());
      }
    }

    return volumeSizeInfo;
  }

  /**
   * instance id, volume id <-> volume size.
   ***/
  public Map<Long, Map<Long, Long>> getEachHaVolumeSizeInfo_bak() {
    Map<Long, Map<Long, Long>> volumeSizeInfo = new HashMap<>();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      long instanceId = entry.getKey();
      Map<Long, Long> eachVolumeSize = new HashMap<>();
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();

      if (instanceToVolumeInfo == null) {
        continue;
      }

      volumeSizeInfo.put(instanceId, eachVolumeSize);
      for (Long volumeId : instanceToVolumeInfo.getVolumeInfo()) {
        VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
        //just move the volume which status is Available
        if (volumeMetadata == null || !volumeMetadata.isVolumeAvailable()) {
          continue;
        } else {

          long volumeSize = volumeMetadata.getVolumeSize();
          eachVolumeSize.put(volumeId, volumeSize);
        }
      }
    }

    return volumeSizeInfo;
  }

  
  public void saveinstancevolumeinfotodb(long instanceId,
      InstanceToVolumeInfo instanceToVolumeInfoGet) {
    InstanceVolumesInformation instanceVolumesInformation = new InstanceVolumesInformation();
    instanceVolumesInformation.setInstanceId(instanceId);

    instanceVolumesInformation.setVolumeIds(instanceToVolumeInfoGet.getVolumeInfo());

    instanceVolumesInformationStore.saveInstanceVolumesInformationToDb(instanceVolumesInformation);
  }

  
  public synchronized void removeVolumeInfo(long instanceId, Long volumeId) {
    if (instanceToVolumeInfoMap.containsKey(instanceId)) {
      InstanceToVolumeInfo instanceToVolumeInfoGet = instanceToVolumeInfoMap.get(instanceId);
      instanceToVolumeInfoGet.moveVolume(volumeId);

      //update to db
      logger.warn("remove volume :{} in instance :{}, the after instanceToVolumeInfo is :{}",
          volumeId, instanceId, instanceToVolumeInfoGet);
      saveinstancevolumeinfotodb(instanceId, instanceToVolumeInfoGet);
    }
  }

  
  public synchronized void removeInstance(long instanceId) {
    //update to db
    logger.warn("removeInstance, the instance id :{}", instanceId);
    instanceToVolumeInfoMap.remove(instanceId);
    instanceVolumesInformationStore.deleteInstanceVolumesInformationFromDb(instanceId);
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  /**
   * just for debug.
   **/
  public Map printMapValue() {
    Map<Long, List<Long>> longListMap = new HashMap<>();
    for (Map.Entry<Long, InstanceToVolumeInfo> entry : instanceToVolumeInfoMap.entrySet()) {
      long instanceId = entry.getKey();
      InstanceToVolumeInfo instanceToVolumeInfo = entry.getValue();
      List<Long> volumeIdList = new ArrayList<>();
      for (Long id : instanceToVolumeInfo.getVolumeInfo()) {
        volumeIdList.add(id);
      }
      longListMap.put(instanceId, volumeIdList);
    }

    return longListMap;
  }

}
