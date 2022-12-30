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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.PyService;
import py.driver.DriverMetadata;
import py.driver.DriverStateEvent;
import py.driver.DriverStatus;
import py.icshare.DomainStore;
import py.icshare.ScsiDriverMetadata;
import py.infocenter.store.DriverStore;
import py.infocenter.store.ScsiDriverStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.AccessPermissionType;
import py.instance.InstanceStatus;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.periodic.Worker;
import py.querylog.eventdatautil.EventDataWorker;
import py.thrift.share.ScsiDeviceStatusThrift;
import py.volume.VolumeMetadata;

/**
 * If driver container is not available for some reason, the drivers cannot be confirmed in info
 * center. Therefore it is necessary to create a worker to sweep the driver table at fixed time in
 * info center to change the driver status {@link DriverStatus} that after sometime has not been
 * reported from driver container to info center.
 */
public class DriverStoreSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(DriverStoreSweeper.class);

  private AppContext appContext;
  private DriverStore driverStore;
  private ScsiDriverStore scsiDriverStore;
  private DomainStore domainStore;
  private VolumeStore volumeStore;
  private int timeToRemove;
  private int updateTime = 5;
  private int driverUmountTag = 99;
  private Map<Integer, DriverStatus> lastDriverStatusMap = new HashMap<Integer, DriverStatus>();
  private long timeout;

  @Override
  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("DriverStoreSweeper, only the master do it");
      if (appContext.getStatus() == InstanceStatus.SUSPEND) {
        // delete memory data
        logger.warn("++clear all driver:{}", driverStore);
        driverStore.clearMemoryData();

        domainStore.clearMemoryMap();
      }

      return;
    }
    logger.info("DriverStoreSweeper begin to work");

    if (1 == (updateTime++ / 5)) {
      checkDriverStatus();

      updateTime = 0;
    }




    checkDriverStatusAndConnectForCsi();

  }

  private void checkDriverStatus() {
    synchronized (driverStore) {
      final long currentTime = System.currentTimeMillis();
      for (DriverMetadata driver : driverStore.list()) {
        try {
          if (currentTime - driver.getLastReportTime() > timeout) {
            logger.warn("Timeout for driver container reporting new driver to update old driver {}",
                driver);
            driver.setDriverStatus(
                driver.getDriverStatus().turnToNextStatusOnEvent(DriverStateEvent.REPORTTIMEOUT));
            driverStore.save(driver);
          }
        } catch (Exception e) {
          logger.error("Caught an error:", e);
        }


      }

      //check the scsi
      for (ScsiDriverMetadata scsiDriverMetadata : scsiDriverStore.list()) {
        try {
          if (currentTime - scsiDriverMetadata.getLastReportTime() > timeout) {
            logger.warn(
                "Timeout for driver container reporting new scsiDriverMetadata to update old "
                    + "driver {}",
                scsiDriverMetadata);
            scsiDriverMetadata.setScsiDeviceStatus(ScsiDeviceStatusThrift.UNKNOWN.name());
            scsiDriverStore.save(scsiDriverMetadata);
          }
        } catch (Exception e) {
          logger.error("Caught an error:", e);
        }
      }
    }
  }

  private void checkVolumeDriverClientInfo() {
    //check driver client info
    Map<Long, Boolean> volumeDriverClientInfo = new HashMap<>();
    List<DriverMetadata> driverMetadataList = driverStore.list();
    List<ScsiDriverMetadata> scsiDriverMetadataList = scsiDriverStore.list();

    for (DriverMetadata driverMetadata : driverMetadataList) {
      long volumeId = driverMetadata.getVolumeId();
      if (volumeDriverClientInfo.containsKey(volumeId) && volumeDriverClientInfo.get(volumeId)) {
        //find the volume have client connect
        continue;
      } else {
        //need check
        Map<String, AccessPermissionType> clientHostAccessRule = driverMetadata
            .getClientHostAccessRule();
        //have client connect
        if (clientHostAccessRule != null && !clientHostAccessRule.isEmpty()) {
          volumeDriverClientInfo.put(volumeId, true);
        } else {
          volumeDriverClientInfo.put(volumeId, false);
        }
      }
    }

    //update volume info
    logger.warn("checkVolumeDriverClientInfo, get the volumeDriverClientInfo:{}",
        volumeDriverClientInfo);
    List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumes();
    long currentTime = System.currentTimeMillis();

    /* clientLastConnectTime value：
     *  -1 default, not any connect before
     *  > 0 have connect, the connect time
     *  < 0, disconnect, the disconnect time
     */
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      //check have client connect history
      long volumeId = volumeMetadata.getVolumeId();
      //have client connect this time
      if (volumeDriverClientInfo.containsKey(volumeId) && volumeDriverClientInfo.get(volumeId)) {
        //check history connect info
        if (volumeMetadata.getClientLastConnectTime() <= 0) {
          //have client connect, so set value
          volumeStore.updateClientLastConnectTime(volumeId, currentTime);
          logger.warn("check the volume :{} have client connect, update it :{}", volumeId,
              currentTime);
        } else {
          logger.info("check the volume :{} have client connect, not need update", volumeId);
        }
      } else {
        //not client connect this time
        if (volumeMetadata.getClientLastConnectTime() > 0) {
          //update time
          volumeStore.updateClientLastConnectTime(volumeId, currentTime * (-1));
          logger.warn("check the volume :{} not client connect, update it to: {}", volumeId,
              currentTime * (-1));
        } else {
          logger.info("check the volume :{} not client connect, the value:{}, not need update",
              volumeId, volumeMetadata.getClientLastConnectTime());
        }
      }

    }
  }

  private void checkDriverStatusAndConnectForCsi() {
    //update volume info
    List<VolumeMetadata> volumeMetadataList = volumeStore.listVolumes();
    for (VolumeMetadata volumeMetadata : volumeMetadataList) {
      long volumeId = volumeMetadata.getVolumeId();

      long clientLastConnectTime = volumeMetadata.getClientLastConnectTime();
      if (clientLastConnectTime == -1) {
        logger.info(
            "check the driver, for volume :{}, there is not any driver connect, not any alarm",
            volumeId);
        continue;
      }

      List<DriverMetadata> driverMetadataList = driverStore.get(volumeId);
      if (driverMetadataList.isEmpty()) {
        logger.info("check the driver, for volume :{}, there is not any driver, clear alarm",
            volumeId);
        generateVolumeDriverInfo(CounterName.DRIVER_CSI_OK_NUMBER, volumeMetadata,
            new ArrayList<>(), driverUmountTag);
        generateVolumeDriverInfo(CounterName.DRIVER_CSI_CONNECT, volumeMetadata, new ArrayList<>(),
            driverUmountTag);
        continue;
      }

      //check ok driver count
      int goodDriverCount = 0;
      for (DriverMetadata driverMetadata : driverMetadataList) {
        DriverStatus driverStatus = driverMetadata.getDriverStatus();

        if (DriverStatus.LAUNCHED.equals(driverStatus)) {
          goodDriverCount++;
        } else {
          logger.info("check the driver :{} for volume :{}, find the status:{}",
              driverMetadata.getDriverName(), volumeId, driverStatus);
        }
      }
      generateVolumeDriverInfo(CounterName.DRIVER_CSI_OK_NUMBER, volumeMetadata, driverMetadataList,
          goodDriverCount);

      //check all driverStatus
      Map<String, Set<String>> clientInfo = new HashMap<>();
      Map<String, DriverMetadata> driverMetadataConnectFailedHashMap = new HashMap<>();

      //get info
      int goodConnectCount = 0;
      for (DriverMetadata driverMetadata : driverMetadataList) {
        DriverStatus driverStatus = driverMetadata.getDriverStatus();
        String driverName = driverMetadata.getDriverName();
        Map<String, AccessPermissionType> clientHostAccessRule = driverMetadata
            .getClientHostAccessRule();
        //DriverMetadata A 10.0.0.80 10.0.0.81
        //DriverMetadata B 10.0.0.80 10.0.0.82

        if (clientHostAccessRule != null && !clientHostAccessRule.isEmpty()) {
          logger.info(
              "check the driver :{} for volume :{}, the status:{}, have the client :{}, some not "
                  + "good",
              driverMetadata.getDriverName(), volumeId, driverStatus, clientHostAccessRule);
          clientInfo.put(driverName, clientHostAccessRule.keySet());
        } else {
          logger.info("check the driver :{} for volume :{}, the status:{}, not find clientInfo",
              driverMetadata.getDriverName(), volumeId, driverStatus);
        }
      }

      //all Driver have connect, check the each client connect info
      //DriverMetadata A 10.0.0.80 10.0.0.81
      //DriverMetadata B 10.0.0.80 10.0.0.82
      Set<String> clientInfoAll = new HashSet<>();
      for (Map.Entry<String, Set<String>> entry : clientInfo.entrySet()) {
        clientInfoAll.addAll(entry.getValue());
      }

      if (clientInfoAll.isEmpty()) {
        logger.info("check the driver for volume :{}, can not find any client info", volumeId);
        generateVolumeDriverInfo(CounterName.DRIVER_CSI_CONNECT, volumeMetadata, driverMetadataList,
            0);
      } else {
        for (String client : clientInfoAll) {
          //check client in each driverMetadata
          for (DriverMetadata driverMetadata : driverMetadataList) {
            Map<String, AccessPermissionType> clientHostAccessRule = driverMetadata
                .getClientHostAccessRule();
            Set<String> clientInfoTemp = clientHostAccessRule.keySet();
            //
            if (!clientInfoTemp.contains(client)) {
              logger.info(
                  "check the driver :{} for volume :{}, the client :{}, not in this driver client"
                      + " info:{}",
                  driverMetadata.getDriverName(), volumeId, client, clientHostAccessRule);
              driverMetadataConnectFailedHashMap
                  .put(driverMetadata.getDriverName(), driverMetadata);
            }
          }
        }

        //get the ok connect
        for (DriverMetadata driverMetadata : driverMetadataList) {
          String driverName = driverMetadata.getDriverName();
          if (!driverMetadataConnectFailedHashMap.containsKey(driverName)) {
            goodConnectCount++;
          }
        }
        generateVolumeDriverInfo(CounterName.DRIVER_CSI_CONNECT, volumeMetadata, driverMetadataList,
            goodConnectCount);


      }
    }
  }

  private void generateVolumeDriverInfo(CounterName counterName, VolumeMetadata volumeMetadata,
      List<DriverMetadata> driverMetadataList, long value) {
    Map<String, String> userDefineParams = new HashMap<>();
    userDefineParams
        .put(UserDefineName.VolumeID.name(), String.valueOf(volumeMetadata.getVolumeId()));
    userDefineParams.put(UserDefineName.VolumeName.name(), volumeMetadata.getName());
    userDefineParams
        .put(UserDefineName.VolumeDescription.name(), volumeMetadata.getVolumeDescription());

    JSONArray jsonRet = new JSONArray();
    for (DriverMetadata driverMetadata : driverMetadataList) {
      Set clientInfo = new HashSet();
      String driverName = driverMetadata.getDriverName();
      DriverStatus driverStatus = driverMetadata.getDriverStatus();
      if (driverMetadata.getClientHostAccessRule() != null && !driverMetadata
          .getClientHostAccessRule().isEmpty()) {
        clientInfo = driverMetadata.getClientHostAccessRule().keySet();
      }

      //construct a dest result
      JSONObject destJson = new JSONObject();
      destJson.put(UserDefineName.DriverName.name(), driverName);
      destJson.put(UserDefineName.DriverStatus.name(), driverStatus.name());
      destJson.put(UserDefineName.ClientInfo.name(), clientInfo.toString());
      jsonRet.add(destJson);
    }
    userDefineParams.put(UserDefineName.DriverInfo.name(), jsonRet.toString());
    logger.info("for volume :{}, the driver userDefineParams:{}, the value:{}",
        volumeMetadata.getVolumeId(), userDefineParams, value);

    EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER, userDefineParams);
    Map<String, Long> counters = new HashMap<>();
    // 0: dis alarm, 100: alarm
    counters.put(counterName.name(), value);
    eventDataWorker.work(OperationName.Driver.name(), counters);
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public ScsiDriverStore getScsiDriverStore() {
    return scsiDriverStore;
  }

  public void setScsiDriverStore(ScsiDriverStore scsiDriverStore) {
    this.scsiDriverStore = scsiDriverStore;
  }

  public DriverStore getDriverStore() {
    return driverStore;
  }

  public void setDriverStore(DriverStore driverStore) {
    this.driverStore = driverStore;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }
}
