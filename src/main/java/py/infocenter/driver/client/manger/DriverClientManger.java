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

package py.infocenter.driver.client.manger;


import com.google.common.collect.Multimap;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.driver.DriverMetadata;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;
import py.infocenter.store.DriverClientStore;
import py.informationcenter.AccessPermissionType;


public class DriverClientManger {

  private static final Logger logger = LoggerFactory.getLogger(DriverClientManger.class);

  private DriverClientStore driverClientStore;
  private long driverClientInfoKeepTime;
  private int driverClientInfoKeepNumber;

  
  public DriverClientManger(DriverClientStore driverClientStore, long driverClientInfoKeepTime,
      int driverClientInfoKeepNumber) {
    this.driverClientStore = driverClientStore;
    this.driverClientInfoKeepTime = driverClientInfoKeepTime;
    this.driverClientInfoKeepNumber = driverClientInfoKeepNumber;
  }

  /**
   * xx.
   *
   * @param reportingDriverMetadata reportingDriverMetadata
   * @param volumeDescription       volumeDescription
   * @return the client in report request, for check the not report client
   */
  public Set<DriverClientKey> checkReportDriverClientInfo(DriverMetadata reportingDriverMetadata,
      String volumeDescription) {
    Set<DriverClientKey> driverClientKeys = new HashSet<>();
    Map<String, AccessPermissionType> clientHostAccessRule = reportingDriverMetadata
        .getClientHostAccessRule();
    for (Map.Entry<String, AccessPermissionType> entry : clientHostAccessRule.entrySet()) {
      String client = entry.getKey();
      //
      DriverClientKey driverClientKey = new DriverClientKey(reportingDriverMetadata, client);
      driverClientKeys.add(driverClientKey);

      DriverClientInformation driverClientInformation = driverClientStore
          .getLastTimeValue(driverClientKey);
      if (driverClientInformation == null) {
        //first report, connect
        driverClientInformation = new DriverClientInformation(driverClientKey,
            System.currentTimeMillis(),
            reportingDriverMetadata.getDriverName(), reportingDriverMetadata.getHostName(), true,
            reportingDriverMetadata.getVolumeName(), volumeDescription);
        logger.warn("in checkReportDriverClientInfo, find the client first connect:{}",
            driverClientInformation);
        driverClientStore.save(driverClientInformation);

      } else {
        //check the last time status, connect again
        if (!driverClientInformation.isStatus()) {
          logger.warn(
              "in checkReportDriverClientInfo, find the client connect again:{}, make it to "
                  + "connect",
              driverClientInformation);
          driverClientInformation = new DriverClientInformation(driverClientKey,
              System.currentTimeMillis(),
              reportingDriverMetadata.getDriverName(), reportingDriverMetadata.getHostName(), true,
              reportingDriverMetadata.getVolumeName(), volumeDescription);
          driverClientStore.save(driverClientInformation);
        }
      }
    }

    return driverClientKeys;
  }

  
  public void checkNotReportDriverClientInfoByDriverContainer(long driverContainerId,
      Set<DriverClientKey> driverClientKeysReport) {
    //all
    Set<DriverClientKey> driverClientKeysNotReport = new HashSet<>();
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    //get each DriverClientKey
    for (DriverClientInformation driverClientInformation : driverClientInformationList) {
      long driverContainerIdTemp = driverClientInformation.getDriverClientKeyInformation()
          .getDriverContainerId();
      if (driverContainerIdTemp != driverContainerId) {
        continue;
      }

      DriverClientKey driverClientKey = new DriverClientKey(
          driverClientInformation.getDriverClientKeyInformation());
      if (driverClientKeysReport.contains(driverClientKey)) {
        continue;
      }

      driverClientKeysNotReport.add(driverClientKey);
    }

    // check
    for (DriverClientKey driverClientKey : driverClientKeysNotReport) {
      DriverClientInformation driverClientInformation = driverClientStore
          .getLastTimeValue(driverClientKey);
      //check the last time status,
      if (driverClientInformation.isStatus()) {
        //make it to dis connect
        driverClientInformation = new DriverClientInformation(driverClientKey,
            System.currentTimeMillis(),
            driverClientInformation.getDriverName(), driverClientInformation.getHostName(), false,
            driverClientInformation.getVolumeName(),
            driverClientInformation.getVolumeDescription());
        driverClientStore.save(driverClientInformation);
        logger.warn("in check not reportDriverClientInfo, find the client dis connect:{}",
            driverClientInformation);
      }
    }
  }

  
  public List<DriverClientInformation> listAllDriverClientInfo() {
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    return driverClientInformationList;
  }

  
  public void removeOldDriverClientInfo() {
    //first remove the time
    long timeMillis = TimeUnit.SECONDS.toMillis(driverClientInfoKeepTime);
    List<DriverClientInformation> driverClientInformationList = driverClientStore.list();
    if (driverClientInformationList != null && !driverClientInformationList.isEmpty()) {
      long currentTime = System.currentTimeMillis();
      Iterator<DriverClientInformation> iterator = driverClientInformationList.iterator();
      while (iterator.hasNext()) {
        DriverClientInformation driverClientInformation = iterator.next();
        if (currentTime - driverClientInformation.getDriverClientKeyInformation().getTime()
            >= timeMillis) {
          logger.warn("time out, remove the client info:{}", driverClientInformation);
          driverClientStore.deleteValue(driverClientInformation);
        }
      }
    }

    //remove the number
    Multimap<DriverClientKey, DriverClientInformation> driverClientInformationMultimap =
        driverClientStore
            .listDriverKey();
    Iterator<DriverClientKey> iterator = driverClientInformationMultimap.keySet().iterator();

    while (iterator.hasNext()) {
      DriverClientKey driverClientKey = iterator.next();
      List<DriverClientInformation> driverClientInformations = new LinkedList<>(
          driverClientInformationMultimap.get(driverClientKey));
      if (driverClientInformations != null && !driverClientInformations.isEmpty()) {
        if (driverClientInformations.size() > driverClientInfoKeepNumber) {
          logger.warn("for driverClient :{}, have :{}, so need delete some", driverClientKey,
              driverClientInformations.size());
          driverClientInformations.sort(new Comparator<DriverClientInformation>() {
            @Override
            public int compare(DriverClientInformation o1, DriverClientInformation o2) {
              return (int) (o1.getDriverClientKeyInformation().getTime() - o2
                  .getDriverClientKeyInformation().getTime());
            }
          });

          //remove
          Iterator<DriverClientInformation> iteratorList = driverClientInformations.iterator();
          while (iteratorList.hasNext()) {
            DriverClientInformation driverClientInformation = iteratorList.next();
            if (driverClientInformations.size() <= driverClientInfoKeepNumber) {
              break;
            }
            logger.warn("have more, so remove the client info:{}", driverClientInformation);
            driverClientStore.deleteValue(driverClientInformation);
            iteratorList.remove();
          }
        }
      }
    }
  }

  public void setDriverClientInfoKeepTime(long driverClientInfoKeepTime) {
    this.driverClientInfoKeepTime = driverClientInfoKeepTime;
  }

  public void setDriverClientInfoKeepNumber(int driverClientInfoKeepNumber) {
    this.driverClientInfoKeepNumber = driverClientInfoKeepNumber;
  }
}
