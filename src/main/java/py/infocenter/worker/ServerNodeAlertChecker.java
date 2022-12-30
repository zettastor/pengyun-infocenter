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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.PyService;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceInformation;
import py.icshare.ServerNode;
import py.infocenter.store.ServerNodeStore;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.periodic.Worker;
import py.querylog.eventdatautil.EventDataWorker;



public class ServerNodeAlertChecker implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(ServerNodeAlertChecker.class);

  private Map<String, Long> serverNodeReportTimeMap;
  private int serverNodeReportOverTimeSecond;
  private InstanceStore instanceStore;
  private ServerNodeStore serverNodeStore;
  private InstanceMaintenanceDbStore instanceMaintenanceDbStore;
  private AppContext appContext;
  private Map<String, Boolean> hasGenerateAlarmBefore;



  public ServerNodeAlertChecker(Map<String, Long> serverNodeReportTimeMap,
      int serverNodeReportOverTimeSecond,
      InstanceStore instanceStore, ServerNodeStore serverNodeStore,
      InstanceMaintenanceDbStore instanceMaintenanceDbStore,
      AppContext appContext) {
    this.serverNodeReportTimeMap = serverNodeReportTimeMap;
    this.serverNodeReportOverTimeSecond = serverNodeReportOverTimeSecond;
    this.instanceStore = instanceStore;
    this.serverNodeStore = serverNodeStore;
    this.instanceMaintenanceDbStore = instanceMaintenanceDbStore;
    this.appContext = appContext;
    this.hasGenerateAlarmBefore = new HashMap<>();
  }

  @Override
  public void doWork() throws Exception {
    logger.debug("begin serverNodeAlertChecker, serverNodeReportTimeMap: {}",
        serverNodeReportTimeMap);
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("only the master can do it");
      return;
    }

    List<String> list = new ArrayList<>();

    Set<Instance> dihInstancesTemp = instanceStore
        .getAll(PyService.DIH.getServiceName(), InstanceStatus.HEALTHY);

    for (Instance instance : dihInstancesTemp) {

      ServerNode serverNodeFromStore = serverNodeStore
          .getServerNodeByIp(instance.getEndPoint().getHostName());

      if (serverNodeFromStore != null) {
        serverNodeReportTimeMap.put(serverNodeFromStore.getId(), System.currentTimeMillis());
        if (!serverNodeFromStore.getStatus().contains("ok")) {
          serverNodeFromStore.setStatus("ok");
          serverNodeStore.updateServerNode(serverNodeFromStore);
        }
      }
    }

    for (Map.Entry<String, Long> entry : serverNodeReportTimeMap.entrySet()) {
      String serverId = entry.getKey();
      Long lastReportTime = entry.getValue();
      ServerNode serverNode = serverNodeStore.listServerNodeById(serverId);
      if (serverNode == null) {
        logger.debug("new server node joined, serverNodeId: {}", serverId);
        continue;
      }
      String serverNodeIp = serverNode.getNetworkCardInfoName();

      long currentTime = System.currentTimeMillis();
      logger.debug("lastReportTime: {}, currentTime: {}, currentTime-lastReportTime: {}",
          lastReportTime,
          currentTime, currentTime - lastReportTime);

      boolean isSystemDaemonFailure = false;
      boolean isDihFailure = false;
      if (currentTime - lastReportTime > serverNodeReportOverTimeSecond * 1000) {
        // system daemon is fail.
        isSystemDaemonFailure = true;

        Set<Instance> dihInstances = instanceStore
            .getAll(PyService.DIH.getServiceName(), InstanceStatus.HEALTHY);

        boolean flag = false;
        for (Instance instance : dihInstances) {
          if (serverNodeIp.contains(instance.getEndPoint().getHostName())) {
            flag = true;
            break;
          }
        }

        if (!flag) {
          isDihFailure = true;
        }

        // dih is fail.
        if (isDihFailure) {

          //Set<Instance> systemDaemonInstances = instanceStore.getAll(
          //PyService.SYSTEMDAEMON.getServiceName());
          Set<Instance> systemDaemonInstances = new HashSet<>();
          for (Instance instance : systemDaemonInstances) {
            if (serverNodeIp.contains(instance.getEndPoint().getHostName())) {
              logger.warn("before modify, alert server node ip is: {}",
                  instance.getEndPoint().getHostName());
              serverNodeIp = instance.getEndPoint().getHostName();
              logger.warn("after modify, alert server node ip is: {}",
                  instance.getEndPoint().getHostName());
              break;
            }
          }

          if (!isInMaintenanceMode(serverNodeIp)) {
            generateEventData(serverNode, false);
            hasGenerateAlarmBefore.put(serverNode.getId(), true);
          }
          serverNode.setStatus("unknown");
          serverNodeStore.updateServerNode(serverNode);

          logger.warn("serverNode report timeout, serverNodeIp: {}", serverNodeIp);


          list.add(serverId);
        }
      }

      if (!hasGenerateAlarmBefore.containsKey(serverNode.getId())) {
        hasGenerateAlarmBefore.put(serverNode.getId(), true);
      }
      if ((!isSystemDaemonFailure || !isDihFailure) && !isInMaintenanceMode(serverNodeIp)
          && hasGenerateAlarmBefore.get(serverNode.getId())) {
        logger.debug("start alert recovery.");
        generateEventData(serverNode, true);
        hasGenerateAlarmBefore.put(serverNode.getId(), false);
      }
    }

    for (String serverId : list) {
      serverNodeReportTimeMap.remove(serverId);
    }
  }

  private boolean isInMaintenanceMode(String serverNodeIp) {
    logger.debug("check if in maintenance mode: {}.", serverNodeIp);
    boolean isInMaintenance = false;

    List<InstanceMaintenanceInformation> instanceMaintenanceInformationList =
        instanceMaintenanceDbStore
            .listAll();
    for (InstanceMaintenanceInformation instanceMaintenanceInformation :
        instanceMaintenanceInformationList) {

      if (System.currentTimeMillis() > instanceMaintenanceInformation.getEndTime()) {
        instanceMaintenanceDbStore.delete(instanceMaintenanceInformation);
        continue;
      }

      Long instanceId = instanceMaintenanceInformation.getInstanceId();
      Set<Instance> instanceSet = instanceStore.getAll();

      String serverNodeIpString;

      for (Instance instance : instanceSet) {
        if (instance.getId().getId() == instanceId) {
          serverNodeIpString = instanceMaintenanceDbStore.getById(instanceId).getIp();
          if (serverNodeIp.equals(serverNodeIpString)) {
            isInMaintenance = true;
            break;
          }
        }
      }
    }

    return isInMaintenance;
  }

  private void generateEventData(ServerNode serverNode, boolean recovery) {
    logger.warn("ServerNodeAlertChecker generate eventdata: {}, recovery status: {}",
        serverNode.getNetworkCardInfoName(), recovery);

    Map<String, String> defaultNameValues = new HashMap<>();
    defaultNameValues
        .put(UserDefineName.ServerNodeIp.toString(), serverNode.getNetworkCardInfoName());
    defaultNameValues.put(UserDefineName.ServerNodeHostName.toString(), serverNode.getHostName());
    defaultNameValues.put(UserDefineName.ServerNodeRackNo.toString(), serverNode.getRackNo());
    defaultNameValues
        .put(UserDefineName.ServerNodeChildFramNo.toString(), serverNode.getChildFramNo());
    defaultNameValues.put(UserDefineName.ServerNodeSlotNo.toString(), serverNode.getSlotNo());
    EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER, defaultNameValues);

    if (recovery) {
      eventDataWorker
          .work(OperationName.ServerNode.toString(), CounterName.SERVERNODE_STATUS.toString(), 1,
              0);
    } else {
      eventDataWorker
          .work(OperationName.ServerNode.toString(), CounterName.SERVERNODE_STATUS.toString(), 0,
              0);
    }

  }

}
