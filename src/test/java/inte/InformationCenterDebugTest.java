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

package inte;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import py.RequestResponseHelper;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.icshare.ListAllDriversRequest;
import py.thrift.icshare.ListAllDriversResponse;
import py.thrift.icshare.ListDriverClientInfoRequest;
import py.thrift.icshare.ListDriverClientInfoResponse;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.icshare.ReportDriverMetadataRequest;
import py.thrift.icshare.ReportDriverMetadataResponse;
import py.thrift.icshare.UpdateVolumeDescriptionRequest;
import py.thrift.icshare.UpdateVolumeDescriptionResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.DriverIpTargetThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverStatusThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.InstanceIncludeVolumeInfoRequest;
import py.thrift.share.InstanceIncludeVolumeInfoResponse;
import py.thrift.share.ListIscsiAccessRulesRequest;
import py.thrift.share.ListIscsiAccessRulesResponse;
import py.thrift.share.PortalTypeThrift;
import py.thrift.share.UmountDriverRequestThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.listZookeeperServiceStatusRequest;
import py.thrift.share.listZookeeperServiceStatusResponse;
import py.volume.VolumeMetadata;

/**
 * just for debug the informationCenter interface.
 ****/
//@Ignore
public class InformationCenterDebugTest extends TestBase {

  private int anInt = 10000;
  private long accountId = SUPERADMIN_ACCOUNT_ID;
  private InformationCenterClientFactory infoCenterClientFactory;
  private InformationCenter.Iface informationCenter;
  private InstanceStore instanceStore;

  /* modify fields, only set the hostName(one of the DIH host)
   **/
  private String hostName = "10.0.0.80";
  private long volumeId = 2152091268712079375L;

  public InformationCenterDebugTest() {
  }

  public EndPoint localdihep() {
    return EndPointParser.parseLocalEndPoint(anInt, hostName);
  }

  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    return dihClientFactory;
  }



  public InstanceStore instanceStore() throws Exception {
    Object instanceStore = DihInstanceStore.getSingleton();
    ((DihInstanceStore) instanceStore).setDihClientFactory(dihClientFactory());
    ((DihInstanceStore) instanceStore).setDihEndPoint(localdihep());
    ((DihInstanceStore) instanceStore).init();
    Thread.sleep(5000);
    System.out.println("====" + ((DihInstanceStore) instanceStore).getAll());
    return (InstanceStore) instanceStore;
  }



  public InformationCenterClientFactory informationCenterClientFactory() throws Exception {
    InformationCenterClientFactory factory = new InformationCenterClientFactory(1);
    factory.setInstanceName(PyService.INFOCENTER.getServiceName());
    factory.setInstanceStore(instanceStore());
    return factory;
  }

  @Override
  @Before
  public void init() throws Exception {
    /* set log **/
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.WARN);

    super.init();
    instanceStore = instanceStore();
    infoCenterClientFactory = informationCenterClientFactory();
  }

  @Test
  public void testListZookeeperServiceStatus() {
    listZookeeperServiceStatusRequest request = new listZookeeperServiceStatusRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(accountId);

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      listZookeeperServiceStatusResponse response = informationCenter
          .listZookeeperServiceStatus(request);
      logger.warn("get the value is :{}", response.getZookeeperStatusList());
    } catch (Exception e) {
      logger
          .error("catch an exception when list testListZookeeperServiceStatus {}", e);
    }
  }

  @Test
  public void testListDriver() {
    ListAllDriversRequest request = new ListAllDriversRequest();
    request.setRequestId(RequestIdBuilder.get());

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      ListAllDriversResponse response = informationCenter.listAllDrivers(request);
      logger.warn("get the listAllDrivers:{}", response);

    } catch (Exception e) {
      logger.error("catch an exception when listDriverClientInfo {}, accountId: {}", volumeId,
          accountId, e);
    }
  }

  @Test
  public void testForceUmountDriver() {
    String driverIp = "192.168.2.81";
    long driverContainerId = 4872845027405069337L;
    long volumeId = 3122877167770092592L;
    List<DriverIpTargetThrift> driverIpTargetList = new ArrayList<DriverIpTargetThrift>();
    driverIpTargetList.add(new DriverIpTargetThrift(0, driverIp,
        DriverTypeThrift.ISCSI, driverContainerId));

    UmountDriverRequestThrift request = new UmountDriverRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(volumeId);
    request.setAccountId(SUPERADMIN_ACCOUNT_ID);
    request.setForceUmount(true);
    request.setDriverIpTargetList(driverIpTargetList);

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      informationCenter.umountDriver(request);
    } catch (Exception e) {
      logger.error("catch an exception when umountDriver for  volumes {}, the error:", volumeId, e);
    }
  }

  @Test
  public void testGetVolumeByPagination() {
    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      InformationCenterClientWrapper informationCenterClientWrapper = infoCenterClientFactory
          .build(6000);

      logger.warn("try to get info center client in time: {}");
      VolumeMetadata volumeMetadataForGetVolume = informationCenterClientWrapper
          .getVolume(volumeId, accountId);

      logger.warn("the volume is :{}", volumeMetadataForGetVolume);


    } catch (Exception e) {
      logger.error("catch an exception when get volume: {}, accountId: {}", volumeId, accountId, e);
    }
  }


  @Test
  public void listVolumes() {
    Set<Long> volumesCanBeList = new HashSet<>();
    ListVolumesRequest request = new ListVolumesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(accountId);

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      ListVolumesResponse response = informationCenter.listVolumes(request);
      logger.warn("get the value is :{}", response);
    } catch (Exception e) {
      logger
          .error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId, e);
    }
  }

  @Test
  public void listIscsiAccessRules() {
    ListIscsiAccessRulesRequest request = new ListIscsiAccessRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(accountId);

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      ListIscsiAccessRulesResponse response = informationCenter.listIscsiAccessRules(request);
      logger.warn("get the value is :{}", response.getAccessRules().size());
    } catch (Exception e) {
      logger
          .error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId, e);
    }
  }

  @Test
  public void listZookeeperServiceStatus() {
    listZookeeperServiceStatusRequest request = new listZookeeperServiceStatusRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(accountId);

    try {
      informationCenter = infoCenterClientFactory.build(10000).getClient();
      listZookeeperServiceStatusResponse response = informationCenter
          .listZookeeperServiceStatus(request);
      logger.warn("get the value is :{}", response.getZookeeperStatusList());
    } catch (Exception e) {
      logger
          .error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId, e);
    }
  }

  @Test
  public void listDriverClients() {
    ListDriverClientInfoRequest request = new ListDriverClientInfoRequest();
    request.setRequestId(RequestIdBuilder.get());

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      ListDriverClientInfoResponse response = informationCenter.listDriverClientInfo(request);
      List<py.thrift.share.DriverClientInfoThrift> driverClientInfoThrift = response
          .getDriverClientInfothrift();
      for (py.thrift.share.DriverClientInfoThrift driverClientInfoThrift1 :
          driverClientInfoThrift) {
        logger.warn("get the value is :{}",
            RequestResponseHelper.buildDriverMetadataFrom(driverClientInfoThrift1));
      }
    } catch (Exception e) {
      logger
          .error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId, e);
    }
  }

  @Test
  public void reportDriverMetadataTest() {

    for (int i = 0; i < 3; i++) {
      logger.warn("---------------- in index :{} ----------------------", i);
      DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
      driverMetadataThrift.setDriverName("testdriver");
      driverMetadataThrift.setVolumeId(782380711328904215L);
      driverMetadataThrift.setSnapshotId(0);
      driverMetadataThrift.setDriverType(DriverTypeThrift.ISCSI);
      driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
      driverMetadataThrift.setHostName("hostname1");
      driverMetadataThrift.setPort(1);
      driverMetadataThrift.setDriverContainerId(1111L);
      driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);

      Map<String, AccessPermissionTypeThrift> clientHostAccessRule = new HashMap<>();

      if (i % 2 == 0) {
        for (int j = 0; j < 2; j++) {
          String host = "10.0.0." + j;
          clientHostAccessRule.put(host, AccessPermissionTypeThrift.READ);
        }
      }

      driverMetadataThrift.setClientHostAccessRule(clientHostAccessRule);

      ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
      request.setRequestId(2222222L);
      request.setDrivercontainerId(1111L);
      request.addToDrivers(driverMetadataThrift);
      try {
        informationCenter = infoCenterClientFactory.build().getClient();
        ReportDriverMetadataResponse response = informationCenter.reportDriverMetadata(request);
      } catch (Exception e) {
        logger.error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId,
            e);
      }
    }
  }

  @Test
  public void reportDriverMetadataTest3() throws InterruptedException {

    for (int i = 0; i < 1000; i++) {
      logger.warn("---------------- in index :{} ----------------------", i);
      DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
      driverMetadataThrift.setDriverName("testdriver");
      driverMetadataThrift.setVolumeId(2667220445248603227L);
      driverMetadataThrift.setSnapshotId(0);
      driverMetadataThrift.setDriverType(DriverTypeThrift.ISCSI);
      driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
      driverMetadataThrift.setHostName("hostname1");
      driverMetadataThrift.setPort(1);
      driverMetadataThrift.setDriverContainerId(1111L);
      driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);

      Map<String, AccessPermissionTypeThrift> clientHostAccessRule = new HashMap<>();

      String host = "10.0.0." + 1;
      clientHostAccessRule.put(host, AccessPermissionTypeThrift.READ);

      if (i % 2 == 0 && i < 50) {
        driverMetadataThrift.setDriverName("testdriver2");
        driverMetadataThrift.setHostName("hostname2");
        driverMetadataThrift.setDriverContainerId(2222L);
        clientHostAccessRule.clear();
        host = "10.0.0." + 2;
        clientHostAccessRule.put(host, AccessPermissionTypeThrift.READ);
      }

      driverMetadataThrift.setClientHostAccessRule(clientHostAccessRule);

      ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
      request.setRequestId(2222222L);
      request.setDrivercontainerId(1111L);
      if (i % 2 == 0) {
        request.setDrivercontainerId(2222L);
      }
      request.addToDrivers(driverMetadataThrift);
      try {
        informationCenter = infoCenterClientFactory.build().getClient();
        ReportDriverMetadataResponse response = informationCenter.reportDriverMetadata(request);
      } catch (Exception e) {
        logger.error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId,
            e);
      }

      Thread.sleep(500);
    }
  }

  @Test
  public void reportDriverMetadataTest2() throws InterruptedException {

    for (int i = 0; i < 100; i++) {
      logger.warn("---------------- in index :{} ----------------------", i);
      DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
      driverMetadataThrift.setDriverName("testdriver");
      driverMetadataThrift.setVolumeId(2667220445248603227L);
      driverMetadataThrift.setSnapshotId(0);
      driverMetadataThrift.setDriverType(DriverTypeThrift.ISCSI);
      driverMetadataThrift.setDriverStatus(DriverStatusThrift.LAUNCHED);
      driverMetadataThrift.setHostName("hostname1");
      driverMetadataThrift.setPort(1);
      driverMetadataThrift.setDriverContainerId(1111L);
      driverMetadataThrift.setPortalType(PortalTypeThrift.IPV4);

      Map<String, AccessPermissionTypeThrift> clientHostAccessRule = new HashMap<>();

      String host = "10.0.0." + 1;
      clientHostAccessRule.put(host, AccessPermissionTypeThrift.READ);

      //disconnect
      if (i > 50) {
        clientHostAccessRule.clear();
      }

      driverMetadataThrift.setClientHostAccessRule(clientHostAccessRule);

      ReportDriverMetadataRequest request = new ReportDriverMetadataRequest();
      request.setRequestId(2222222L);
      request.setDrivercontainerId(1111L);
      request.addToDrivers(driverMetadataThrift);
      try {
        informationCenter = infoCenterClientFactory.build().getClient();
        ReportDriverMetadataResponse response = informationCenter.reportDriverMetadata(request);
      } catch (Exception e) {
        logger.error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId,
            e);
      }

      Thread.sleep(500);
    }
  }

  @Test
  public void updateVolumeDescriptionTest() {
    UpdateVolumeDescriptionRequest request = new UpdateVolumeDescriptionRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(accountId);
    request.setVolumeId(volumeId);
    request.setVolumeDescription("0728");

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      UpdateVolumeDescriptionResponse response = informationCenter.updateVolumeDescription(request);

    } catch (Exception e) {
      logger
          .error("catch an exception when list volumes {}, accountId: {}", volumeId, accountId, e);
    }
  }

  @Test
  public void listDriverClientInfoTest() {
    ListDriverClientInfoRequest request = new ListDriverClientInfoRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(volumeId);

    try {
      informationCenter = infoCenterClientFactory.build().getClient();
      ListDriverClientInfoResponse response = informationCenter.listDriverClientInfo(request);
      logger.warn("get the ListDriverClientInfoResponse:{}", response);

    } catch (Exception e) {
      logger.error("catch an exception when listDriverClientInfo {}, accountId: {}", volumeId,
          accountId, e);
    }
  }

  @Test
  public void testGetSomeInfoAboutHa() {
    try {

      informationCenter = infoCenterClientFactory.build().getClient();
      logger.warn("try to get info center volume {}");

      Map<Long, EndPoint> instanceInfo = new HashMap<>();
      Set<Instance> instances = instanceStore.getAll(PyService.INFOCENTER.getServiceName());
      logger.warn("get the all ");

      for (Instance id : instances) {
        if (id.getStatus().equals(InstanceStatus.HEALTHY) || id.getStatus()
            .equals(InstanceStatus.SUSPEND)) {
          instanceInfo.put(id.getId().getId(), id.getEndPoint());
        }
      }

      //get each instance volume info
      InstanceIncludeVolumeInfoRequest request = new InstanceIncludeVolumeInfoRequest(
          RequestIdBuilder.get());
      InstanceIncludeVolumeInfoResponse response = null;
      try {
        response = informationCenter.getInstanceIncludeVolumeInfo(request);
      } catch (Exception e) {
        logger.error("when getInstanceIncludeVolumeInfo,get error:", e);
      }

      //
      ListVolumesResponse listVolumesResponse = null;
      ListVolumesRequest listVolumesRequest = new ListVolumesRequest();
      listVolumesRequest.setRequestId(RequestIdBuilder.get());
      listVolumesRequest.setAccountId(accountId);
      listVolumesRequest.setContainDeadVolume(true);
      try {
        listVolumesResponse = informationCenter.listVolumes(listVolumesRequest);
      } catch (Exception e) {
        logger.error("when listVolumes,get error:", e);
      }
      logger.info("list all volumes :{}", listVolumesResponse.getVolumes());

      Map<Long, String> volumes = new HashMap<>();
      Map<EndPoint, List<String>> volumesPrint = new HashMap<>();
      for (VolumeMetadataThrift volumeMetadataThrift : listVolumesResponse.getVolumes()) {
        volumes.put(volumeMetadataThrift.getVolumeId(), volumeMetadataThrift.getName());
      }

      if (response != null) {
        Map<Long, Set<Long>> instanceIncludeVolumeInfo = response.getInstanceIncludeVolumeInfo();
        for (Map.Entry<Long, Set<Long>> entry : instanceIncludeVolumeInfo.entrySet()) {

          long instanceId = entry.getKey();
          Set<Long> volume = entry.getValue();
          List<String> volumeNames = new ArrayList<>();

          for (Long id : volume) {
            volumeNames.add(volumes.get(id));
          }

          volumesPrint.put(instanceInfo.get(instanceId), volumeNames);

        }
      }

      for (Map.Entry<EndPoint, List<String>> entry : volumesPrint.entrySet()) {
        logger.warn("the end, get the instance :{}  have volumes :{} ", entry.getKey(),
            entry.getValue());
      }

    } catch (Exception e) {
      logger.error("catch an exception when get volume: {}, accountId: {}", volumeId, accountId, e);
    }
  }

  private void compareTwoSegment(SegmentMetadata beforeSegment, SegmentMetadata afterSegment) {
    assertEquals(beforeSegment.getSegId().getVolumeId().getId(),
        afterSegment.getSegId().getVolumeId().getId());
    for (InstanceId beforeInstanceId : beforeSegment.getSegmentUnitMetadataTable().keySet()) {
      assertTrue(afterSegment.getSegmentUnitMetadataTable().containsKey(beforeInstanceId));
    }

    List<SegmentMembership> beforeMemberships = new ArrayList<>();
    List<SegmentMembership> afterMemberships = new ArrayList<>();

    for (SegmentUnitMetadata beforeUnit : beforeSegment.getSegmentUnitMetadataTable().values()) {
      beforeMemberships.add(beforeUnit.getMembership());
    }
    for (SegmentUnitMetadata afterUnit : afterSegment.getSegmentUnitMetadataTable().values()) {
      afterMemberships.add(afterUnit.getMembership());
    }

    for (SegmentMembership before : beforeMemberships) {
      boolean found = false;
      for (SegmentMembership after : afterMemberships) {
        if (after.equals(before)) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }
}
