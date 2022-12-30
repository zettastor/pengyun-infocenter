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

package py.infocenter.service2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.exception.GenericThriftClientFactoryException;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.TargetType;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.InformationCenterClientWrapper;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.DriverStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.control.OperationStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.infocenter.service.CheckVolumeIsReadOnlyRequest;
import py.thrift.infocenter.service.CheckVolumeIsReadOnlyResponse;
import py.thrift.infocenter.service.SaveOperationLogsToCsvRequest;
import py.thrift.infocenter.service.SaveOperationLogsToCsvResponse;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.ArchiveTypeThrift;
import py.thrift.share.GetConnectClientInfoRequest;
import py.thrift.share.GetConnectClientInfoResponse;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.OnlineDiskRequest;
import py.thrift.share.SettleArchiveTypeRequest;
import py.thrift.share.VolumeIsConnectedByWritePermissionClientExceptionThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

public class InformationCenterImplTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(InformationCenterImplTest.class);

  //
  static {
    // 1
    List<String> tests = new ArrayList<>();
    for (String test : tests) {

    }

    // 2
    Iterator<String> it = tests.iterator();
    while (it.hasNext()) {
      String test = it.next();
    }
    //
    // 1
    Map<Integer, String> testMaps = new HashMap<>();
    for (Entry<Integer, String> entry : testMaps.entrySet()) {
      entry.getKey();
      entry.getValue();
    }
    // 2
    Iterator<Integer> itMap = testMaps.keySet().iterator();
    while (itMap.hasNext()) {
      Integer key = itMap.next();
      String value = testMaps.get(key);
    }
  }

  InformationCenterImpl informationCenter;
  @Mock
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
  @Mock
  private PySecurityManager securityManager;
  @Mock
  private OperationStore operationStore;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private VolumeInformationManger volumeInformationManger;
  @Mock
  private DriverStore driverStore;
  @Mock
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
  private long volumeId = RequestIdBuilder.get();
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;



  @Before
  public void setup() throws Exception {
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();

    informationCenter = new InformationCenterImpl();
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setAppContext(appContext);
    informationCenter.setDriverStore(driverStore);
    informationCenter.setVolumeInformationManger(volumeInformationManger);
    informationCenter.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore);
    informationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    List<DriverMetadata> driverList = new ArrayList<>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName3"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName4"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READONLY);
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
  }

  @Ignore
  @Test
  public void testRequestValidator() {
    try {
      GetVolumeResponse getVolumeResponse = mock(GetVolumeResponse.class);
      InformationCenterClientWrapper clientWrapper = mock(InformationCenterClientWrapper.class);
      VolumeMetadataThrift volumeMetadataThrift = mock(VolumeMetadataThrift.class);

      /*
       * volume has been found by volume id ,needn't to check out the volume name.
       */
      when(informationCenter.getVolume(any(GetVolumeRequest.class))).thenReturn(getVolumeResponse);
      when(getVolumeResponse.getVolumeMetadata()).thenReturn(volumeMetadataThrift);

      /*
       * volume has not been found by volume id ,but been found by volume name.
       */
      when(informationCenter.getVolume(any(GetVolumeRequest.class))).thenReturn(null);
      when(getVolumeResponse.getVolumeMetadata()).thenReturn(null);
      when(informationCenter.getVolumeNotDeadByName(any(GetVolumeRequest.class)))
          .thenReturn(getVolumeResponse);
      when(getVolumeResponse.getVolumeMetadata()).thenReturn(volumeMetadataThrift);

      /*
       * volume has not been found by volume id ,and also volume name.
       */
      when(informationCenter.getVolume(any(GetVolumeRequest.class))).thenReturn(null);
      when(getVolumeResponse.getVolumeMetadata()).thenReturn(null);
      when(informationCenter.getVolumeNotDeadByName(any(GetVolumeRequest.class))).thenReturn(null);
      when(getVolumeResponse.getVolumeMetadata()).thenReturn(null);

    } catch (Exception e) {
      logger.info("Caught an exception", e);
      fail();
    }
  }

  /**
   * Test mark volume readOnly and then check volume is readOnly. Volume is connected by a client
   * with ReadWrite permission and is not applied any access rule. Expected IsNotReadOnlyException.
   */
  @Test
  public void volumeIsReadOnlyWhenVolumeIsConnectedByWriteClientTest() throws Exception {

    InstanceStore instanceStore = mock(InstanceStore.class);
    CoordinatorClientFactory coordinatorClientFactory = mock(CoordinatorClientFactory.class);

    informationCenter.setAppContext(appContext);
    informationCenter.setInstanceStore(instanceStore);
    informationCenter.setCoordinatorClientFactory(coordinatorClientFactory);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    // Prepare coordinator
    Set<Instance> coordinatorSet = new HashSet<>();
    Instance coordinator = new Instance(new InstanceId(1L), "coordinator", InstanceStatus.HEALTHY,
        new EndPoint("0.0.0.1", 8082));
    Map<PortType, EndPoint> endPointMap = coordinator.getEndPoints();
    endPointMap.put(PortType.IO, new EndPoint("0.0.0.1", 8081));
    coordinatorSet.add(coordinator);
    when(instanceStore.getAll(anyString(), any(InstanceStatus.class))).thenReturn(coordinatorSet);

    // Prepare isVolumeReadOnlyResponse

    // Build coordinator client
    CoordinatorClientFactory.CoordinatorClientWrapper coordinatorClientWrapper = mock(
        CoordinatorClientFactory.CoordinatorClientWrapper.class);
    when(coordinatorClientFactory.build(any(EndPoint.class))).thenReturn(coordinatorClientWrapper);
    Coordinator.Iface coordinatorClient = mock(Coordinator.Iface.class);
    when(coordinatorClientWrapper.getClient()).thenReturn(coordinatorClient);

    // Parpare getConnectClientInfoResponse
    GetConnectClientInfoResponse getConnectClientInfoResponse = new GetConnectClientInfoResponse();
    Map<String, AccessPermissionTypeThrift> accessPermissionTypeThriftMap = new HashMap<>();
    accessPermissionTypeThriftMap.put("client1", AccessPermissionTypeThrift.READWRITE);
    getConnectClientInfoResponse.setConnectClientAndAccessType(accessPermissionTypeThriftMap);

    // Start the test
    when(coordinatorClient.getConnectClientInfo(any(GetConnectClientInfoRequest.class)))
        .thenReturn(getConnectClientInfoResponse);
    CheckVolumeIsReadOnlyRequest readOnlyRequest = new CheckVolumeIsReadOnlyRequest();
    readOnlyRequest.setRequestId(RequestIdBuilder.get());
    readOnlyRequest.setVolumeId(volumeId);

    boolean findException = false;
    try {
      informationCenter.checkVolumeIsReadOnly(readOnlyRequest);
    } catch (VolumeIsConnectedByWritePermissionClientExceptionThrift exceptionThrift) {
      findException = true;
    }

    assertTrue(findException);
  }

  @Test
  public void testIsReadOnlyVolume() throws GenericThriftClientFactoryException, TException {

    InstanceStore instanceStore = mock(InstanceStore.class);
    CoordinatorClientFactory coordinatorClientFactory = mock(CoordinatorClientFactory.class);

    informationCenter.setAppContext(appContext);
    informationCenter.setInstanceStore(instanceStore);
    informationCenter.setCoordinatorClientFactory(coordinatorClientFactory);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    // Prepare coordinator
    Set<Instance> coordinatorSet = new HashSet<>();
    Instance coordinator = new Instance(new InstanceId(1L), "coordinator", InstanceStatus.HEALTHY,
        new EndPoint("0.0.0.1", 8082));
    Map<PortType, EndPoint> endPointMap = coordinator.getEndPoints();
    endPointMap.put(PortType.IO, new EndPoint("0.0.0.2", 8081));
    coordinatorSet.add(coordinator);
    when(instanceStore.getAll(anyString(), any(InstanceStatus.class))).thenReturn(coordinatorSet);

    // Build coordinator client
    CoordinatorClientFactory.CoordinatorClientWrapper coordinatorClientWrapper = mock(
        CoordinatorClientFactory.CoordinatorClientWrapper.class);
    when(coordinatorClientFactory.build(any(EndPoint.class))).thenReturn(coordinatorClientWrapper);
    Coordinator.Iface coordinatorClient = mock(Coordinator.Iface.class);
    when(coordinatorClientWrapper.getClient()).thenReturn(coordinatorClient);

    CheckVolumeIsReadOnlyRequest readOnlyRequest = new CheckVolumeIsReadOnlyRequest();
    readOnlyRequest.setRequestId(RequestIdBuilder.get());
    readOnlyRequest.setVolumeId(volumeId);

    CheckVolumeIsReadOnlyResponse readOnlyResponse = informationCenter
        .checkVolumeIsReadOnly(readOnlyRequest);

    assertTrue(readOnlyResponse.isReadOnly());
  }

  /**
   * Test save operation to csv file api. There are two operations in the store, when test do not
   * set filter, then return all two, if the filter can filtrate one out, then return only one, if
   * the filter filtrate all two out, then return no record, but anyway the csv header will return.
   */
  @Test
  public void testSaveOperationLogsToCsv() throws Exception {
    informationCenter.setOperationStore(operationStore);
    List<Operation> operations = new ArrayList<>();
    when(operationStore.getAllOperation()).thenReturn(operations);

    // prepare operations
    Operation operation1 = new Operation();
    operation1.setAccountName("test1");
    operation1.setOperationType(OperationType.LOGIN);
    operation1.setTargetType(TargetType.USER);
    operation1.setOperationObject("test1");
    operation1.setTargetName("");
    operation1.setStatus(OperationStatus.SUCCESS);
    operation1.setStartTime(System.currentTimeMillis());
    operation1.setEndTime(System.currentTimeMillis());
    operations.add(operation1);

    Operation operation2 = new Operation();
    operation2.setAccountName("test2");
    operation2.setOperationType(OperationType.LOGIN);
    operation2.setTargetType(TargetType.USER);
    operation2.setOperationObject("test2");
    operation2.setTargetName("");
    operation2.setStatus(OperationStatus.SUCCESS);
    operation2.setStartTime(System.currentTimeMillis() + 1000L);
    operation2.setEndTime(System.currentTimeMillis() + 1000L);
    operations.add(operation2);

    // test request with no filter
    SaveOperationLogsToCsvRequest saveOperationLogsToCsvRequest =
        new SaveOperationLogsToCsvRequest();
    saveOperationLogsToCsvRequest.setRequestId(RequestIdBuilder.get());
    saveOperationLogsToCsvRequest.setAccountId(1L);
    SaveOperationLogsToCsvResponse response;
    String csvFile;
    response = informationCenter.saveOperationLogsToCsv(saveOperationLogsToCsvRequest);
    csvFile = new String(response.getCsvFile(), "UTF-8");
    logger.warn("csvFile: {}", csvFile);
    List<String> stringsSplitByCommon;

    stringsSplitByCommon = Arrays.asList(csvFile.split("[,"

        + System.getProperty("line.separator", "\n") + "]"));
    assertEquals(21, stringsSplitByCommon.size());

    // test request with filter filtrate one out
    saveOperationLogsToCsvRequest.setAccountName("test1");
    response = informationCenter.saveOperationLogsToCsv(saveOperationLogsToCsvRequest);
    csvFile = new String(response.getCsvFile(), "UTF-8");
    logger.warn("csvFile: {}", csvFile);
    stringsSplitByCommon = Arrays.asList(csvFile.split("[,"

        + System.getProperty("line.separator", "\n") + "]"));
    assertEquals(14, stringsSplitByCommon.size());

    // test request with filter filtrate all two out
    saveOperationLogsToCsvRequest.setAccountName("test3");
    response = informationCenter.saveOperationLogsToCsv(saveOperationLogsToCsvRequest);
    csvFile = new String(response.getCsvFile(), "UTF-8");
    logger.warn("csvFile: {}", csvFile);
    stringsSplitByCommon = Arrays.asList(csvFile.split("[,"

        + System.getProperty("line.separator", "\n") + "]"));
    assertEquals(7, stringsSplitByCommon.size());
  }

  @Test
  @Deprecated
  public void onlineDiskTest() throws TException, GenericThriftClientFactoryException {

    OnlineDiskRequest req = new OnlineDiskRequest();
    req.setRequestId(RequestIdBuilder.get());
    req.setAccountId(1L);

    InstanceMetadataThrift instance = new InstanceMetadataThrift();
    instance.setEndpoint("10.0.1.79:8010");
    req.setInstance(instance);

    ArchiveMetadataThrift onlineArchive = new ArchiveMetadataThrift();
    onlineArchive.setDevName("testDveName");
    req.setOnlineArchive(onlineArchive);

    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(dataNodeClient);

    OperationStore operationStore = mock(OperationStore.class);

    informationCenter.setOperationStore(operationStore);
    informationCenter.setDataNodeClientFactory(dataNodeClientFactory);
    informationCenter.onlineDisk(req);

    Mockito.verify(operationStore,
        Mockito.times(1)).saveOperation(any(Operation.class));
  }

  @Test
  public void settleArchiveTypeTest() throws TException, GenericThriftClientFactoryException {

    SettleArchiveTypeRequest req = new SettleArchiveTypeRequest();
    req.setRequestId(RequestIdBuilder.get());
    req.setAccountId(1L);

    InstanceMetadataThrift instance = new InstanceMetadataThrift();
    instance.setEndpoint("10.0.1.79:8010");
    req.setInstance(instance);

    List<ArchiveTypeThrift> archiveTypeList = new ArrayList<>();
    archiveTypeList.add(ArchiveTypeThrift.RAW_DISK);
    req.setArchiveTypes(archiveTypeList);

    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(dataNodeClient);

    OperationStore operationStore = mock(OperationStore.class);

    informationCenter.setOperationStore(operationStore);
    informationCenter.setDataNodeClientFactory(dataNodeClientFactory);
    informationCenter.settleArchiveType(req);

    Mockito.verify(dataNodeClient,
        Mockito.times(1)).settleArchiveType(req);
    Mockito.verify(operationStore,
        Mockito.times(1)).saveOperation(any(Operation.class));
  }

  @Test
  public void fixConfigMismatchDiskTest() throws TException, GenericThriftClientFactoryException {
    OnlineDiskRequest req = new OnlineDiskRequest();
    req.setRequestId(RequestIdBuilder.get());
    req.setAccountId(1L);

    InstanceMetadataThrift instance = new InstanceMetadataThrift();
    instance.setEndpoint("10.0.1.79:8010");
    req.setInstance(instance);

    ArchiveMetadataThrift onlineArchive = new ArchiveMetadataThrift();
    onlineArchive.setDevName("testDveName");
    req.setOnlineArchive(onlineArchive);

    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(dataNodeClient);

    OperationStore operationStore = mock(OperationStore.class);

    informationCenter.setOperationStore(operationStore);
    informationCenter.setDataNodeClientFactory(dataNodeClientFactory);
    informationCenter.fixConfigMismatchDisk(req);

    Mockito.verify(dataNodeClient,
        Mockito.times(1)).fixConfigMismatchedDisk(req);
    Mockito.verify(operationStore,
        Mockito.times(1)).saveOperation(any(Operation.class));
  }

  @Test
  public void ceilVolumeSizeWithSegmentSizeTest()
      throws TException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method method = informationCenter.getClass()
        .getDeclaredMethod("ceilVolumeSizeWithSegmentSize", long.class);
    method.setAccessible(true);
    informationCenter.setSegmentSize(3);

    long factVolumeSize = (Long) method.invoke(informationCenter, 1);
    assertTrue(factVolumeSize == 3);

    factVolumeSize = (Long) method.invoke(informationCenter, 2);
    assertTrue(factVolumeSize == 3);

    factVolumeSize = (Long) method.invoke(informationCenter, 5);
    assertTrue(factVolumeSize == 6);

    factVolumeSize = (Long) method.invoke(informationCenter, 9);
    assertTrue(factVolumeSize == 9);

    boolean isOk = false;
    try {
      factVolumeSize = (Long) method.invoke(informationCenter, 0);
    } catch (Exception e) {
      isOk = true;
    }
    assertTrue(isOk);

    isOk = false;
    try {
      factVolumeSize = (Long) method.invoke(informationCenter, -1);
    } catch (Exception e) {
      isOk = true;
    }
    Assert.assertTrue(isOk);
  }

  private VolumeMetadata buildVolumeMetadata() {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    long volumeId = RequestIdBuilder.get();
    volumeMetadata.setRootVolumeId(volumeId);
    volumeMetadata.setVolumeId(volumeId);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setEnableLaunchMultiDrivers(true);

    SegId segId = new SegId(volumeId, 1);
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, 2);
    SegmentMembership highestMembershipInSegment = mock(SegmentMembership.class);
    SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 1);
    segmentUnitMetadata.setStatus(SegmentUnitStatus.Arbiter);
    volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);

    return volumeMetadata;
  }

  private DriverMetadata buildDriver(DriverType type, String host) {
    DriverMetadata driver = new DriverMetadata();
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(type);
    driver.setHostName("0.0.0.1");
    driver.setDriverName("test");
    driver.setCoordinatorPort(8081);
    return driver;
  }
}
