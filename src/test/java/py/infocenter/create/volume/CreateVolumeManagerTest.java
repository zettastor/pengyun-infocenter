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

package py.infocenter.create.volume;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.volume.VolumeInAction.NULL;
import static py.volume.VolumeMetadata.VolumeSourceType.CREATE_VOLUME;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;
import junit.framework.Assert;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.exception.InternalErrorException;
import py.exception.InvalidInputException;
import py.icshare.AccountMetadata;
import py.icshare.VolumeCreationRequest;
import py.icshare.authorization.AccountStore;
import py.infocenter.InfoCenterConfiguration;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.VolumeStore;
import py.test.TestBase;
import py.thrift.datanode.service.CreateSegmentUnitBatchRequest;
import py.thrift.datanode.service.CreateSegmentUnitBatchResponse;
import py.thrift.datanode.service.CreateSegmentUnitFailedCodeThrift;
import py.thrift.datanode.service.CreateSegmentUnitFailedNode;
import py.thrift.datanode.service.CreateSegmentUnitNode;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.CreateSegmentUnitResponse;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DeleteSegmentUnitRequest;
import py.thrift.datanode.service.DeleteSegmentUnitResponse;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.infocenter.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.SegmentExistingExceptionThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class CreateVolumeManagerTest extends TestBase {

  private static boolean testStatusForGetCreateSegmentUnitRequest = false;
  @Mock
  GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
  @Mock
  GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory;
  private boolean testStatusForWithControllerAndReport = false;
  private boolean testStatusForFailedToCreateSegmentUnits = false;
  private boolean testStatusForQuorumNumberOk = false;
  private boolean testStatusForSegmentExistingException = false;
  private boolean testStatusForRetryOk = false;
  private boolean testStatusForRetryFail = false;
  private boolean testStatusForExtend = false;
  private boolean normalStatus = false;
  private DataNodeService.AsyncIface asyncIfaceMock = mock(DataNodeService.AsyncIface.class);
  private InfoCenterConfiguration infoCenterConfiguration;
  private AtomicInteger testGoodNumber = new AtomicInteger(0);
  private AtomicInteger setNumberForRetryOk = new AtomicInteger(0);
  private AtomicInteger setNumberForRetryFail = new AtomicInteger(0);
  @Mock
  private AccountStore accountStore;
  @Mock
  private InformationCenterClientFactory infoCenterClientFactory;
  @Mock
  private InformationCenterClientWrapper infocenterWrapper;
  @Mock
  private VolumeStore volumeStore;

  @Mock
  private VolumeInformationManger volumeInformationManger;

  private Random random = new Random();

  @Override
  public void init() throws Exception {
    super.init();
    //set log
    setLogLevel(Level.WARN);
    AccountMetadata accountMetadata = new AccountMetadata("test", "312",
        AccountMetadata.AccountType.Regular.name(), 1L);
    when(accountStore.getAccountById(anyLong())).thenReturn(accountMetadata);
  }

  /**
   * Test is that send a create segment unit request. Test step 1 : create datanode service. Step 2
   * :create a manager with crate volume and init infocenter configuration . Step 3 : build a
   * request . Step 4 : send create request to manager
   */
  @Test
  public void testCreateSegmentUnitsForGetCreateSegmentUnitRequest() {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();
    long volumeSize = 1000;
    long segSize = 1;
    try {
      final CreateVolumeManager createVolumeManager = setTesEnvNew(syncService, asyncIfaceMock,
          asyncService, volumeSize);

      infoCenterConfiguration.setSegmentCreatorEnabled(false);
      VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
          VolumeType.REGULAR, 1L, true);
      volumeRequest.setSegmentSize(segSize);
      volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          segId2InstancesFromRemote = new HashMap<>();
      for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
          currentSegIndex++) {
        List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
            buildInstances(3));
        Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
            new HashMap<>();
        unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
        unitType2InstanceMap
            .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
        segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
      }

      logger.warn("the segId2InstancesFromRemote size : {}", segId2InstancesFromRemote.size());
      testStatusForGetCreateSegmentUnitRequest = true;
      // create segment unit should succeed without throwing any exceptions
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    } catch (InvalidInputException e) {
      e.printStackTrace();
    } catch (InternalErrorException e) {
      e.printStackTrace();
    } catch (GenericThriftClientFactoryException e) {
      e.printStackTrace();
    } catch (InternalErrorThrift internalErrorThrift) {
      internalErrorThrift.printStackTrace();
    } catch (RuntimeException e) {
      logger.error("time out : :", e);
    } catch (Exception e) {
      logger.error("caught exception", e);

    }

    /* the end check in setTesEnvNew function **/
    assertTrue(testStatusForGetCreateSegmentUnitRequest);
  }

  /**
   * Test create segment units for quorum number ok.
   */
  @Test
  public void testCreateSegmentUnitsForQuorumNumberOk() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 1;
    long segSize = 1;

    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.REGULAR, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(3));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    logger.warn("the segId2InstancesFromRemote size : {}", segId2InstancesFromRemote.size());
    testStatusForQuorumNumberOk = true;
    // create segment unit should succeed without throwing any exceptions
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);

  }


  @Test
  public void testCreateSegmentUnitsForSegmentExistingException() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 1;
    long segSize = 1;

    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.REGULAR, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(3));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    logger.warn("the segId2InstancesFromRemote size : {}", segId2InstancesFromRemote.size());
    testStatusForSegmentExistingException = true;
    // create segment unit should succeed without throwing any exceptions
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);

  }


  @Test
  public void testCreateSegmentUnitsForPss() throws Exception {
    long volumeSize = 1;
    long segSize = 1;
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.REGULAR, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(3));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    logger.warn("the segId2InstancesFromRemote size : {}", segId2InstancesFromRemote.size());
    normalStatus = true;
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    // create segment unit should succeed without throwing any exceptions
    createVolumeManager.setPasselCount(2);
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
  }

  /**
   * Test create segment units for psa.
   */
  @Test
  public void testCreateSegmentUnitsForPsa() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 1;
    long segSize = 1;

    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.SMALL, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(2));
      List<InstanceIdAndEndPointThrift> arbiterInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstancesForArbiter(2));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Arbiter, arbiterInstancesThrift);
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    logger.warn("the segId2InstancesFromRemote size : {}", segId2InstancesFromRemote.size());
    normalStatus = true;
    // create segment unit should succeed without throwing any exceptions
    createVolumeManager.setPasselCount(2);
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);

  }

  /**
   * Test create segment units with set blocking time.
   */
  @Test
  public void testCreateSegmentUnitsWithSetBlockingTime() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    infoCenterConfiguration.setSegmentCreatorEnabled(true);
    infoCenterConfiguration.setSegmentCreatorBlockingTimeoutMillis(5000);
    long volumeSize = 1;
    long segSize = 1;

    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.REGULAR, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(3));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    final long beginTime = System.currentTimeMillis();
    normalStatus = true;
    // create segment unit should succeed without throwing any exceptions
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    final long endTime = System.currentTimeMillis();
    Assert.assertTrue(endTime - beginTime >= 5000 && endTime - beginTime < 7000);
  }


  @Test
  public void testCreateSegmentUnitsWithControllerAndReport() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    infoCenterConfiguration.setSegmentCreatorBlockingTimeoutMillis(5000);
    final long volumeSize = 1000;
    final long segSize = 1;

    final VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.REGULAR, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(3));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    Thread reporter = new Thread() {
      public void run() {
        while (true) {
          try {
            for (int segIndex = 0; segIndex < volumeSize / segSize; segIndex++) {
              SegId segId = new SegId(volumeRequest.getVolumeId(), segIndex);
              CreateSegmentUnitsRequest segmentCreator = createVolumeManager
                  .getSegmentCreator(segId);
              if (null != segmentCreator) {
                segmentCreator.close();
              }
            }
          } catch (Exception e) {
            logger.error("caught exception", e);
          }
        }
      }
    };
    reporter.start();
    testStatusForWithControllerAndReport = true;
    // create segment unit should succeed without throwing any exceptions
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
  }

  /**
   * Test the scenario where the creation of segment units fails.
   */
  @Test
  public void testFailedToCreateSegmentUnits() {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();
    boolean createSegmentUnitsStatus = true;
    try {

      final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
      long volumeSize = 10;
      long segSize = 1;

      VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
          VolumeType.REGULAR, 1L, true);
      volumeRequest.setSegmentSize(segSize);
      volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          segId2InstancesFromRemote = new HashMap<>();
      for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
          currentSegIndex++) {
        List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
            buildInstances(3));
        Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
            new HashMap<>();
        unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
        unitType2InstanceMap
            .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
        segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
      }
      testStatusForFailedToCreateSegmentUnits = true;
      // create segment unit should succeed without throwing any exceptions
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);

    } catch (InternalErrorThrift internalErrorThrift) {
      internalErrorThrift.printStackTrace();
    } catch (InternalErrorException e) {
      logger.error("error :", e);
    } catch (GenericThriftClientFactoryException e) {
      logger.error("error :", e);
    } catch (InvalidInputException e) {
      logger.error("error :", e);
    } catch (RuntimeException e) {
      logger.error("error :", e);
      createSegmentUnitsStatus = false;
    } catch (Exception e) {
      logger.error("error :", e);
    }

    assertTrue(!createSegmentUnitsStatus);
  }

  @Test
  public void testFailedToCreateSegmentUnitsOnError() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();
    asyncService.timesToThrowException = new AtomicInteger(2);
    asyncService.exceptionToThrow = new InternalErrorThrift();
    // deleting segment units are good
    CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 80;
    long segSize = 10;
    boolean createSegmentUnitsStatus = true;

    try {
      VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
          VolumeType.REGULAR, 1L, true);
      volumeRequest.setSegmentSize(segSize);
      volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          segId2InstancesFromRemote = new HashMap<>();
      for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
          currentSegIndex++) {
        List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
            buildInstances(5));
        Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
            new HashMap<>();
        unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
        unitType2InstanceMap
            .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
        segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
      }

      // create segment unit should succeed without throwing any exceptions
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    } catch (InternalErrorThrift internalErrorThrift) {
      internalErrorThrift.printStackTrace();
    } catch (InternalErrorException e) {
      logger.error("error :", e);
    } catch (GenericThriftClientFactoryException e) {
      logger.error("error :", e);
    } catch (InvalidInputException e) {
      logger.error("error :", e);
    } catch (RuntimeException e) {
      logger.error("error :", e);
      createSegmentUnitsStatus = false;
    } catch (Exception e) {
      logger.error("error :", e);
    }

    assertTrue(!createSegmentUnitsStatus);
  }

  /**
   * Test create segment units on error that not enough type.
   */
  @Test
  public void testFailedToCreateSegmentUnitsOnErrorNotEnoughType() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();
    // deleting segment units are good
    CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 1;
    long segSize = 1;
    boolean createSegmentUnitsStatus = true;

    try {
      VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
          VolumeType.REGULAR, 1L, true);
      volumeRequest.setSegmentSize(segSize);
      volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          segId2InstancesFromRemote = new HashMap<>();
      for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
          currentSegIndex++) {
        List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
            buildInstances(1));
        Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
            new HashMap<>();
        unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
        unitType2InstanceMap
            .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
        segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
      }

      // create segment unit should succeed without throwing any exceptions
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    } catch (InternalErrorThrift internalErrorThrift) {
      internalErrorThrift.printStackTrace();
    } catch (InternalErrorException e) {
      createSegmentUnitsStatus = false;
      logger.error("error :", e);
    } catch (GenericThriftClientFactoryException e) {
      logger.error("error :", e);
    } catch (InvalidInputException e) {
      logger.error("error :", e);
    } catch (RuntimeException e) {
      logger.error("error :", e);
    } catch (Exception e) {
      logger.error("error :", e);
    }

    assertTrue(!createSegmentUnitsStatus);
  }

  /**
   * Test create segment unit with retry fail.
   */
  @Test
  public void testCreateSegmentUnitsAfterRetriesOk() throws Exception {
    // make sure we have enough data nodes as candidates to retry the creation, otherwise the 
    // retries will be
    // failed.
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    // deleting segment units are good
    final CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 1;
    long segSize = 1;

    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
        VolumeType.REGULAR, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(5));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    // create segment unit should succeed without throwing any exceptions
    testStatusForRetryOk = true;
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
  }

  /**
   * Test create segment unit with retry fail.
   */
  @Test
  public void testCreateSegmentUnitsAfterRetriesFail() throws Exception {
    // make sure we have enough data nodes as candidates to retry the creation, otherwise the 
    // retries will be
    // failed.
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    // deleting segment units are good
    CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long volumeSize = 1;
    long segSize = 1;
    boolean createSegmentUnitsStatus = true;
    try {
      VolumeCreationRequest volumeRequest = new VolumeCreationRequest(1L, volumeSize,
          VolumeType.REGULAR, 1L, true);
      volumeRequest.setSegmentSize(segSize);
      volumeRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          segId2InstancesFromRemote = new HashMap<>();
      for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
          currentSegIndex++) {
        List<InstanceIdAndEndPointThrift> instancesThrift = buildInstanceIdAndEndPoint(
            buildInstances(5));
        Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
            new HashMap<>();
        unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, instancesThrift);
        unitType2InstanceMap
            .put(SegmentUnitTypeThrift.Arbiter, new ArrayList<InstanceIdAndEndPointThrift>());
        segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
      }

      // create segment unit should succeed without throwing any exceptions
      testStatusForRetryFail = true;
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    } catch (InvalidInputException e) {
      e.printStackTrace();
    } catch (InternalErrorException e) {
      e.printStackTrace();
    } catch (GenericThriftClientFactoryException e) {
      e.printStackTrace();
    } catch (InternalErrorThrift internalErrorThrift) {
      internalErrorThrift.printStackTrace();
    } catch (RuntimeException e) {
      logger.error("RuntimeException :", e);
      createSegmentUnitsStatus = false;
    } catch (Exception e) {
      logger.error("caught exception", e);

    }

    assertTrue(!createSegmentUnitsStatus);

  }

  /**
   * Test extend volume on 2 copy volume when take any exceptions, means unit test failed.
   */
  @Test
  public void testCreateSegmentUnits_ExtendVolume_2Copy_Success() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    CreateVolumeManager createVolumeManager = null;
    try {
      createVolumeManager = setTesEnv(syncService, asyncService);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    long segSize = 1073741824;
    long volumeSize = 100 * segSize;

    //construct extend request
    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(2L, volumeSize,
        VolumeType.SMALL, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.EXTEND_VOLUME);
    volumeRequest.setRootVolumeId(1);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(2));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, buildInstanceIdAndEndPoint(buildInstances(1)));
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    //the volume which to extend
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(1);
    volumeMetadata.setVolumeSize(segSize);
    volumeMetadata.setAccountId(10);
    volumeMetadata.setVolumeType(VolumeType.SMALL);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setSegmentSize(segSize);

    volumeMetadata.setSegmentTable(new HashMap<>());

    when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);

    // create segment unit should succeed without throwing any exceptions
    testStatusForExtend = true;
    createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);

    assert (true);
  }

  /**
   * Test extend volume failed on 2 copy volume when take any exceptions, means unit test failed.
   */
  @Test
  public void testCreateSegmentUnits_ExtendVolume_2Copy_Failed() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    final SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    asyncService.setTimeout(1000);
    String errMsg = "self fail";
    asyncService.exceptionToThrow = new RuntimeException(errMsg);
    asyncService.timesToThrowException = new AtomicInteger(0);

    CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long segSize = 1073741824;
    long volumeSize = 100 * segSize;

    //construct extend request
    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(2L, volumeSize,
        VolumeType.SMALL, 1L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.EXTEND_VOLUME);
    volumeRequest.setRootVolumeId(1);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(2));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, buildInstanceIdAndEndPoint(buildInstances(1)));
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    //construct root volume
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(1);
    volumeMetadata.setVolumeSize(segSize);
    volumeMetadata.setAccountId(10);
    volumeMetadata.setVolumeType(VolumeType.SMALL);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setSegmentTable(new HashMap<>());
    volumeMetadata.setSegmentSize(segSize);

    when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);

    // create segment unit should succeed without throwing any exceptions
    try {
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    } catch (RuntimeException e) {
      assert (true);
      return;
    }

    assert (false);
  }

  /**
   * Test extend volume failed on 2 copy volume when take any TTransport exceptions, means unit test
   * failed.
   */
  @Test
  public void testCreateSegmentUnits_ExtendVolume_2Copy_TransportFailed() throws Exception {
    AsyncedDataNodeServiceImplForTest asyncService = new AsyncedDataNodeServiceImplForTest();
    final SyncedDataNodeServiceImplForTest syncService = new SyncedDataNodeServiceImplForTest();

    asyncService.setTimeout(1000);
    String errMsg = "self fail";
    asyncService.exceptionToThrow = new TTransportException(errMsg);
    asyncService.timesToThrowException = new AtomicInteger(0);

    CreateVolumeManager createVolumeManager = setTesEnv(syncService, asyncService);
    long segSize = 1073741824;
    long volumeSize = 1 * segSize;

    //construct extend request
    infoCenterConfiguration.setSegmentCreatorEnabled(false);
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(2L, volumeSize,
        VolumeType.SMALL, 0L, true);
    volumeRequest.setSegmentSize(segSize);
    volumeRequest.setRequestType(VolumeCreationRequest.RequestType.EXTEND_VOLUME);
    volumeRequest.setRootVolumeId(1);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segId2InstancesFromRemote = new HashMap<>();
    for (int currentSegIndex = 0; currentSegIndex < (int) (volumeSize / segSize);
        currentSegIndex++) {
      List<InstanceIdAndEndPointThrift> normalInstancesThrift = buildInstanceIdAndEndPoint(
          buildInstances(2));
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceMap =
          new HashMap<>();
      unitType2InstanceMap.put(SegmentUnitTypeThrift.Normal, normalInstancesThrift);
      unitType2InstanceMap
          .put(SegmentUnitTypeThrift.Arbiter, buildInstanceIdAndEndPoint(buildInstances(1)));
      segId2InstancesFromRemote.put(currentSegIndex, unitType2InstanceMap);
    }

    //construct root volume
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(1);
    volumeMetadata.setVolumeSize(segSize);
    volumeMetadata.setAccountId(10);
    volumeMetadata.setVolumeType(VolumeType.SMALL);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setSegmentTable(new HashMap<>());
    volumeMetadata.setSegmentSize(segSize);

    //for extend
    when(volumeInformationManger.getVolumeNew(anyLong(), anyLong())).thenReturn(volumeMetadata);

    // create segment unit should succeed without throwing any exceptions
    try {
      createVolumeManager.createSegmentUnits(volumeRequest, segId2InstancesFromRemote, 0);
    } catch (RuntimeException e) {
      assert (true);
      return;
    }

    assert (false);
  }

  /**
   * This method that build a machine instances.
   *
   * @param instanceId the machine instance id
   */
  private InstanceMetadataThrift buildInstance(long instanceId) {
    InstanceMetadataThrift instance = new InstanceMetadataThrift();
    instance.setInstanceId(instanceId);
    instance.setEndpoint("localhost:1234" + instanceId);
    return instance;
  }

  /**
   * This method that build a cluster of machine instances.
   *
   * @param n the number of machine instance
   */
  private List<InstanceMetadataThrift> buildInstances(int n) {
    List<InstanceMetadataThrift> instancesThrift = new ArrayList<InstanceMetadataThrift>();
    for (int i = 0; i < n; i++) {
      InstanceMetadataThrift instance1 = buildInstance(i);
      instancesThrift.add(instance1);
    }
    return instancesThrift;
  }

  /**
   * This method that the master of the simulated machine.
   *
   * @param instanceId machine instance id
   */
  private List<InstanceMetadataThrift> buildInstancesForArbiter(int instanceId) {
    List<InstanceMetadataThrift> instancesThrift = new ArrayList<InstanceMetadataThrift>();
    InstanceMetadataThrift instance1 = buildInstance(instanceId);
    instancesThrift.add(instance1);
    return instancesThrift;
  }

  /**
   * This method that simulate a machine instance for building a unit test execution environment.
   *
   * @param instances machine instance information
   */
  private List<InstanceIdAndEndPointThrift> buildInstanceIdAndEndPoint(
      List<InstanceMetadataThrift> instances) {
    List<InstanceIdAndEndPointThrift> idAndEndPointList =
        new ArrayList<InstanceIdAndEndPointThrift>();
    for (InstanceMetadataThrift instance : instances) {
      InstanceIdAndEndPointThrift idAndEndPoint = new InstanceIdAndEndPointThrift();
      idAndEndPoint.setEndPoint(instance.getEndpoint());
      idAndEndPoint.setInstanceId(instance.getInstanceId());
      idAndEndPointList.add(idAndEndPoint);
    }
    return idAndEndPointList;
  }

  /**
   * Create test running basic environment.
   *
   * @param iface      datanode service
   * @param asyncIface datanode async service
   */
  private CreateVolumeManager setTesEnv(DataNodeService.Iface iface,
      DataNodeService.AsyncIface asyncIface)
      throws Exception {
    infoCenterConfiguration = new InfoCenterConfiguration();
    CreateVolumeManager createVolumeManager = new CreateVolumeManager();
    createVolumeManager.setAccountStore(accountStore);
    createVolumeManager.setVolumeStore(volumeStore);
    createVolumeManager.setInfoCenterConfiguration(infoCenterConfiguration);

    createVolumeManager.setInfoCenterConfiguration(infoCenterConfiguration);
    createVolumeManager.setPasselCount(10);
    createVolumeManager.setDataNodeClientFactory(dataNodeClientFactory);
    createVolumeManager.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory);
    /* set the time out for wait data node**/
    createVolumeManager.setCreateVolumeTimeOutSecond(10);
    createVolumeManager.setThriftTimeOut(10);
    createVolumeManager.setCreateSegmentUnitThreadPoolCoreSize(2);
    createVolumeManager.setCreateSegmentUnitThreadPoolMaxSize(50);
    createVolumeManager.setVolumeInformationManger(volumeInformationManger);

    createVolumeManager.init();

    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(iface);
    when(dataNodeAsyncClientFactory.generateAsyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(asyncIface);
    when(infoCenterClientFactory.build()).thenReturn(infocenterWrapper);
    AccountMetadata accountMetadata4Test = new AccountMetadata();
    accountMetadata4Test.setAccountId(0L);
    accountMetadata4Test.setAccountType("Admin");
    VolumeMetadata volumeMetadata = generateVolumeMetadata(VolumeType.REGULAR);
    VolumeMetadataThrift volumeMetadataThrift = RequestResponseHelper
        .buildThriftVolumeFrom(volumeMetadata, true);
    GetVolumeResponse getVolumeResponse = new GetVolumeResponse();
    getVolumeResponse.setRequestId(1L);
    getVolumeResponse.setVolumeMetadata(volumeMetadataThrift);
    getVolumeResponse.setDriverMetadatas(new ArrayList<DriverMetadataThrift>());

    //init volume
    when(volumeStore.getVolume(anyLong())).thenReturn(volumeMetadata);

    createVolumeManager.setDataNodeClientFactory(dataNodeClientFactory);
    createVolumeManager.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory);
    return createVolumeManager;
  }

  /**
   * Create test running basic environment.
   *
   * @param iface             datanode service provider api
   * @param asyncIfaceForMock mocked apis
   * @param asyncIface        async api
   * @param segmentUnitSize   segment size for volume
   */
  private CreateVolumeManager setTesEnvNew(DataNodeService.Iface iface,
      DataNodeService.AsyncIface asyncIfaceForMock,
      DataNodeService.AsyncIface asyncIface, long segmentUnitSize)
      throws Exception {
    infoCenterConfiguration = new InfoCenterConfiguration();
    CreateVolumeManager createVolumeManager = new CreateVolumeManager();

    createVolumeManager.setAccountStore(accountStore);
    createVolumeManager.setInfoCenterConfiguration(infoCenterConfiguration);

    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(iface);
    when(dataNodeAsyncClientFactory.generateAsyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(asyncIfaceForMock);
    AccountMetadata accountMetadata4Test = new AccountMetadata();
    accountMetadata4Test.setAccountId(0L);
    accountMetadata4Test.setAccountType("Admin");
    VolumeMetadata volumeMetadata = generateVolumeMetadata(VolumeType.REGULAR);
    VolumeMetadataThrift volumeMetadataThrift = RequestResponseHelper
        .buildThriftVolumeFrom(volumeMetadata, true);
    GetVolumeResponse getVolumeResponse = new GetVolumeResponse();
    getVolumeResponse.setRequestId(1L);
    getVolumeResponse.setVolumeMetadata(volumeMetadataThrift);
    getVolumeResponse.setDriverMetadatas(new ArrayList<DriverMetadataThrift>());

    createVolumeManager.setDataNodeClientFactory(dataNodeClientFactory);
    createVolumeManager.setDataNodeClientFactory(dataNodeClientFactory);
    createVolumeManager.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] arguments = invocation.getArguments();
        CreateSegmentUnitBatchRequest createSegmentUnitBatchRequest =
            (CreateSegmentUnitBatchRequest) arguments[0];
        List<CreateSegmentUnitNode> createSegmentUnitNodeList = createSegmentUnitBatchRequest
            .getSegmentUnits();
        logger.warn("get the SegmentUnitNode number :{}", createSegmentUnitNodeList.size());
        if (createSegmentUnitNodeList.size() == (int) segmentUnitSize) {
          testStatusForGetCreateSegmentUnitRequest = true;
        }
        assertTrue(createSegmentUnitNodeList.size() == (int) segmentUnitSize);
        return asyncIface;
      }
    }).when(asyncIfaceForMock).createSegmentUnitBatch(any(CreateSegmentUnitBatchRequest.class),
        any(AsyncMethodCallback.class));
    return createVolumeManager;
  }

  /**
   * According to volume type get a volume metadata.
   *
   * @param volumeType volume type
   */
  private VolumeMetadata generateVolumeMetadata(VolumeType volumeType) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setAccountId(RequestIdBuilder.get());
    volumeMetadata.setVolumeId(RequestIdBuilder.get());
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());
    volumeMetadata.setName("volume_test");
    volumeMetadata.setVolumeSize(0);
    volumeMetadata.setVolumeType(volumeType);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setVersion(1);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setVolumeSource(CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setPageWrappCount(128);
    volumeMetadata.setSegmentWrappCount(10);
    volumeMetadata.setInAction(NULL);

    return volumeMetadata;
  }

  private class SyncedDataNodeServiceImplForTest extends DataNodeService.Client {

    protected Exception exceptionToThrow;
    protected Long delay;
    protected AtomicInteger timeBeingInvoked = new AtomicInteger();
    protected AtomicInteger timesToThrowException;

    public SyncedDataNodeServiceImplForTest() {
      super(null);
    }

    @Override
    public DeleteSegmentUnitResponse deleteSegmentUnit(DeleteSegmentUnitRequest request)
        throws SegmentNotFoundExceptionThrift, py.thrift.share.ServiceHavingBeenShutdownThrift,
        py.thrift.share.StaleMembershipExceptionThrift, py.thrift.share.InternalErrorThrift,
        TException {
      timeBeingInvoked.incrementAndGet();
      if (exceptionToThrow != null && timesToThrowException.getAndDecrement() > 0) {
        if (exceptionToThrow instanceof SegmentExistingExceptionThrift) {
          throw (SegmentExistingExceptionThrift) exceptionToThrow;
        }
        if (exceptionToThrow instanceof ServiceHavingBeenShutdownThrift) {
          throw (ServiceHavingBeenShutdownThrift) exceptionToThrow;
        }
        if (exceptionToThrow instanceof StaleMembershipExceptionThrift) {
          throw (StaleMembershipExceptionThrift) exceptionToThrow;
        }
        if (exceptionToThrow instanceof InternalErrorThrift) {
          throw (InternalErrorThrift) exceptionToThrow;
        }
        throw new TException();
      }

      try {
        if (delay != null) {
          Thread.sleep(delay);
        } else {
          // by default, it sleeps 100 ms to emulate the time to take to process the request
          Thread.sleep(100);
        }
      } catch (InterruptedException e) {
        logger.error("interruptted", e);
      }

      return new DeleteSegmentUnitResponse(1L);
    }
  }

  private class AsyncedDataNodeServiceImplForTest extends DataNodeService.AsyncClient {

    protected Exception exceptionToThrow;
    protected Long delay;
    protected AtomicInteger timeBeingInvoked = new AtomicInteger(0);
    protected AtomicInteger timesToThrowException;

    public AsyncedDataNodeServiceImplForTest() {
      super(null, null, null);
    }

    @Override
    public void createSegmentUnit(CreateSegmentUnitRequest request,
        AsyncMethodCallback resultHandler)
        throws TException {
      timeBeingInvoked.incrementAndGet();

      final AsyncMethodCallback<createSegmentUnit_call> handler = resultHandler;
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          createSegmentUnit_call call = mock(createSegmentUnit_call.class);
          try {
            if (exceptionToThrow != null && timesToThrowException.getAndDecrement() > 0) {
              if (exceptionToThrow instanceof SegmentExistingExceptionThrift) {
                when(call.getResult())
                    .thenThrow((SegmentExistingExceptionThrift) exceptionToThrow);
              } else {
                when(call.getResult()).thenThrow((InternalErrorThrift) exceptionToThrow);
              }
            } else {
              CreateSegmentUnitResponse response = new CreateSegmentUnitResponse(1L);
              when(call.getResult()).thenReturn(response);
            }
          } catch (TException e) {
            logger.error("caught exception", e);
          }

          if (exceptionToThrow instanceof TimeoutException) {
            handler.onError((TimeoutException) exceptionToThrow);
          } else if (exceptionToThrow != null) {
            handler.onError(exceptionToThrow);
          } else {
            handler.onComplete(call);
          }
        }
      }, delay != null ? delay : 100);
    }

    @Override
    public void createSegmentUnitBatch(CreateSegmentUnitBatchRequest request,
        AsyncMethodCallback resultHandler) throws TException {
      timeBeingInvoked.incrementAndGet();

      final AsyncMethodCallback<createSegmentUnitBatch_call> handler = resultHandler;
      Timer timer = new Timer();
      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          createSegmentUnitBatch_call call = mock(createSegmentUnitBatch_call.class);
          try {
            if (exceptionToThrow != null && timesToThrowException.getAndDecrement() > 0) {
              if (exceptionToThrow instanceof SegmentExistingExceptionThrift) {
                when(call.getResult())
                    .thenThrow((SegmentExistingExceptionThrift) exceptionToThrow);
              } else {
                when(call.getResult()).thenThrow((InternalErrorThrift) exceptionToThrow);
              }
            } else {
              long requestId = 2L;
              long volumeId = 1L;

              //each test unit
              if (testStatusForWithControllerAndReport) {
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                for (int i = 0; i < 1000; i++) {
                  successedSegs.add(i);
                }
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              if (testStatusForFailedToCreateSegmentUnits) {
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                  CreateSegmentUnitFailedNode createSegmentUnitFailedNode =
                      new CreateSegmentUnitFailedNode();
                  createSegmentUnitFailedNode.setSegIndex(i);
                  createSegmentUnitFailedNode
                      .setErrorCode(CreateSegmentUnitFailedCodeThrift.NotEnoughSpaceException);
                  failedSegs.add(createSegmentUnitFailedNode);
                }
                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              //two good, one error
              if (testStatusForQuorumNumberOk) {
                try {
                  Thread.sleep(random.nextInt(1000));
                } catch (InterruptedException e) {
                  logger.warn("sleep error");
                }
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                if (testGoodNumber.get() == 2) {
                  CreateSegmentUnitFailedNode createSegmentUnitFailedNode =
                      new CreateSegmentUnitFailedNode();
                  createSegmentUnitFailedNode.setSegIndex(0);
                  createSegmentUnitFailedNode
                      .setErrorCode(CreateSegmentUnitFailedCodeThrift.NotEnoughSpaceException);
                  failedSegs.add(createSegmentUnitFailedNode);
                } else {
                  successedSegs.add(0);
                  testGoodNumber.incrementAndGet();
                }

                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              //one good, two SegmentExistingException
              if (testStatusForSegmentExistingException) {
                try {
                  Thread.sleep(random.nextInt(1000));
                } catch (InterruptedException e) {
                  logger.warn("sleep error");
                }
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                if (testGoodNumber.get() == 0) {
                  successedSegs.add(0);
                  testGoodNumber.incrementAndGet();
                } else {
                  CreateSegmentUnitFailedNode createSegmentUnitFailedNode =
                      new CreateSegmentUnitFailedNode();
                  createSegmentUnitFailedNode.setSegIndex(0);
                  createSegmentUnitFailedNode
                      .setErrorCode(CreateSegmentUnitFailedCodeThrift.SegmentExistingException);
                  failedSegs.add(createSegmentUnitFailedNode);
                  testGoodNumber.incrementAndGet();
                }

                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              // 1. one good, two error, to get the next SegmentUnit Type to retry
              if (testStatusForRetryOk) {
                try {
                  Thread.sleep(random.nextInt(1000));
                } catch (InterruptedException e) {
                  logger.warn("sleep error");
                }
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                if (setNumberForRetryOk.get() < 3) {
                  if (testGoodNumber.get() == 0) {
                    successedSegs.add(0);
                    testGoodNumber.incrementAndGet();
                  } else {
                    CreateSegmentUnitFailedNode createSegmentUnitFailedNode =
                        new CreateSegmentUnitFailedNode();
                    createSegmentUnitFailedNode.setSegIndex(0);
                    createSegmentUnitFailedNode
                        .setErrorCode(CreateSegmentUnitFailedCodeThrift.NotEnoughSpaceException);
                    failedSegs.add(createSegmentUnitFailedNode);
                    testGoodNumber.incrementAndGet();
                  }

                  setNumberForRetryOk.incrementAndGet();
                } else { //retry ,all good
                  successedSegs.add(0);
                }

                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              // 1. one good, two error, to get the next SegmentUnit Type to retry
              if (testStatusForRetryFail) {
                try {
                  Thread.sleep(random.nextInt(1000));
                } catch (InterruptedException e) {
                  logger.warn("sleep error");
                }
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                if (setNumberForRetryFail.get() < 3) {
                  if (testGoodNumber.get() == 0) {
                    successedSegs.add(0);
                    logger.warn("i am going ------------");
                  } else {
                    CreateSegmentUnitFailedNode createSegmentUnitFailedNode =
                        new CreateSegmentUnitFailedNode();
                    createSegmentUnitFailedNode.setSegIndex(0);
                    createSegmentUnitFailedNode
                        .setErrorCode(CreateSegmentUnitFailedCodeThrift.NotEnoughSpaceException);
                    failedSegs.add(createSegmentUnitFailedNode);
                  }

                  testGoodNumber.incrementAndGet();
                  setNumberForRetryFail.get();
                } else { //2. retry ,all error
                  CreateSegmentUnitFailedNode createSegmentUnitFailedNode =
                      new CreateSegmentUnitFailedNode();
                  createSegmentUnitFailedNode.setSegIndex(0);
                  createSegmentUnitFailedNode
                      .setErrorCode(CreateSegmentUnitFailedCodeThrift.NotEnoughSpaceException);
                  failedSegs.add(createSegmentUnitFailedNode);
                }

                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              //each test unit
              if (testStatusForExtend) {
                CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                //the extend ok
                for (int i = 1; i < 101; i++) {
                  successedSegs.add(i);
                }
                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }

              if (normalStatus) {
                final CreateSegmentUnitBatchResponse response =
                    new CreateSegmentUnitBatchResponse();
                List<Integer> successedSegs = new ArrayList<>();
                successedSegs.add(0);
                successedSegs.add(1);
                successedSegs.add(2);

                List<CreateSegmentUnitFailedNode> failedSegs = new ArrayList<>();
                response.setSuccessedSegs(successedSegs);
                response.setFailedSegs(failedSegs);
                when(call.getResult()).thenReturn(response);
              }
            }
          } catch (TException e) {
            logger.error("caught exception", e);
          }

          if (exceptionToThrow instanceof TimeoutException) {
            handler.onError((TimeoutException) exceptionToThrow);
          } else if (exceptionToThrow != null) {
            handler.onError(exceptionToThrow);
          } else {
            handler.onComplete(call);
          }
        }
      }, delay != null ? delay : 100);
    }
  }

}
