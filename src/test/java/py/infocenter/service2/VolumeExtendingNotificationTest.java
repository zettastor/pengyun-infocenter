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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.client.CoordinatorClientFactory.CoordinatorClientWrapper;
import py.icshare.VolumeCreationRequest;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.test.TestBase;
import py.thrift.coordinator.service.Coordinator;
import py.thrift.coordinator.service.UpdateVolumeOnExtendingRequest;
import py.thrift.infocenter.service.ExtendVolumeRequest;
import py.thrift.share.BadLicenseTokenExceptionThrift;
import py.thrift.share.DomainIsDeletingExceptionThrift;
import py.thrift.share.DomainNotExistedExceptionThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.NotEnoughGroupExceptionThrift;
import py.thrift.share.NotEnoughLicenseTokenExceptionThrift;
import py.thrift.share.RootVolumeBeingDeletedExceptionThrift;
import py.thrift.share.RootVolumeNotFoundExceptionThrift;
import py.thrift.share.StoragePoolIsDeletingExceptionThrift;
import py.thrift.share.StoragePoolNotExistInDoaminExceptionThrift;
import py.thrift.share.StoragePoolNotExistedExceptionThrift;
import py.thrift.share.UselessLicenseExceptionThrift;
import py.thrift.share.VolumeExistingExceptionThrift;
import py.thrift.share.VolumeIsCloningExceptionThrift;
import py.thrift.share.VolumeNameExistedExceptionThrift;
import py.volume.CacheType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * A class contains some tests for volume extending notification.
 *
 */
public class VolumeExtendingNotificationTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(VolumeExtendingNotificationTest.class);

  private final int segmentSize = 1024;

  private VolumeStore volumeStore = mock(VolumeStore.class);

  private TestInformationCenterImpl testInformationCenter = new TestInformationCenterImpl(
      volumeStore);

  @Mock
  private PySecurityManager securityManager;
  @Mock
  private VolumeInformationManger volumeInformationManger;

  @Mock
  private VolumeJobStoreDb volumeJobStoreDb;
  @Mock
  private OperationStore operationStore;

  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private VolumeMetadata volume;


  public void init() throws Exception {
    super.init();
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();

    testInformationCenter.setSecurityManager(securityManager);
    testInformationCenter.setVolumeInformationManger(volumeInformationManger);
    testInformationCenter.setVolumeJobStoreDb(volumeJobStoreDb);
    testInformationCenter.setVolumeStore(volumeStore);
    testInformationCenter.setOperationStore(operationStore);
    testInformationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);
    testInformationCenter.setAppContext(appContext);

    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
  }


  @Test
  public void notifyCoordinator() throws Exception {
    testInformationCenter.setSegmentSize(segmentSize);

    when(volume.getVolumeType()).thenReturn(VolumeType.REGULAR);
    when(volume.getInAction()).thenReturn(VolumeInAction.NULL);
    when(volume.isVolumeAvailable()).thenReturn(true);

    List<DriverMetadata> driverLst = stubDrivers(3);

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverLst);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);

    // generage instance containing all coordinator binded the volume
    Set<Instance> instances = stubInstances(10);
    InstanceStore instanceStore = mock(InstanceStore.class);
    when(instanceStore.getAll(anyString(), any(InstanceStatus.class))).thenReturn(instances);
    testInformationCenter.setInstanceStore(instanceStore);

    CoordinatorClientFactory coordinatorClientFactory = mock(CoordinatorClientFactory.class);
    CoordinatorClientWrapper coordinatorClientWrapper = mock(CoordinatorClientWrapper.class);
    Coordinator.Iface coordinator = mock(Coordinator.Iface.class);
    when(coordinatorClientFactory.build(any(EndPoint.class))).thenReturn(coordinatorClientWrapper);
    when(coordinatorClientWrapper.getClient()).thenReturn(coordinator);
    testInformationCenter.setCoordinatorClientFactory(coordinatorClientFactory);

    ExtendVolumeRequest request = stubExtendVolumeRequest();
    testInformationCenter.extendVolume(request);

    verify(coordinator, times(3))
        .updateVolumeOnExtending(any(UpdateVolumeOnExtendingRequest.class));

    // generage instance containing partial coordinator binded the volume
    instances = stubInstances(2);
    when(instanceStore.getAll(anyString(), any(InstanceStatus.class))).thenReturn(instances);
    testInformationCenter.extendVolume(request);

    verify(coordinator, times(3 + 2 * 3))
        .updateVolumeOnExtending(any(UpdateVolumeOnExtendingRequest.class));
  }

  private ExtendVolumeRequest stubExtendVolumeRequest() {
    ExtendVolumeRequest request = new ExtendVolumeRequest();
    request.setAccountId(0L);
    request.setVolumeId(0L);
    request.setExtendSize(segmentSize);
    request.setDomainId(0);
    request.setStoragePoolId(0L);

    return request;
  }

  private List<DriverMetadata> stubDrivers(int n) {
    List<DriverMetadata> driverLst = new ArrayList<DriverMetadata>();

    for (int i = 0; i < n; i++) {
      DriverMetadata driver = new DriverMetadata();
      driver.setHostName(String.format("0.0.0.%s", i));
      driver.setCoordinatorPort(i);

      driverLst.add(driver);
    }

    return driverLst;
  }

  private Set<Instance> stubInstances(int n) {
    Set<Instance> instances = new HashSet<Instance>();

    for (int i = 0; i < n; i++) {
      EndPoint endPoint = new EndPoint(String.format("0.0.0.%s", i), i);
      Instance instance = new Instance(new InstanceId((long) i), PyService.COORDINATOR.name(),
          InstanceStatus.HEALTHY,
          endPoint);
      instance.putEndPointByServiceName(PortType.IO, endPoint);
      instances.add(instance);
    }

    return instances;
  }

  private class TestInformationCenterImpl extends InformationCenterImpl {

    public TestInformationCenterImpl(VolumeStore volumeStore) {
      super();
    }

    protected void grantToken(long accountId, long expectedSize)
        throws BadLicenseTokenExceptionThrift,
        UselessLicenseExceptionThrift, NotEnoughLicenseTokenExceptionThrift, TException {
      return;
    }

    @Override
    protected void makeVolumeCreationRequestAndcreateVolume(VolumeCreationRequest volumeRequest)
        throws InternalErrorThrift, InvalidInputExceptionThrift, VolumeNameExistedExceptionThrift,
        VolumeExistingExceptionThrift, RootVolumeBeingDeletedExceptionThrift,
        RootVolumeNotFoundExceptionThrift, StoragePoolNotExistInDoaminExceptionThrift,
        DomainNotExistedExceptionThrift, StoragePoolNotExistedExceptionThrift,
        DomainIsDeletingExceptionThrift, StoragePoolIsDeletingExceptionThrift,
        NotEnoughGroupExceptionThrift, VolumeIsCloningExceptionThrift, TException {
    }
  }
}
