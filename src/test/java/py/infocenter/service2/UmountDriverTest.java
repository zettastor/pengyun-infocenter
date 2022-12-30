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
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.drivercontainer.client.DriverContainerServiceBlockingClientWrapper;
import py.icshare.exception.FlowerNotFoundException;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.DriverStore;
import py.infocenter.store.control.OperationStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.test.TestBase;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.share.DriverIpTargetThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.ExistsClientExceptionThrift;
import py.thrift.share.UmountDriverRequestThrift;
import py.thrift.share.UmountDriverResponseThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.volume.VolumeMetadata;

/**
 * A class includes some test for interface {@code ControlCenter.Iface.umountDriver}.
 */
public class UmountDriverTest extends TestBase {

  @Mock
  private InstanceStore instanceStore;

  @Mock
  private InformationCenterClientFactory icClientFactory;

  @Mock
  private DriverContainerClientFactory dcClientFactory;
  @Mock
  private DriverContainerServiceBlockingClientWrapper dcClientWrapper;
  @Mock
  private DriverContainer.Iface dcClient;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private OperationStore operationStore;

  @Mock
  private PySecurityManager securityManager;

  private MyInformationCenterImpl informationCenter;

  @Mock
  private VolumeInformationManger volumeInformationManger;

  @Mock
  private DriverStore driverStore;

  private LockForSaveVolumeInfo lockForSaveVolumeInfo;


  @Before
  public void init() throws Exception {
    super.init();
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();

    informationCenter = new MyInformationCenterImpl();
    informationCenter.setDriverContainerClientFactory(dcClientFactory);
    informationCenter.setInstanceStore(instanceStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setOperationStore(operationStore);
    informationCenter.setVolumeInformationManger(volumeInformationManger);
    informationCenter.setDriverStore(driverStore);
    informationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    when(dcClientFactory.build(any(EndPoint.class))).thenReturn(dcClientWrapper);
    when(dcClientFactory.build(any(EndPoint.class), anyLong())).thenReturn(dcClientWrapper);
    when(dcClientWrapper.getClient()).thenReturn(dcClient);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * Test the case in which request to umount nbd driver with existing clients should be refused.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void umountnbddriverwithexistingclients() throws Exception {
    Long driverContainerId = setVolumeAndDrivers(DriverType.NBD);

    Set<Instance> dcInstance = buildInstances(driverContainerId, 1);
    when(dcClient.umountDriver(any(UmountDriverRequestThrift.class)))
        .thenThrow(new ExistsClientExceptionThrift());
    when(instanceStore.getAll(anyString())).thenReturn(dcInstance);

    List<DriverIpTargetThrift> driverIpTargetList = new ArrayList<DriverIpTargetThrift>();
    driverIpTargetList
        .add(new DriverIpTargetThrift(0, dcInstance.iterator().next().getEndPoint().getHostName(),
            DriverTypeThrift.NBD, driverContainerId));

    UmountDriverRequestThrift request = new UmountDriverRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(RequestIdBuilder.get());
    request.setDriverIpTargetList(driverIpTargetList);

    boolean exceptionCached = false;
    try {
      informationCenter.umountDriver(request);
    } catch (ExistsClientExceptionThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);
  }

  /**
   * Test the case in which the request to umount iscsi driver specified iscsi host.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void umountiscsidriveronspecifichost() throws Exception {
    Long driverContainerId = setVolumeAndDrivers(DriverType.ISCSI);

    Set<Instance> dcInstance = buildInstances(driverContainerId, 1);
    when(instanceStore.getAll(anyString())).thenReturn(dcInstance);

    List<DriverIpTargetThrift> driverIpTargetList = new ArrayList<DriverIpTargetThrift>();
    driverIpTargetList
        .add(new DriverIpTargetThrift(0, dcInstance.iterator().next().getEndPoint().getHostName(),
            DriverTypeThrift.ISCSI, driverContainerId));

    UmountDriverRequestThrift request = new UmountDriverRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(RequestIdBuilder.get());
    request.setDriverIpTargetList(driverIpTargetList);

    UmountDriverResponseThrift response = new UmountDriverResponseThrift();
    when(dcClient.umountDriver(any(UmountDriverRequestThrift.class)))
        .thenReturn(response);

    informationCenter.umountDriver(request);

    Mockito.verify(dcClient, Mockito.times(1)).umountDriver(any(UmountDriverRequestThrift.class));
  }

  /**
   * Test the case in which the request to umount iscsi driver does not specify iscsi host.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void umountalliscsidriver() throws Exception {
    Long driverContainerId = setVolumeAndDrivers(DriverType.NBD);
    Set<Instance> dcInstances = buildInstances(driverContainerId, 2);

    List<DriverIpTargetThrift> driverIpTargetList = new ArrayList<DriverIpTargetThrift>();
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();

    for (Instance instance : dcInstances) {
      DriverMetadata driver = buildDriver(driverContainerId, 0, DriverType.ISCSI);
      driver.setHostName(instance.getEndPoint().getHostName());
      driverList.add(driver);
      driverIpTargetList.add(new DriverIpTargetThrift(0, instance.getEndPoint().getHostName(),
          DriverTypeThrift.ISCSI, driverContainerId));
    }

    when(instanceStore.getAll(anyString())).thenReturn(dcInstances);

    UmountDriverRequestThrift request = new UmountDriverRequestThrift();
    request.setRequestId(RequestIdBuilder.get());
    request.setVolumeId(RequestIdBuilder.get());
    request.setDriverIpTargetList(driverIpTargetList);

    UmountDriverResponseThrift response = new UmountDriverResponseThrift();
    when(dcClient.umountDriver(any(UmountDriverRequestThrift.class)))
        .thenReturn(response);

    informationCenter.umountDriver(request);

    Mockito.verify(dcClient, Mockito.times(2)).umountDriver(any(UmountDriverRequestThrift.class));
  }

  private Set<Instance> buildInstances(Long driverContainerId, int amount) {
    Set<Instance> instances = new HashSet<>();
    for (int i = 0; i < amount; i++) {
      Instance instance = new Instance(new InstanceId(i), PyService.DRIVERCONTAINER.name(),
          InstanceStatus.HEALTHY,
          new EndPoint("10.0.1." + i, i));
      instance.putEndPointByServiceName(PortType.IO, instance.getEndPoint());
      instance.setId(new InstanceId(driverContainerId));
      instances.add(instance);
    }

    return instances;
  }

  private DriverMetadata buildDriver(Long driverContainerId, int snapshotId, DriverType type) {
    DriverMetadata driver = new DriverMetadata();
    driver.setVolumeId(RequestIdBuilder.get());
    driver.setDriverType(type);
    driver.setDriverContainerId(driverContainerId);
    driver.setSnapshotId(snapshotId);

    return driver;
  }


  public Long setVolumeAndDrivers(DriverType type)
      throws VolumeNotFoundException, FlowerNotFoundException {
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    Long driverContainerId = RequestIdBuilder.get();
    driverList.add(buildDriver(driverContainerId, 0, type));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = new VolumeMetadata();
    volume.setName("test");
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    return driverContainerId;
  }

  class MyInformationCenterImpl extends InformationCenterImpl {

    @Override
    public void markDriverStatus(long volumeId, List<DriverIpTargetThrift> driverIpTargetList)
        throws VolumeNotFoundExceptionThrift {
      return;
    }
  }
}
