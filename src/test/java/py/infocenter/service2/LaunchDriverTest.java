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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.drivercontainer.client.DriverContainerServiceBlockingClientWrapper;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.DriverStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.instance.InstanceStatus;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.share.DriverAmountAndHostNotFitThrift;
import py.thrift.share.DriverHostCannotUseThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.LaunchDriverResponseThrift;
import py.thrift.share.SystemMemoryIsNotEnoughThrift;
import py.thrift.share.TooManyDriversExceptionThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * A class including some tests for interface {@code ControlCenter.Iface.launchDriver}.
 */
public class LaunchDriverTest extends TestBase {

  public List<DriverContainerCandidate> driverContainerCandidateList = new ArrayList<>();
  @Mock
  private VolumeStore volumeStore;
  @Mock
  private InformationCenterClientFactory icClientFactory;
  @Mock
  private DriverContainerClientFactory dcClientFactory;
  @Mock
  private DriverContainerServiceBlockingClientWrapper dcClientWrapper;
  @Mock
  private DriverContainer.Iface dcClient;
  @Mock
  private OperationStore operationStore;
  private MyInformationCenterImpl informationCenter;
  @Mock
  private PySecurityManager securityManager;
  @Mock
  private VolumeInformationManger volumeInformationManger;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private DriverContainerClientFactory driverContainerClientFactory;
  @Mock
  private DriverStore driverStore;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;



  @Before
  public void init() throws Exception {
    super.init();
    driverContainerCandidateList.clear();

    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();

    informationCenter = new MyInformationCenterImpl();
    informationCenter.setDriverContainerClientFactory(dcClientFactory);
    informationCenter.setOperationStore(operationStore);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setVolumeStore(volumeStore);
    informationCenter.setVolumeInformationManger(volumeInformationManger);
    informationCenter.setAppContext(appContext);
    informationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);
    informationCenter.setDriverStore(driverStore);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(dcClientFactory.build(any(EndPoint.class))).thenReturn(dcClientWrapper);
    when(dcClientFactory.build(any(EndPoint.class), anyLong())).thenReturn(dcClientWrapper);
    when(dcClientWrapper.getClient()).thenReturn(dcClient);
    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);

    //set driverContainer
    when(driverContainerClientFactory.build(any(EndPoint.class), anyLong()))
        .thenReturn(dcClientWrapper);

  }

  @After
  public void clean() throws Exception {
    driverContainerCandidateList.clear();
  }

  /**
   * Test the case in which infocenter allocate less amount of driver container than amount of
   * drivers to launch. Expect control center to throw {@link TooManyDriversExceptionThrift}.
   */
  @Test
  public void allocateLittleDriverContainer() throws Exception {
    String hosts = null;
    boolean exceptionCached = false;

    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.NBD, "test"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);

    /* set the ok DriverContainer is empty ***/
    buildAllocDriverContainerResponse(0);
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.NBD, 1, hosts));
    } catch (TooManyDriversExceptionThrift e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);

    // information center allocate 1 driver container to launch 2 iscsi driver
    exceptionCached = false;
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 2, hosts));
    } catch (TooManyDriversExceptionThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);
  }

  /**
   * Test the case in which candidate drivercontainer is out of memory Expect control center to
   * throw {@link SystemMemoryIsNotEnoughThrift}.
   */
  @Test
  public void launchDriverWithNoEnoughMemory() throws SystemMemoryIsNotEnoughThrift, Exception {
    String hosts = null;

    //current volume has launch Drivers
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.NBD, "test"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    //the DriverContain return the SystemMemoryIsNotEnough
    when(dcClientWrapper.getClient().launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenThrow(new SystemMemoryIsNotEnoughThrift());

    buildAllocDriverContainerResponse(1);

    boolean exceptionCached = false;
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.NBD, 1, hosts));
    } catch (SystemMemoryIsNotEnoughThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);
  }

  /**
   * Test case in which a volume launched an iscsi driver before, when try to launch a nbd driver,
   * driverc only allow launch once.
   */
  @Test
  public void launchnbdbaseonlaunchediscsi() throws Exception {
    final String hosts = null;

    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName0"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(1);

    // set the  driverContainer LaunchDriverResponse
    LaunchDriverResponseThrift launchDriverResponseThrift = mock(
        LaunchDriverResponseThrift.class);
    when(launchDriverResponseThrift.isSetRelatedDriverContainerIds()).thenReturn(true);
    Set<Long> driverContainerIds = new HashSet<>();
    driverContainerIds.add(RequestIdBuilder.get());
    when(launchDriverResponseThrift.getRelatedDriverContainerIds()).thenReturn(driverContainerIds);
    when(launchDriverResponseThrift.getRelatedDriverContainerIdsSize())
        .thenReturn(driverContainerIds.size());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);

    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.NBD, 1, hosts));

    Mockito.verify(dcClient, Mockito.times(1)).launchDriver(any(LaunchDriverRequestThrift.class));
  }

  /**
   * Test case in which a volume launched an iscsi driver before, when try to launch a nbd driver,
   * on diff driverc with diff drivertype.
   */
  @Test
  public void launchnbdbaseonlaunchediscsi2() throws Exception {
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName3"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(1);

    // set the  driverContainer LaunchDriverResponse
    LaunchDriverResponseThrift launchDriverResponseThrift = mock(
        LaunchDriverResponseThrift.class);
    when(launchDriverResponseThrift.isSetRelatedDriverContainerIds()).thenReturn(true);
    Set<Long> driverContainerIds = new HashSet<>();
    driverContainerIds.add(RequestIdBuilder.get());
    when(launchDriverResponseThrift.getRelatedDriverContainerIds()).thenReturn(driverContainerIds);
    when(launchDriverResponseThrift.getRelatedDriverContainerIdsSize())
        .thenReturn(driverContainerIds.size());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);

    String hosts = null;
    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.NBD, 1, hosts));
    Mockito.verify(dcClient, Mockito.times(1)).launchDriver(any(LaunchDriverRequestThrift.class));
  }

  /**
   * Test case in which a volume launched an iscsi and fsd driver before, when try to launch a nbd
   * driver, control center should throw {@link TooManyDriversExceptionThrift}. one volume on one
   * driverc only allow launch once.
   */
  @Test
  public void launchnbdbaseonlaunchediscsiandfsd() throws Exception {
    final String hosts = null;
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.FSD, "hostName0"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName1"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(5);

    // set the  driverContainer LaunchDriverResponse
    LaunchDriverResponseThrift launchDriverResponseThrift = mock(
        LaunchDriverResponseThrift.class);
    when(launchDriverResponseThrift.isSetRelatedDriverContainerIds()).thenReturn(true);
    Set<Long> driverContainerIds = new HashSet<>();
    driverContainerIds.add(RequestIdBuilder.get());
    when(launchDriverResponseThrift.getRelatedDriverContainerIds()).thenReturn(driverContainerIds);
    when(launchDriverResponseThrift.getRelatedDriverContainerIdsSize())
        .thenReturn(driverContainerIds.size());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);

    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.NBD, 1, hosts));
    Mockito.verify(dcClient, Mockito.times(1)).launchDriver(any(LaunchDriverRequestThrift.class));
  }

  /**
   * Test case in which a volume launched an iscsi and fsd driver before, when try to launch a nbd
   * driver, control center should throw {@link TooManyDriversExceptionThrift}. allow one volume
   * launch on diff driverc with diff drivertype.
   */
  @Test
  public void launchnbdbaseonlaunchediscsiandfsd2() throws Exception {
    final String hosts = null;
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.FSD, "hostName3"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName4"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(2);

    LaunchDriverResponseThrift launchDriverResponseThrift = mock(
        LaunchDriverResponseThrift.class);
    when(launchDriverResponseThrift.isSetRelatedDriverContainerIds()).thenReturn(true);
    Set<Long> driverContainerIds = new HashSet<>();
    driverContainerIds.add(RequestIdBuilder.get());
    when(launchDriverResponseThrift.getRelatedDriverContainerIds()).thenReturn(driverContainerIds);
    when(launchDriverResponseThrift.getRelatedDriverContainerIdsSize())
        .thenReturn(driverContainerIds.size());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);

    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.NBD, 1, hosts));
    Mockito.verify(dcClient, Mockito.times(1)).launchDriver(any(LaunchDriverRequestThrift.class));

  }

  /**
   * Test case in which a volume launched an FSD driver before, when try to launch a FSD driver,
   * throw {@link TooManyDriversExceptionThrift}.
   */
  @Test
  public void launchfsdbaseonlaunchedfsdandiscsi() throws Exception {

    String hosts = null;
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName0"));
    driverList.add(buildDriver(DriverType.FSD, "hostName1"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);

    buildAllocDriverContainerResponse(0);
    boolean exceptionCached = false;
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.FSD, 1, hosts));
    } catch (TooManyDriversExceptionThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);

  }

  /**
   * Test case try to launch 2 FSD driver, control center should throw {@link
   * TooManyDriversExceptionThrift}.
   */
  @Test
  public void launchtwofsd() throws Exception {
    String hosts = null;
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);

    buildAllocDriverContainerResponse(0);
    boolean exceptionCached = false;
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.FSD, 2, hosts));
    } catch (TooManyDriversExceptionThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);
  }

  /**
   * Test case in which a volume launched fsd driver before, when try to launch 2 iscsi driver,
   * control center should throw {@link TooManyDriversExceptionThrift}.
   */
  @Test
  public void launchtwoiscsibaseonfsd() throws Exception {
    final String hosts = null;
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.FSD, "hostName0"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(2);

    LaunchDriverResponseThrift launchDriverResponseThrift = mock(
        LaunchDriverResponseThrift.class);
    when(launchDriverResponseThrift.isSetRelatedDriverContainerIds()).thenReturn(true);
    Set<Long> driverContainerIds = new HashSet<>();
    driverContainerIds.add(RequestIdBuilder.get());
    when(launchDriverResponseThrift.getRelatedDriverContainerIds()).thenReturn(driverContainerIds);
    when(launchDriverResponseThrift.getRelatedDriverContainerIdsSize())
        .thenReturn(driverContainerIds.size());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);

    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 2, hosts));
    Mockito.verify(dcClient, Mockito.times(2)).launchDriver(any(LaunchDriverRequestThrift.class));

  }

  /**
   * Test case successfully launch 2 iscsi driver. host is null.
   */
  @Test
  public void launch2Iscsidriver() throws Exception {
    final String hosts = null;
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(5);

    LaunchDriverResponseThrift launchDriverResponseThrift = mock(
        LaunchDriverResponseThrift.class);
    when(launchDriverResponseThrift.isSetRelatedDriverContainerIds()).thenReturn(true);
    Set<Long> driverContainerIds = new HashSet<>();
    driverContainerIds.add(RequestIdBuilder.get());
    when(launchDriverResponseThrift.getRelatedDriverContainerIds()).thenReturn(driverContainerIds);
    when(launchDriverResponseThrift.getRelatedDriverContainerIdsSize())
        .thenReturn(driverContainerIds.size());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);

    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 2, hosts));
    Mockito.verify(dcClient, Mockito.times(2)).launchDriver(any(LaunchDriverRequestThrift.class));
  }

  /**
   * Test case in which a volume launched with given hosts.
   */
  @Test
  public void launchGivenNotEnoughHosts() throws Exception {
    //hosts not enough
    String hosts1 = "hostName5";
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.FSD, "hostName1"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName2"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);

    buildAllocDriverContainerResponse(2);

    boolean exceptionCached = false;
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 2, hosts1));
    } catch (DriverAmountAndHostNotFitThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);
  }

  /**
   * Test case in which a volume launched with given hosts.
   */
  @Test
  public void launchWithNotEnoughCandidates() throws Exception {
    //candidate not enough
    String hosts4 = "hostName3";
    List<DriverMetadata> driverList = new ArrayList<DriverMetadata>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName3"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName4"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);

    buildAllocDriverContainerResponse(1);

    boolean exceptionCached = false;
    try {
      informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 1, hosts4));
    } catch (DriverHostCannotUseThrift e) {
      exceptionCached = true;
    }
    Assert.assertTrue(exceptionCached);
  }

  /**
   * Test case in which a volume launched with given hosts.
   */
  @Test
  public void launchOnepathWithGivenHosts() throws Exception {
    //one path normal
    final String hosts3 = "hostName0";
    List<DriverMetadata> driverList = new ArrayList<>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName3"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName4"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(1);

    LaunchDriverResponseThrift launchDriverResponseThrift = new LaunchDriverResponseThrift();
    launchDriverResponseThrift.addToRelatedDriverContainerIds(RequestIdBuilder.get());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);
    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 1, hosts3));
    Mockito.verify(dcClient, Mockito.times(1)).launchDriver(any(LaunchDriverRequestThrift.class));
  }

  /**
   * Test case in which a volume launched with given hosts.
   */
  @Test
  public void launchTwopathWithGivenHosts() throws Exception {
    //two path normal
    final String hosts1 = "hostName0";
    final String hosts2 = "hostName1";
    List<DriverMetadata> driverList = new ArrayList<>();
    driverList.add(buildDriver(DriverType.ISCSI, "hostName3"));
    driverList.add(buildDriver(DriverType.ISCSI, "hostName4"));

    VolumeMetadataAndDrivers volumeAndDriverInfo = new VolumeMetadataAndDrivers();
    VolumeMetadata volume = buildVolumeMetadata();
    volumeAndDriverInfo.setVolumeMetadata(volume);
    volumeAndDriverInfo.setDriverMetadatas(driverList);
    when(volumeStore.getVolume(anyLong())).thenReturn(volume);
    when(volumeInformationManger.getDriverContainVolumes(anyLong(), anyLong(), anyBoolean()))
        .thenReturn(volumeAndDriverInfo);
    when(driverStore.get(volume.getVolumeId())).thenReturn(driverList);

    buildAllocDriverContainerResponse(2);

    LaunchDriverResponseThrift launchDriverResponseThrift = new LaunchDriverResponseThrift();
    launchDriverResponseThrift.addToRelatedDriverContainerIds(RequestIdBuilder.get());
    when(dcClient.launchDriver(any(LaunchDriverRequestThrift.class)))
        .thenReturn(launchDriverResponseThrift);
    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 1, hosts1));
    Mockito.verify(dcClient, Mockito.times(1)).launchDriver(any(LaunchDriverRequestThrift.class));

    informationCenter.launchDriver(buildLaunchDriverRequest(DriverTypeThrift.ISCSI, 1, hosts2));
    Mockito.verify(dcClient, Mockito.times(2)).launchDriver(any(LaunchDriverRequestThrift.class));
  }

  private LaunchDriverRequestThrift buildLaunchDriverRequest(DriverTypeThrift type, int amount,
      String hosts) {
    LaunchDriverRequestThrift request = new LaunchDriverRequestThrift();
    request.setDriverType(type);
    request.setDriverAmount(amount);
    request.setHostName(hosts);
    return request;
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
    driver.setHostName(host);
    driver.setDriverName("test");
    return driver;
  }


  public void buildAllocDriverContainerResponse(int amountOfContainer) {
    for (int i = 0; i < amountOfContainer; i++) {
      DriverContainerCandidate driverContainerCandidate = new DriverContainerCandidate();
      driverContainerCandidate.setHostName("hostName" + i);
      driverContainerCandidate.setPort(i);
      driverContainerCandidateList.add(driverContainerCandidate);
    }
  }


  public class MyInformationCenterImpl extends InformationCenterImpl {

    @Override
    public List<DriverContainerCandidate> allocDriverContainer(long volumeId, int driverAmount,
        boolean getForScsiDriver)
        throws TooManyDriversExceptionThrift {

      return driverContainerCandidateList;
    }
  }
}
