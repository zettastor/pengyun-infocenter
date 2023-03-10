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

package py.infocenter.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Date;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import py.app.context.AppContext;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.icshare.CapacityRecord;
import py.icshare.CapacityRecordStore;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.SegmentUnitTimeoutStoreImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStatusTransitionStoreImpl;
import py.infocenter.store.VolumeStore;
import py.infocenter.worker.TimeoutSweeper;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.storage.EdRootpathSingleton;
import py.test.TestBase;
import py.test.TestUtils;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;

/**
 * Test the basic function of Segment Unit timeout store.
 *
 */
public class SegmentUnitTimeoutStoreTest extends TestBase {

  VolumeMetadata volume = null;
  SegmentMetadata seg1 = null;
  SegmentMetadata seg2 = null;
  SegmentUnitMetadata primaryUnit = null;
  SegmentUnitMetadata secondaryUnit1 = null;
  SegmentUnitMetadata secondaryUnit2 = null;
  int segUnitTimeout = 3; // timeout is 3 seconds
  int toBeCreatedTimeout = 3;
  int deadToRemoveTime = 3;
  ArrayList<Long> volumes = null;
  SegmentUnitTimeoutStore timeoutStore = null;
  TimeoutSweeper timeoutSweeper = null;
  VolumeStore volumeStore = null;
  VolumeStatusTransitionStore statusStore = null;

  @Mock
  private AppContext appContext;

  @Mock
  private DomainStore domainStore;

  @Mock
  private StorageStore storageStore;

  @Mock
  private StoragePoolStore storagePoolStore;

  @Mock
  private CapacityRecordStore capacityRecordStore;

  private MemoryVolumeStoreImpl memoryVolumeStore;

  @Mock
  private DbVolumeStoreImpl dbVolumeStore;
  @Mock
  private ServerStatusCheck serverStatusCheck;


  @Before
  public void setUp() throws Exception {
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    timeoutStore = new SegmentUnitTimeoutStoreImpl(segUnitTimeout);
    memoryVolumeStore = new MemoryVolumeStoreImpl();
    volumeStore = new TwoLevelVolumeStoreImpl(memoryVolumeStore, dbVolumeStore);

    statusStore = new VolumeStatusTransitionStoreImpl();
    timeoutSweeper = new TimeoutSweeper();
    timeoutSweeper.setVolumeStore(volumeStore);
    timeoutSweeper.setVolumeStatusStore(statusStore);
    timeoutSweeper.setSegUnitTimeoutStore(timeoutStore);
    timeoutSweeper.setAppContext(appContext);
    when(domainStore.listAllDomains()).thenReturn(new ArrayList<Domain>());
    timeoutSweeper.setDomainStore(domainStore);
    timeoutSweeper.setStorageStore(storageStore);
    when(storagePoolStore.listAllStoragePools()).thenReturn(new ArrayList<StoragePool>());
    timeoutSweeper.setStoragePoolStore(storagePoolStore);
    when(capacityRecordStore.getCapacityRecord()).thenReturn(mock(CapacityRecord.class));
    timeoutSweeper.setCapacityRecordStore(capacityRecordStore);
    timeoutSweeper.setServerStatusCheck(serverStatusCheck);

    volumes = new ArrayList<Long>();
    volume = TestUtils.generateVolumeMetadata();
    volumeStore.saveVolume(volume);
    volumeStore.saveVolumeForReport(volume);

    seg1 = volume.getSegmentByIndex(0);
    seg2 = volume.getSegmentByIndex(1);
    InstanceId primaryId = new InstanceId(1);
    InstanceId secondaryId1 = new InstanceId(2);
    InstanceId secondaryId2 = new InstanceId(3);
    primaryUnit = seg1.getSegmentUnitMetadata(primaryId);
    secondaryUnit1 = seg1.getSegmentUnitMetadata(secondaryId1);
    secondaryUnit2 = seg1.getSegmentUnitMetadata(secondaryId2);

    //alter event
    EdRootpathSingleton edRootpathSingleton = EdRootpathSingleton.getInstance();
    edRootpathSingleton.setRootPath("/tmp/testing");
  }

  @Test
  public void testDrainToWithSegmentUnit() throws Exception {
    primaryUnit.setLastReported(System.currentTimeMillis() - (segUnitTimeout + 1) * 1000);
    timeoutStore.addSegmentUnit(primaryUnit);
    int cnt = timeoutStore.drainTo(volumes);
    assertEquals(cnt, 1);
    assertEquals(volumes.get(0).longValue(), volume.getVolumeId());
  }

  @Test
  public void testDrainToWithoutSegmentUnit() throws Exception {
    primaryUnit.setLastReported(System.currentTimeMillis() - (segUnitTimeout - 1) * 1000);
    timeoutStore.addSegmentUnit(primaryUnit);
    int cnt = timeoutStore.drainTo(volumes);
    assertEquals(cnt, 0);
    assertEquals(volumes.size(), 0);
  }

  /* test the segment unit come out, but the last updated time is refreshed, also this segment 
  unit is not timeout */
  @Test
  public void testDrainToWithSegmentUnit_lastUpdatedTime_refreshed() throws Exception {
    primaryUnit.setLastReported(System.currentTimeMillis());
    timeoutStore.addSegmentUnit(primaryUnit);
    Thread.sleep(2 * 1000);
    primaryUnit.setLastReported(System.currentTimeMillis());
    Thread.sleep(2 * 1000);
    int cnt = timeoutStore.drainTo(volumes);
    assertEquals(cnt, 0);
    assertEquals(volumes.size(), 0);
  }

  @Test
  public void testDrainToWithManySegmentUnitInIt() throws Exception {
    primaryUnit.setLastReported(System.currentTimeMillis() + 5000);
    secondaryUnit1.setLastReported(System.currentTimeMillis() - (segUnitTimeout + 1) * 1000);
    secondaryUnit2.setLastReported(System.currentTimeMillis() - (segUnitTimeout + 2) * 1000);
    timeoutStore.addSegmentUnit(primaryUnit);
    timeoutStore.addSegmentUnit(secondaryUnit1);
    timeoutStore.addSegmentUnit(secondaryUnit2);
    int cnt = timeoutStore.drainTo(volumes);
    assertEquals(cnt, 2);
    assertEquals(volumes.size(), 2);
  }

  @Test
  public void timeoutSweeper_dowork_toBeCreatedTimeout() throws Exception {
    volume.setVolumeStatus(VolumeStatus.ToBeCreated);
    volume.setVolumeCreatedTime(
        new Date(System.currentTimeMillis() - (toBeCreatedTimeout - 1) * 1000));

    // not timeout
    for (int i = 0; i < 5; i++) {
      timeoutSweeper.doWork();
    }

    ArrayList<VolumeMetadata> volumes = new ArrayList<VolumeMetadata>();
    statusStore.drainTo(volumes);
    assertEquals(volumes.size(), 0);

    // set the toBeCreat is timeout
    volume.setVolumeCreatedTime(new Date(System.currentTimeMillis()
        - (InfoCenterConstants.getVolumeToBeCreatedTimeout() + 1) * 1000));

    for (int i = 0; i < 5; i++) {
      timeoutSweeper.doWork();
    }
    volumes = new ArrayList<>();
    statusStore.drainTo(volumes);
    assertEquals(volumes.size(), 1);
    assertTrue(volumes.get(0) == volume);
  }

  @Test
  public void timeoutSweeper_dowork_deadVolumeToDelete() throws Exception {
    volume.setVolumeStatus(VolumeStatus.Dead);
    volume.setDeadTime(System.currentTimeMillis() - (deadToRemoveTime - 1) * 1000);

    // not dead timout
    for (int i = 0; i < 5; i++) {
      timeoutSweeper.doWork();
    }
    ArrayList<VolumeMetadata> volumes = new ArrayList<VolumeMetadata>();
    statusStore.drainTo(volumes);
    assertEquals(volumes.size(), 0);

    // dead status timeout
    long timeout = (InfoCenterConstants.getTimeOfdeadVolumeToRemove() + 10L) * 1000L;
    volume.setDeadTime(System.currentTimeMillis() - timeout);
    for (int i = 0; i < 5; i++) {
      timeoutSweeper.doWork();
    }
    statusStore.drainTo(volumes);
    assertEquals(volumes.size(), 1);
    assertTrue(volumes.get(0) == volume);
  }

  @Test
  public void timeoutSweeper_dowork_segmentUnitTimeout() throws Exception {
    // not timeout
    primaryUnit.setLastReported(System.currentTimeMillis() - 1 * 1000);
    timeoutStore.addSegmentUnit(primaryUnit);
    timeoutSweeper.doWork();
    ArrayList<VolumeMetadata> volumes = new ArrayList<VolumeMetadata>();
    statusStore.drainTo(volumes);
    assertEquals(volumes.size(), 0);

    // timeout
    primaryUnit.setLastReported(System.currentTimeMillis());
    Thread.sleep((segUnitTimeout + 1) * 1000);
    timeoutSweeper.doWork();
    statusStore.drainTo(volumes);
    assertEquals(volumes.size(), 1);
  }
}
