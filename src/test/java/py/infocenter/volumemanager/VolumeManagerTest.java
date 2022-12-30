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

package py.infocenter.volumemanager;


import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.HaInstanceMangerWithZk;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceToVolumeInfo;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.store.VolumeStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.test.TestBase;
import py.volume.VolumeMetadata;


public class VolumeManagerTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(VolumeManagerTest.class);

  private VolumeInformationManger volumeInformationManger;
  @Mock
  private VolumeStore volumeStore;

  @Mock
  private InstanceStore instanceStore;

  private Map<Long, InstanceToVolumeInfo> instanceToVolumeInfoMap;

  private MyInstanceIncludeVolumeInfoManger myInstanceIncludeVolumeInfoManger;

  private Map<Long, List<Long>> removeVolumeInInfoTableWhenEquilibriumOk;

  @Mock
  private InfoCenterAppContext appContext;


  @Before
  public void init() {
    instanceToVolumeInfoMap = new ConcurrentHashMap<>();

    myInstanceIncludeVolumeInfoManger = new MyInstanceIncludeVolumeInfoManger();
    myInstanceIncludeVolumeInfoManger.setInstanceToVolumeInfoMap(instanceToVolumeInfoMap);

    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(myInstanceIncludeVolumeInfoManger);
    volumeInformationManger.setAppContext(appContext);

    removeVolumeInInfoTableWhenEquilibriumOk = new ConcurrentHashMap<>();
  }

  /**
   * this volume has report to master.
   **/
  @Test
  public void testChooseHaInMaster() {
    // in the master, the volume
    long volumeId = 123461L;
    long instanceIdMaster = 3;
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    when(volumeStore.getVolumeForReport(volumeId)).thenReturn(volumeMetadata);

    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);

    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    Instance instanceMaster = new Instance(new InstanceId(String.valueOf(instanceIdMaster)),
        new Group(1), "test3", InstanceStatus.HEALTHY);
    haInstanceManger.setMaterInstance(instanceMaster);

    when(appContext.getInstanceId()).thenReturn(new InstanceId(String.valueOf(instanceIdMaster)));
    long instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId);

    Assert.assertTrue(instanceId == 3);
  }

  @Ignore
  @Test
  public void testChooseHaInOneHa() {
    // in the master, the volume
    long volumeId1 = 123461L;
    long volumeId2 = 1234562;

    long volumeId1Size = 10L;
    long volumeId2Size = 5L;

    final VolumeMetadata volumeMetadata1 = makeVolume(volumeId1, volumeId1Size);
    final VolumeMetadata volumeMetadata2 = makeVolume(volumeId2, volumeId2Size);

    List<Long> volumeMetadataList1 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    volumeMetadataList1.add(volumeId1);
    totalSegmentUnitMetadataNumberMap.put(volumeId1, 0L);

    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    volumeMetadataList2.add(volumeId2);
    totalSegmentUnitMetadataNumberMap2.put(volumeId2, 0L);

    when(volumeStore.getVolume(volumeId1)).thenReturn(volumeMetadata1);
    when(volumeStore.getVolume(volumeId2)).thenReturn(volumeMetadata2);

    long instanceIdHa1 = 1;
    long instanceIdHa2 = 2;
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa1, volumeMetadataList1,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa2, volumeMetadataList2,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap2);

    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    Instance instanceMaster = new Instance(new InstanceId("3"), new Group(1), "test3",
        InstanceStatus.HEALTHY);
    haInstanceManger.setMaterInstance(instanceMaster);

    long instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId1);

    Assert.assertTrue(instanceId == instanceIdHa1);

  }

  @Test
  public void testChooseHaToChooseFromUnUsedHa() {
    // in the master, the volume
    long volumeId1 = 1234561;
    long volumeId2 = 1234562;
    final long volumeId3 = 1234563;
    final long volumeId4 = 1234564;

    List<Long> volumeMetadataList = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    totalSegmentUnitMetadataNumberMap.put(volumeId1, 0L);
    volumeMetadataList.add(volumeId1);

    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap2.put(volumeId2, 0L);
    volumeMetadataList2.add(volumeId2);

    List<Long> volumeMetadataList3 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap3 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap3.put(volumeId3, 0L);
    volumeMetadataList3.add(volumeId3);

    long instanceIdHa = 1;
    long instanceIdHa2 = 2;
    long instanceIdHa3 = 3;
    final long instanceIdHa4 = 4;

    myInstanceIncludeVolumeInfoManger
        .saveVolumeInfo(instanceIdHa, volumeMetadataList, removeVolumeInInfoTableWhenEquilibriumOk,
            totalSegmentUnitMetadataNumberMap);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa2, volumeMetadataList2,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap2);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa3, volumeMetadataList3,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap3);

    //instance,get the unUsed Ha, so choose instanceIdHa4
    final HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);

    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(1L, new Instance(new InstanceId("1"), new Group(1), "test1", InstanceStatus.SUSPEND));
    flowerInstanceMap
        .put(2L, new Instance(new InstanceId("2"), new Group(1), "test2", InstanceStatus.SUSPEND));
    flowerInstanceMap
        .put(4L, new Instance(new InstanceId("4"), new Group(1), "test4", InstanceStatus.SUSPEND));

    //the master
    Instance instanceMaster = new Instance(new InstanceId("3"), new Group(1), "test3",
        InstanceStatus.HEALTHY);

    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    //the master
    InstanceId instanceIdMaster = new InstanceId("3");
    when(appContext.getInstanceId()).thenReturn(instanceIdMaster);

    //the volume4 com, set to instanceIdHa4, the instanceIdHa4 is empty
    long instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId4);

    Assert.assertTrue(instanceId == instanceIdHa4);

    //the volume4 com, set to instanceIdHa4 same
    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId3);

    Assert.assertTrue(instanceId == instanceIdHa3);

  }

  @Test
  public void testChooseHaToChooseFromTheLeastHa() {
    // in the master, the volume
    long volumeId1 = 1234561;
    long volumeId2 = 1234562;
    long volumeId3 = 1234563;
    long volumeId4 = 1234564;
    long volumeId5 = 1234565;
    long volumeId6 = 1234566;
    long volumeId7 = 1234567;

    long volumeId1Size = 5;
    long volumeId2Size = 5;
    long volumeId3Size = 6;
    long volumeId4Size = 3;
    long volumeId5Size = 10;
    long volumeId6Size = 1;
    long volumeId7Size = 2;

    VolumeMetadata volumeMetadata1 = makeVolume(volumeId1, volumeId1Size);
    VolumeMetadata volumeMetadata2 = makeVolume(volumeId2, volumeId2Size);
    VolumeMetadata volumeMetadata3 = makeVolume(volumeId3, volumeId3Size);
    VolumeMetadata volumeMetadata4 = makeVolume(volumeId4, volumeId4Size);
    VolumeMetadata volumeMetadata5 = makeVolume(volumeId5, volumeId5Size);
    VolumeMetadata volumeMetadata6 = makeVolume(volumeId6, volumeId6Size);
    VolumeMetadata volumeMetadata7 = makeVolume(volumeId7, volumeId7Size);

    when(volumeStore.getVolume(volumeId1)).thenReturn(volumeMetadata1);
    when(volumeStore.getVolume(volumeId2)).thenReturn(volumeMetadata2);
    when(volumeStore.getVolume(volumeId3)).thenReturn(volumeMetadata3);
    when(volumeStore.getVolume(volumeId4)).thenReturn(volumeMetadata4);
    when(volumeStore.getVolume(volumeId5)).thenReturn(volumeMetadata5);
    when(volumeStore.getVolume(volumeId6)).thenReturn(volumeMetadata6);
    when(volumeStore.getVolume(volumeId7)).thenReturn(volumeMetadata7);

    //follower 1 5G
    List<Long> volumeMetadataList = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    totalSegmentUnitMetadataNumberMap.put(volumeId1, 0L);
    volumeMetadataList.add(volumeId1);

    //follower 2 5G+10G
    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap2.put(volumeId2, 0L);
    totalSegmentUnitMetadataNumberMap2.put(volumeId5, 0L);
    volumeMetadataList2.add(volumeId2);
    volumeMetadataList2.add(volumeId5);

    //master 3G+6G
    List<Long> volumeMetadataList3 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap3 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap3.put(volumeId3, 0L);
    totalSegmentUnitMetadataNumberMap3.put(volumeId4, 0L);
    volumeMetadataList3.add(volumeId3);
    volumeMetadataList3.add(volumeId4);

    //mast have 3G+6G , follower1  5G, follower2 5G+10G, choose follower1
    long instanceIdHaFlower1 = 1; //follower 1
    long instanceIdHaFlower2 = 2; //follower 2
    long instanceIdHaMaster = 3; //master

    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaFlower1, volumeMetadataList,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaFlower2, volumeMetadataList2,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap2);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaMaster, volumeMetadataList3,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap3);

    //instance,get the unused Ha
    final HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(1L, new Instance(new InstanceId("1"), new Group(1), "test1", InstanceStatus.SUSPEND));
    flowerInstanceMap
        .put(2L, new Instance(new InstanceId("2"), new Group(1), "test2", InstanceStatus.SUSPEND));

    InstanceId instanceIdMaster = new InstanceId("3");
    Instance instanceMaster = new Instance(instanceIdMaster, new Group(1), "test3",
        InstanceStatus.HEALTHY);
    when(appContext.getInstanceId()).thenReturn(instanceIdMaster);

    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    long instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId6);
    Assert.assertTrue(instanceId == instanceIdHaFlower1);

    //add one to follower 1, choose the first one,
    //follower 1
    VolumeMetadata volumeMetadataHaveHa6 = new VolumeMetadata();
    volumeMetadataHaveHa6.setVolumeId(volumeId6);

    //follower 1 5G + 1G
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaFlower1, volumeId6);

    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId7);

    //choose flower1 again, the size is small
    Assert.assertTrue(instanceId == instanceIdHaFlower1);

    //remove one volume in follower 2, the result 5G
    myInstanceIncludeVolumeInfoManger.removeVolumeInfo(instanceIdHaFlower2, volumeId5);
    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId7);

    Assert.assertTrue(instanceId == instanceIdHaFlower2);

    //system first, choose one
    instanceToVolumeInfoMap.clear();
    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId7);
    Assert.assertTrue(instanceId == instanceIdHaMaster);
  }

  @Test
  public void testChooseHaToChooseWithTheSegmentNumber() {
    // in the master, the volume
    long volumeId1 = 1234561;
    long volumeId2 = 1234562;
    long volumeId3 = 1234563;
    long volumeId4 = 1234564;
    long volumeId5 = 1234565;
    long volumeId6 = 1234566;
    long volumeId7 = 1234567;

    long volumeId1Size = 5;
    long volumeId2Size = 5;
    long volumeId3Size = 6;
    long volumeId4Size = 3;
    long volumeId5Size = 10;
    long volumeId6Size = 1;
    long volumeId7Size = 2;

    VolumeMetadata volumeMetadata1 = makeVolume(volumeId1, volumeId1Size);
    VolumeMetadata volumeMetadata2 = makeVolume(volumeId2, volumeId2Size);
    VolumeMetadata volumeMetadata3 = makeVolume(volumeId3, volumeId3Size);
    VolumeMetadata volumeMetadata4 = makeVolume(volumeId4, volumeId4Size);
    VolumeMetadata volumeMetadata5 = makeVolume(volumeId5, volumeId5Size);
    VolumeMetadata volumeMetadata6 = makeVolume(volumeId6, volumeId6Size);
    VolumeMetadata volumeMetadata7 = makeVolume(volumeId7, volumeId7Size);

    when(volumeStore.getVolume(volumeId1)).thenReturn(volumeMetadata1);
    when(volumeStore.getVolume(volumeId2)).thenReturn(volumeMetadata2);
    when(volumeStore.getVolume(volumeId3)).thenReturn(volumeMetadata3);
    when(volumeStore.getVolume(volumeId4)).thenReturn(volumeMetadata4);
    when(volumeStore.getVolume(volumeId5)).thenReturn(volumeMetadata5);
    when(volumeStore.getVolume(volumeId6)).thenReturn(volumeMetadata6);
    when(volumeStore.getVolume(volumeId7)).thenReturn(volumeMetadata7);

    //follower 1 5G
    List<Long> volumeMetadataList = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    totalSegmentUnitMetadataNumberMap.put(volumeId1, 0L);
    volumeMetadataList.add(volumeId1);

    //follower 2 5G+10G
    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap2.put(volumeId2, 0L);
    totalSegmentUnitMetadataNumberMap2.put(volumeId5, 0L);
    volumeMetadataList2.add(volumeId2);
    volumeMetadataList2.add(volumeId5);

    //master 3G+6G
    List<Long> volumeMetadataList3 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap3 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap3.put(volumeId3, 0L);
    totalSegmentUnitMetadataNumberMap3.put(volumeId4, 0L);
    volumeMetadataList3.add(volumeId3);
    volumeMetadataList3.add(volumeId4);

    //mast have 3G+6G , follower1  5G, follower2 5G+10G, choose follower1
    long instanceIdHaFlower1 = 1; //follower 1
    long instanceIdHaFlower2 = 2; //follower 2
    long instanceIdHaMaster = 3; //master

    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaFlower1, volumeMetadataList,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaFlower2, volumeMetadataList2,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap2);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaMaster, volumeMetadataList3,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap3);

    //instance,get the unused Ha
    final HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);
    Map<Long, Instance> flowerInstanceMap = new ConcurrentHashMap<>();
    flowerInstanceMap
        .put(1L, new Instance(new InstanceId("1"), new Group(1), "test1", InstanceStatus.SUSPEND));
    flowerInstanceMap
        .put(2L, new Instance(new InstanceId("2"), new Group(1), "test2", InstanceStatus.SUSPEND));

    InstanceId instanceIdMaster = new InstanceId("3");
    Instance instanceMaster = new Instance(instanceIdMaster, new Group(1), "test3",
        InstanceStatus.HEALTHY);
    when(appContext.getInstanceId()).thenReturn(instanceIdMaster);

    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    haInstanceManger.setMaterInstance(instanceMaster);
    haInstanceManger.setFollowerInstanceMap(flowerInstanceMap);

    long instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId6);
    Assert.assertTrue(instanceId == instanceIdHaFlower1);

    //add one to follower 1, choose the first one,
    //follower 1
    VolumeMetadata volumeMetadataHaveHa6 = new VolumeMetadata();
    volumeMetadataHaveHa6.setVolumeId(volumeId6);

    //follower 1 5G + 1G
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHaFlower1, volumeId6);

    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId7);

    //choose flower1 again, the size is small
    Assert.assertTrue(instanceId == instanceIdHaFlower1);

    //remove one volume in follower 2, the result 5G
    myInstanceIncludeVolumeInfoManger.removeVolumeInfo(instanceIdHaFlower2, volumeId5);
    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId7);

    Assert.assertTrue(instanceId == instanceIdHaFlower2);

    //system first, choose one
    instanceToVolumeInfoMap.clear();
    instanceId = volumeInformationManger.distributeVolumeToReportUnit(volumeId7);
    Assert.assertTrue(instanceId == instanceIdHaMaster);
  }

  /**
   * test one volume report by more Ha instance, choose the best one instance for next time report.
   */
  @Test
  public void testChooseInstanceForMoreInstanceReportSameVolume() {
    // in the master, the volume
    long volumeId1 = 1234561;

    /* 1. init ***/
    List<Long> volumeMetadataList = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    totalSegmentUnitMetadataNumberMap.put(volumeId1, 1L);
    volumeMetadataList.add(volumeId1);

    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap2.put(volumeId1, 0L);
    volumeMetadataList2.add(volumeId1);

    List<Long> volumeMetadataList3 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap3 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap3.put(volumeId1, 2L); //
    volumeMetadataList3.add(volumeId1);

    /* instance ***/
    long instanceIdHa = 1;
    final long instanceIdHa2 = 2;
    final long instanceIdHa3 = 3;

    /* init the volumeId1 report to instanceIdHa, in table ****/
    InstanceToVolumeInfo instanceToVolumeInfo = new InstanceToVolumeInfo();
    instanceToVolumeInfo.addVolume(volumeId1, 1L);
    instanceToVolumeInfoMap.put(instanceIdHa, instanceToVolumeInfo);

    /* 2 . each instance report same volume, this time is volumeId1 ***/
    Set<Long> result = null;

    /* instanceIdHa report volumeId1, in table  ***/
    result = myInstanceIncludeVolumeInfoManger
        .saveVolumeInfo(instanceIdHa, volumeMetadataList, removeVolumeInInfoTableWhenEquilibriumOk,
            totalSegmentUnitMetadataNumberMap);
    Assert.assertTrue(result.isEmpty());

    /* instanceIdHa2 report volumeId1, but it is have 0 SegmentUnit ***/
    result = myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa2, volumeMetadataList2,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap2);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.iterator().next() == volumeId1);

    /* instanceIdHa3 report volumeId1, have 2 SegmentUnit.
     more then the table which save value is 1  ***/
    result = myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa3, volumeMetadataList3,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap3);
    Assert.assertTrue(result.isEmpty());

    //save in instanceIdHa3 and remove in instanceIdHa
    logger.warn("---- print the instanceToVolumeInfoMap :{}", instanceToVolumeInfoMap);
    Assert.assertTrue(!instanceToVolumeInfoMap.get(instanceIdHa).containsValue(volumeId1));
    Assert.assertTrue(instanceToVolumeInfoMap.get(instanceIdHa3).containsValue(volumeId1));

    /* instanceIdHa report volumeId1 again, remove  ***/
    result = myInstanceIncludeVolumeInfoManger
        .saveVolumeInfo(instanceIdHa, volumeMetadataList, removeVolumeInInfoTableWhenEquilibriumOk,
            totalSegmentUnitMetadataNumberMap);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.iterator().next() == volumeId1);

    logger.warn("---- print the instanceToVolumeInfoMap :{}", instanceToVolumeInfoMap);
  }

  /**
   * test one volume report by more Ha instance, choose the best one instance for next time report.
   */
  @Test
  public void testChooseInstanceForMoreInstanceReportSameVolume_more() {
    // in the master, the volume
    long volumeId1 = 1234561;

    /* 1. init ***/
    List<Long> volumeMetadataList = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    totalSegmentUnitMetadataNumberMap.put(volumeId1, 1L);
    volumeMetadataList.add(volumeId1);

    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap2.put(volumeId1, 0L);
    volumeMetadataList2.add(volumeId1);

    List<Long> volumeMetadataList3 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap3 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap3.put(volumeId1, 2L); //
    volumeMetadataList3.add(volumeId1);

    /* instance ***/
    long instanceIdHa = 1;
    final long instanceIdHa2 = 2;
    long instanceIdHa3 = 3;

    /* init the volumeId1 report to instanceIdHa, in table ****/
    InstanceToVolumeInfo instanceToVolumeInfo = new InstanceToVolumeInfo();
    instanceToVolumeInfo.addVolume(volumeId1, 1L);
    instanceToVolumeInfoMap.put(instanceIdHa, instanceToVolumeInfo);

    InstanceToVolumeInfo instanceToVolumeInfo2 = new InstanceToVolumeInfo();
    instanceToVolumeInfo2.addVolume(volumeId1, 2L);
    instanceToVolumeInfoMap.put(instanceIdHa2, instanceToVolumeInfo2);

    /* 2 . each instance report same volume, this time is volumeId1 ***/
    Set<Long> result = null;

    /* instanceIdHa report volumeId1, in table  ***/
    result = myInstanceIncludeVolumeInfoManger
        .saveVolumeInfo(instanceIdHa, volumeMetadataList, removeVolumeInInfoTableWhenEquilibriumOk,
            totalSegmentUnitMetadataNumberMap);
    Assert.assertTrue(result.size() == 1);
    Assert.assertTrue(result.iterator().next() == volumeId1);

    logger.warn("---- print the instanceToVolumeInfoMap :{}", instanceToVolumeInfoMap);
  }

  @Test
  public void testJson() {

    Map<Long, Student> test = new HashMap<>();
    Student s1 = new Student("wen", 10);
    Student s2 = new Student("wen2", 12);
    test.put(1L, s1);
    test.put(2L, s2);

    String value = objectToString(test);

    System.out.println("get the value :" + value);

    ObjectMapper mapper = new ObjectMapper();
    try {
      Map returnObject = mapper.readValue(value, Map.class);
      System.out.println("get the value !!! :" + returnObject);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Test
  public void testPrint() {

    List<Long> volumeMetadataList = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap = new HashMap<>();
    totalSegmentUnitMetadataNumberMap.put(11111L, 0L);
    volumeMetadataList.add(11111L);

    List<Long> volumeMetadataList2 = new ArrayList<>();
    Map<Long, Long> totalSegmentUnitMetadataNumberMap2 = new HashMap<>();
    totalSegmentUnitMetadataNumberMap2.put(22222L, 0L);
    volumeMetadataList2.add(22222L);

    long instanceIdHa = 1;
    long instanceIdHa2 = 2;
    myInstanceIncludeVolumeInfoManger
        .saveVolumeInfo(instanceIdHa, volumeMetadataList, removeVolumeInInfoTableWhenEquilibriumOk,
            totalSegmentUnitMetadataNumberMap);
    myInstanceIncludeVolumeInfoManger.saveVolumeInfo(instanceIdHa2, volumeMetadataList2,
        removeVolumeInInfoTableWhenEquilibriumOk,
        totalSegmentUnitMetadataNumberMap2);

    Map<Long, List<Long>> map = myInstanceIncludeVolumeInfoManger.printMapValue();
    logger.warn("get the map value :{}", map);
  }


  public String objectToString(Object o) {
    String stringValue = null;
    try {
      ObjectMapper mapper = new ObjectMapper();
      stringValue = mapper.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      logger.error("failed to build string, the error ", e);
    }
    return stringValue;
  }


  public VolumeMetadata makeVolume(long volumeId, long volumeSize) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(volumeId);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setSegmentSize(1);
    return volumeMetadata;
  }

  @After
  public void clear() {
    instanceToVolumeInfoMap.clear();
  }


  public class MyInstanceIncludeVolumeInfoManger extends InstanceIncludeVolumeInfoManger {

    @Override
    public void saveinstancevolumeinfotodb(long instanceId,
        InstanceToVolumeInfo instanceToVolumeInfoGet) {
      return;
    }
  }


  public class Student {

    String name;
    int age;

    public Student(String name, int age) {
      this.name = name;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }
}
