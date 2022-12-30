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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import py.app.context.AppContext;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumesForEquilibrium;
import py.infocenter.worker.HaInstanceEquilibriumSweeper;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;


public class HaInstanceVolumeEquilibriumAlgorithmTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(HaInstanceVolumeEquilibriumAlgorithmTest.class);

  private MyInstanceIncludeVolumeInfoManger myInstanceIncludeVolumeInfoManger;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;

  @Mock
  private AppContext appContext;

  private HaInstanceEquilibriumSweeper haInstanceMoveVolumeSweeper;


  @Before
  public void init() {
    setLogLevel(Level.WARN);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    when(appContext.getInstanceId()).thenReturn(new InstanceId(11));

    myInstanceIncludeVolumeInfoManger = new MyInstanceIncludeVolumeInfoManger();
    haInstanceMoveVolumeSweeper = new HaInstanceEquilibriumSweeper();

    instanceVolumeInEquilibriumManger = new InstanceVolumeInEquilibriumManger();
    instanceVolumeInEquilibriumManger
        .setInstanceIncludeVolumeInfoManger(myInstanceIncludeVolumeInfoManger);
    instanceVolumeInEquilibriumManger.setSegmentSize(1L);

    haInstanceMoveVolumeSweeper.setEnableInstanceEquilibriumVolume(true);
    haInstanceMoveVolumeSweeper.setAppContext(appContext);
    haInstanceMoveVolumeSweeper
        .setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
  }


  @Test
  public void testMove() throws Exception {
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    /* init 1 ***/
    /* init the instance 111111 **/
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123451L, 3L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123452L, 4L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123453L, 20L, true));

    /* init the instance 111112 **/
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223451L, 3L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223452L, 4L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223453L, 5L, true));

    /* init the instance 111113 **/
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323451L, 3L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323452L, 4L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323453L, 5L, true));

    /* init the instance 111114 **/
    volumeSizeInfo.add(111114L, new VolumesForEquilibrium());

    /* init 2  Random test ***/

    /* ---------------- ***/
    logger.warn("the begin map :{}", volumeSizeInfo);

    //begin
    MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo = new LinkedMultiValueMap<>();
    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : volumeSizeInfo.entrySet()) {
      List<VolumesForEquilibrium> volumesForEquilibriumList = entry.getValue();
      long instanceId = entry.getKey();
      theEndVolumeSizeInfo.put(instanceId, new ArrayList<>(volumesForEquilibriumList));
    }

    instanceVolumeInEquilibriumManger.volumeEquilibriumForEachInstance(theEndVolumeSizeInfo);

    logger.warn("get the begin --- :{}", volumeSizeInfo);
    logger.warn("get the end --- :{}", theEndVolumeSizeInfo);

    Map<Long, Long> theBeginResult = new HashMap<>();
    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : volumeSizeInfo.entrySet()) {
      long instanceId = entry.getKey();
      long totalSize = 0;
      List<VolumesForEquilibrium> instanceVolume = entry.getValue();
      for (VolumesForEquilibrium volumesForEquilibrium : instanceVolume) {
        totalSize += volumesForEquilibrium.getVolumeSize();
      }

      theBeginResult.put(instanceId, totalSize);
    }

    Map<Long, Long> theResult = new HashMap<>();
    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : theEndVolumeSizeInfo.entrySet()) {
      long instanceId = entry.getKey();
      long totalSize = 0;
      List<VolumesForEquilibrium> instanceVolume = entry.getValue();
      for (VolumesForEquilibrium volumesForEquilibrium : instanceVolume) {
        totalSize += volumesForEquilibrium.getVolumeSize();
      }

      theResult.put(instanceId, totalSize);
    }

    logger.error("the theBeginResult :{}", theBeginResult);
    logger.error("the      theResult :{}", theResult);

    Set<Long> eachTotalSize = new HashSet<>();
    eachTotalSize.add(20L);
    eachTotalSize.add(12L);
    eachTotalSize.add(9L);
    eachTotalSize.add(10L);

    //theResult :{111111=20, 111113=12, 111112=9, 111114=10}
    for (Map.Entry<Long, Long> entry : theResult.entrySet()) {
      Assert.assertTrue(eachTotalSize.contains(entry.getValue()));
    }
  }

  @Test
  public void testGetVolumeMoveToWhichInstance_VolumeAvailable() throws Exception {
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    /* init 1 ***/
    /* init the instance 111111 **/
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123451L, 3L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123452L, 4L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123453L, 20L, true));

    /* init the instance 111112 **/
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223451L, 3L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223452L, 4L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223453L, 5L, true));

    /* init the instance 111113 **/
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323451L, 3L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323452L, 4L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323453L, 5L, true));

    /* init the instance 111114 **/
    volumeSizeInfo.add(111114L, new VolumesForEquilibrium());

    /* init 2  Random test ***/

    /* ---------------- ***/
    logger.warn("the begin map :{}", volumeSizeInfo);

    //set the instance 111114 as master
    when(appContext.getInstanceId()).thenReturn(new InstanceId(111114L));

    myInstanceIncludeVolumeInfoManger.setVolumeSizeInfo(volumeSizeInfo);
    haInstanceMoveVolumeSweeper.doWork();

    //before move and after move_ for back
    //{111111={123451=3, 123453=20, 123452=4}, 111113={323452=4, 323453=5, 323451=3}, 
    // 111112={223451=3, 223453=5, 223452=4}, 111114={}}
    //{111111={123453=20}, 111113={323452=4, 323453=5, 323451=3}, 111112={223453=5, 223452=4}, 
    // 111114={223451=3, 123451=3, 123452=4}}

    //before move and after move
    //{111111={123451=3, 123453=20, 123452=4}, 111113={323452=4, 323453=5, 323451=3}, 
    // 111112={223451=3, 223453=5, 223452=4}, 111114={}}
    //{111111={123453=20}, 111113={323452=4, 323453=5}, 111112={223453=5, 223452=4,223452=3}, 
    // 111114={323451=3, 123451=3, 123452=4}}

    //result
    //  {111111={123451=111114, 123452=111114}, 111113={323451=111114}}
    Map<Long, Map<Long, Long>> result = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibrium();
    logger.warn("end the map :{}", result);

    Map<Long, Map<Long, Long>> getValue = new HashMap<>();
    Map<Long, Long> value = new HashMap<>();

    value.put(123451L, 111114L);
    value.put(123452L, 111114L);
    getValue.put(111111L, value);

    Map<Long, Long> value2 = new HashMap<>();
    value2.put(323451L, 111114L);
    getValue.put(111113L, value2);

    for (Map.Entry<Long, Map<Long, Long>> entry : result.entrySet()) {
      logger.warn("--- {} {} {}", entry.getKey(), getValue.containsKey(entry.getKey()),
          getValue.containsValue(entry.getValue()));
    }

    Assert.assertTrue(getValue.equals(result));

    //for rebuild {123451={111111=111114}, 123452={111111=111114}, 323451={111113=111114}}s
    final Map<Long, Map<Long, Long>> resultRebuild = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibriumBuildWithVolumeId();

    Map<Long, Map<Long, Long>> getValue2 = new HashMap<>();
    Map<Long, Long> value3 = new HashMap<>();

    value3.put(111113L, 111114L);
    getValue2.put(323451L, value3); //323451={111113=111114}

    Map<Long, Long> value4 = new HashMap<>();
    value4.put(111111L, 111114L);
    getValue2.put(123451L, value4); //123451={111111=111114}

    Map<Long, Long> value5 = new HashMap<>();
    value5.put(111111L, 111114L);
    getValue2.put(123452L, value5); //123452={111111=111114}}

    logger.warn("get the result getValue2 :{}", getValue2);
    logger.warn("get the result resultRebuild :{}", resultRebuild);
    Assert.assertTrue(getValue2.equals(resultRebuild));

  }

  @Test
  public void testGetVolumeMoveToWhichInstance_VolumeUnavailableAndAvailable() throws Exception {
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    /* init 1 ***/
    /* init the instance 111111 **/
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123451L, 3L, false));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123452L, 4L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123453L, 20L, true));

    /* init the instance 111112 **/
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223451L, 3L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223452L, 4L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223453L, 5L, true));

    /* init the instance 111113 **/
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323451L, 3L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323452L, 4L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323453L, 5L, true));

    /* init the instance 111114 **/
    volumeSizeInfo.add(111114L, new VolumesForEquilibrium());

    /* init 2  Random test ***/

    /* ---------------- ***/
    //set the instance 111114 as master
    when(appContext.getInstanceId()).thenReturn(new InstanceId(111114L));

    logger.warn("the begin map :{}", volumeSizeInfo);
    myInstanceIncludeVolumeInfoManger.setVolumeSizeInfo(volumeSizeInfo);
    haInstanceMoveVolumeSweeper.doWork();

    //before move and after move
    //{111111={123451=3, 123453=20, 123452=4}, 111113={323452=4, 323453=5, 323451=3}, 
    // 111112={223451=3, 223453=5, 223452=4}, 111114={}}

    //the end

    //result
    // {111111={123452=111114}, 111113={323451=111114}, 111112={223451=111114}} //the 123451 is 
    // Unavailable,can not move
    Map<Long, Map<Long, Long>> result = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibrium();
    logger.warn("end the map :{}", result);

    Map<Long, Map<Long, Long>> getValue = new HashMap<>();
    Map<Long, Long> value = new HashMap<>();

    value.put(123452L, 111114L);
    getValue.put(111111L, value);

    Map<Long, Long> value3 = new HashMap<>();
    value3.put(223451L, 111114L);
    getValue.put(111112L, value3);

    Map<Long, Long> value2 = new HashMap<>();
    value2.put(323451L, 111114L);
    getValue.put(111113L, value2);

    logger.warn("the getValue value :{}", result);
    Assert.assertTrue(getValue.equals(result));

    //for rebuild {223451={111112=111114}, 323451={111113=111114}, 123452={111111=111114}}
    final Map<Long, Map<Long, Long>> resultRebuild = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibriumBuildWithVolumeId();

    Map<Long, Map<Long, Long>> getValue2 = new HashMap<>();
    Map<Long, Long> value6 = new HashMap<>();

    value6.put(111112L, 111114L);
    getValue2.put(223451L, value6); //223451={111112=111114}

    Map<Long, Long> value4 = new HashMap<>();
    value4.put(111113L, 111114L);
    getValue2.put(323451L, value4); //323451={111113=111114}

    Map<Long, Long> value5 = new HashMap<>();
    value5.put(111111L, 111114L);
    getValue2.put(123452L, value5); //123452={111111=111114}}

    logger.warn("get the result getValue2 :{}", getValue2);
    logger.warn("get the result resultRebuild :{}", resultRebuild);
    Assert.assertTrue(getValue2.equals(resultRebuild));
  }

  @Test
  public void testGetVolumeMoveToWhichInstance_TheMasterSegmentIsLargeWhenEquilibrium()
      throws Exception {

    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    /* init 1 ***/
    /* init the instance 111111 **/
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123451L, 3L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123452L, 4L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123453L, 20L, true));

    /* init the instance 111112 **/
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223451L, 3L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223452L, 4L, true));
    volumeSizeInfo.add(111112L, new VolumesForEquilibrium(223453L, 5L, true));

    /* init the instance 111113 **/
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323451L, 3L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323452L, 4L, true));
    volumeSizeInfo.add(111113L, new VolumesForEquilibrium(323453L, 5L, true));

    /* init the instance 111114 **/
    volumeSizeInfo.add(111114L, new VolumesForEquilibrium(423451L, 1L, true));

    //set the instance 111114 as master
    when(appContext.getInstanceId()).thenReturn(new InstanceId(111114L));
    instanceVolumeInEquilibriumManger.setPercentNumberSegmentNumberMasterSave(0.3);
    instanceVolumeInEquilibriumManger.setAllSegmentNumberToSave(10);

    /* ---------------- ***/
    logger.warn("the begin map :{}", volumeSizeInfo);
    myInstanceIncludeVolumeInfoManger.setVolumeSizeInfo(volumeSizeInfo);
    haInstanceMoveVolumeSweeper.doWork();

    //before move and after move
    //{111111={123451=3, 123453=20, 123452=4}, 111113={323452=4, 323453=5, 323451=3}, 
    // 111112={223451=3, 223453=5, 223452=4}, 111114={423451=1}}

    //result, after move, the master is large, so not need move
    //  {111111={123451=111113, 123452=111112}}
    Map<Long, Map<Long, Long>> result = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibrium();
    logger.warn("end the map :{}", result);

    Map<Long, Map<Long, Long>> getValue = new HashMap<>();
    Map<Long, Long> value = new HashMap<>();

    value.put(123451L, 111113L);
    value.put(123452L, 111112L);
    getValue.put(111111L, value);

    Assert.assertTrue(getValue.equals(result));

    //for rebuild {123451={111111=111113}, 123452={111111=111112}}
    final Map<Long, Map<Long, Long>> resultRebuild = instanceVolumeInEquilibriumManger
        .getVolumeReportToInstanceEquilibriumBuildWithVolumeId();

    Map<Long, Map<Long, Long>> getValue2 = new HashMap<>();
    Map<Long, Long> value3 = new HashMap<>();

    value3.put(111111L, 111112L);
    getValue2.put(123452L, value3); //123452={111111=111112}

    Map<Long, Long> value4 = new HashMap<>();
    value4.put(111111L, 111113L);
    getValue2.put(123451L, value4); //123451={111111=111113}

    logger.warn("get the result getValue2 :{}", getValue2);
    logger.warn("get the result resultRebuild :{}", resultRebuild);
    Assert.assertTrue(getValue2.equals(resultRebuild));

    instanceVolumeInEquilibriumManger.setTheSegmentInfoToDefault();
  }

  @Test
  public void testEmpty() {
    /* ---------------- ***/
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    //have one instance
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123451L, 3L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123452L, 4L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123453L, 20L, true));
    logger.warn("the begin map :{}", volumeSizeInfo);

    MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo2 = new LinkedMultiValueMap<>();

    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : volumeSizeInfo.entrySet()) {
      List<VolumesForEquilibrium> volumesForEquilibriumList = entry.getValue();
      long instanceId = entry.getKey();
      theEndVolumeSizeInfo2.put(instanceId, new ArrayList<>(volumesForEquilibriumList));
    }

    instanceVolumeInEquilibriumManger.volumeEquilibriumForEachInstance(theEndVolumeSizeInfo2);
    Assert.assertTrue(theEndVolumeSizeInfo2.equals(volumeSizeInfo));
  }

  @Test
  public void testEq() throws Exception {
    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    //have one instance
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123451L, 3L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123452L, 4L, true));
    volumeSizeInfo.add(111111L, new VolumesForEquilibrium(123453L, 20L, true));
    logger.warn("the begin map :{}", volumeSizeInfo);

    MultiValueMap<Long, VolumesForEquilibrium> theEndVolumeSizeInfo2 = new LinkedMultiValueMap<>();

    for (Map.Entry<Long, List<VolumesForEquilibrium>> entry : volumeSizeInfo.entrySet()) {
      List<VolumesForEquilibrium> volumesForEquilibriumList = entry.getValue();
      long instanceId = entry.getKey();
      theEndVolumeSizeInfo2.put(instanceId, new ArrayList<>(volumesForEquilibriumList));
    }

    myInstanceIncludeVolumeInfoManger.setVolumeSizeInfo(volumeSizeInfo);
    haInstanceMoveVolumeSweeper.doWork();

  }

  class MyInstanceIncludeVolumeInfoManger extends InstanceIncludeVolumeInfoManger {

    MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo = new LinkedMultiValueMap<>();

    @Override
    public MultiValueMap<Long, VolumesForEquilibrium> getEachHaVolumeSizeInfo() {
      return volumeSizeInfo;
    }

    public void setVolumeSizeInfo(MultiValueMap<Long, VolumesForEquilibrium> volumeSizeInfo) {
      this.volumeSizeInfo = volumeSizeInfo;
    }
  }
}
