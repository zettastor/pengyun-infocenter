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
import static org.junit.Assert.fail;
import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;
import org.junit.Test;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.common.VolumeMetadataJsonParser;
import py.instance.InstanceId;
import py.test.TestBase;
import py.test.TestUtils;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class JsonPaserTest extends TestBase {

  @Test
  public void testVolumeMetadatatoJsonAndGetBack() {

    VolumeMetadata volumeMetadata = new VolumeMetadata(20000, 20001, 20002,
        20003, null, 0L, 0L);

    volumeMetadata.setVolumeId(37002);
    volumeMetadata.setRootVolumeId(1003);
    volumeMetadata.setChildVolumeId(null);
    volumeMetadata.setVolumeSize(1005);
    volumeMetadata.setExtendingSize(1006);
    volumeMetadata.setName("stdname");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(1008);
    volumeMetadata.setDeadTime(0L);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setPageWrappCount(128);
    volumeMetadata.setSegmentWrappCount(10);
    volumeMetadata.setLastExtendedTime(new Date(0));
    SegId segId = new SegId(37002, 11);
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());

    volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

    // build volumemetadata as a string
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS,false);
    logger.info("volumeMetadata string :{}", volumeMetadata);
    String volumeString = null;
    try {
      volumeString = mapper.writeValueAsString(volumeMetadata);
    } catch (JsonProcessingException e) {
      logger.error("failed to build volumemetadata string ", e);
      fail();
    }

    System.out.println(volumeString);

    VolumeMetadataJsonParser parser = new VolumeMetadataJsonParser(volumeMetadata.getVersion(),
        volumeString);

    VolumeMetadata volumeFromJson;
    try {
      volumeFromJson = mapper.readValue(parser.getVolumeMetadataJson(), VolumeMetadata.class);
      Date now = new Date();
      assertTrue("CreatedTime isn't close enough to the testTime!",
          now.getTime() - volumeFromJson.getVolumeCreatedTime().getTime() < 1000 * 60);

      assertEquals(VolumeMetadata.VolumeSourceType.CREATE_VOLUME, volumeFromJson.getVolumeSource());
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void test() {
    Set<InstanceId> instanceIdList = new TreeSet<>((o1, o2) -> {
      int valCompare = Long.compare(0L, 0L);
      if (valCompare == 0) {
        return Integer.compare(o1.hashCode(), o2.hashCode());
      } else {
        return valCompare;
      }
    });
    InstanceId instanceId1 = new InstanceId(1L);
    InstanceId instanceId2 = new InstanceId(2L);
    instanceIdList.add(instanceId1);
    instanceIdList.add(instanceId2);
    logger.warn("1: {}, 2: {}", instanceId1.hashCode(), instanceId2.hashCode());

    logger.warn("the :{}", TimeUnit.MINUTES.toMillis(2));
  }

  @Test
  public void test2() {
    Set<Long> set1 = new HashSet<>();
    Set<Long> set2 = new HashSet<>();

    set1.add(11L);
    set1.add(22L);

    set2.add(33L);
    set2.add(22L);

    Map<Long, Set<Long>> longSetHashMap = new HashMap<>();
    Set<Long> longHashSet = new HashSet<>();
    longHashSet.add(3333L);

    Set<Long> longHashSet1 = new HashSet<>();
    longHashSet1.add(3333L);

    longSetHashMap.put(22L, longHashSet);
    longSetHashMap.put(33L, longHashSet1);

    logger.warn("the value :{}", longSetHashMap);
    Set<Long> temp = longSetHashMap.get(22L);
    temp.remove(3333L);

    logger.warn("the value :{}", longSetHashMap);


  }
}