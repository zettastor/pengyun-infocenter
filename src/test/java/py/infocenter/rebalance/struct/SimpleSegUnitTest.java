/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.infocenter.rebalance.struct;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.infocenter.rebalance.InstanceInfo;
import py.instance.InstanceId;
import py.test.TestBase;
import py.volume.VolumeType;


@Deprecated
public class SimpleSegUnitTest extends TestBase {

  @Ignore
  @Test
  public void testCanBeMovedToWithOtherStatus() {
    SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1L, VolumeType.REGULAR, 1L),
        0);
    SimpleSegUnitInfo segUnit = segment.getSecondaries().get(0);

    for (SegmentUnitStatus status : SegmentUnitStatus.values()) {
      if (status == SegmentUnitStatus.Primary || status == SegmentUnitStatus.PrePrimary
          || status == SegmentUnitStatus.Deleting
          || status == SegmentUnitStatus.Deleted) {
        segUnit.setStatus(status);
        assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));
      }
    }
    segUnit.setStatus(SegmentUnitStatus.Secondary);

    segUnit = segment.getPrimary();
    segUnit.setStatus(SegmentUnitStatus.Secondary);
    assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));

  }

  @Ignore
  @Test
  public void testCanBeMovedToWithNotEnoughMembers() {
    SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1L, VolumeType.REGULAR, 1L),
        0);
    segment.getPrimary().freeMySelf();
    for (SimpleSegUnitInfo segUnit : segment.getSegUnits()) {
      assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));
    }

    segment = generateSegment(new SimpleVolumeInfo(1L, VolumeType.REGULAR, 1L), 0);
    SimpleSegUnitInfo segUnit1 = segment.getSecondaries().get(0);
    segUnit1.freeMySelf();
    for (SimpleSegUnitInfo segUnit : segment.getSegUnits()) {
      assertTrue(!segUnit.canBeMovedTo(Mockito.mock(InstanceInfo.class)));
    }

  }

  @Ignore
  @Test
  public void testCanBeMovedToWithWrongGroup() {
    SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1L, VolumeType.REGULAR, 1L),
        0);
    InstanceInfoImpl instanceInfo = Mockito.mock(InstanceInfoImpl.class);
    Mockito.when(instanceInfo.getGroupId()).thenReturn(segment.getPrimary().getGroupId());

    for (SimpleSegUnitInfo segUnit : segment.getSegUnits()) {
      assertTrue(!segUnit.canBeMovedTo(instanceInfo));
    }

  }

  @Ignore
  @Test
  public void testCanBeMovedToWithAperfectSegUnit() {

    InstanceInfoImpl instanceInfo = Mockito.mock(InstanceInfoImpl.class);
    Mockito.when(instanceInfo.getGroupId()).thenReturn(4);

    SimpleSegmentInfo segment = generateSegment(new SimpleVolumeInfo(1L, VolumeType.REGULAR, 1L),
        0);
    SimpleSegUnitInfo segUnit = segment.getSecondaries().get(0);
    for (SegmentUnitStatus status : SegmentUnitStatus.values()) {
      if (status == SegmentUnitStatus.Primary || status == SegmentUnitStatus.PrePrimary
          || status == SegmentUnitStatus.Deleting
          || status == SegmentUnitStatus.Deleted) {
        segUnit.setStatus(status);
        assertTrue(!segUnit.canBeMovedTo(instanceInfo));
      } else {
        segUnit.setStatus(status);
        assertTrue(segUnit.canBeMovedTo(instanceInfo));
      }
    }
  }

  private SimpleSegmentInfo generateSegment(SimpleVolumeInfo volume, int segIndex) {
    int memberCount = volume.getVolumeType().getNumMembers();
    long storagePoolId = volume.getStoragePoolId();
    SegId segId = new SegId(volume.getVolumeId(), segIndex);

    List<Integer> groups = new ArrayList<>();
    for (int i = 0; i < memberCount; i++) {
      groups.add(i);
    }
    Collections.shuffle(groups);

    SimpleSegmentInfo segment = new SimpleSegmentInfo(segId, null);
    volume.addSegment(segment);

    for (int i = 0; i < memberCount; i++) {
      int group = groups.get(i);
      SimpleSegUnitInfo segUnit = new SimpleSegUnitInfo(segId, group, storagePoolId,
          new InstanceId(group));
      segUnit.setArchiveId(1L);
      segUnit.setStatus(group == 0 ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary);
      segment.addSegmentUnit(segUnit);
    }

    return segment;
  }

}
