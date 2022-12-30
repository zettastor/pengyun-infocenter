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

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import java.util.Date;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.VolumeStore;
import py.infocenter.test.base.InformationCenterAppConfigTest;
import py.test.TestBase;
import py.test.TestUtils;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterAppConfigTest.class})
public class VolumeStoreImplTest extends TestBase {

  @Autowired
  public VolumeStore volumeStoreDb;

  @Before
  public void init() {
  }

  @Test
  public void testInMemoryVolumeStoreImpl() {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    final MemoryVolumeStoreImpl memoryVolumeStore = new MemoryVolumeStoreImpl();

    volumeMetadata.setName("aaa");
    volumeMetadata.setAccountId(11111);
    volumeMetadata.setVolumeId(11111);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);

    //test put and get normal is OK;
    memoryVolumeStore.saveVolume(volumeMetadata);
    VolumeMetadata volume = memoryVolumeStore.getVolume(11111L);
    Assert.assertNotNull(volume);
    Assert.assertEquals(volume.getAccountId(), 11111);
    Assert.assertEquals(volume.getName(), "aaa");
    Assert.assertEquals(volume.getVolumeStatus(), VolumeStatus.Available);

    volume = memoryVolumeStore.getVolume(11112L);
    Assert.assertNull(volume);

    memoryVolumeStore.deleteVolume(volumeMetadata);
    volume = memoryVolumeStore.getVolume(11111L);
    Assert.assertNull(volume);

    //test get volume not in dead status is OK
    memoryVolumeStore.saveVolume(volumeMetadata);
    volume = memoryVolumeStore.getVolumeNotDeadByName("aaa");
    Assert.assertNotNull(volume);

    volume = memoryVolumeStore.getVolumeNotDeadByName("AAA");
    Assert.assertNull(volume);

    volumeMetadata.setVolumeStatus(VolumeStatus.Dead);
    volume = memoryVolumeStore.getVolumeNotDeadByName("aaa");
    Assert.assertNull(volume);

    VolumeMetadata otherVolumeData = new VolumeMetadata();
    otherVolumeData.setName("aaa");
    otherVolumeData.setVolumeId(11111);
    otherVolumeData.setVolumeStatus(VolumeStatus.Available);
    memoryVolumeStore.saveVolume(otherVolumeData);
    volume = memoryVolumeStore.getVolumeNotDeadByName("aaa");
    Assert.assertTrue(volume == otherVolumeData);
    Assert.assertTrue(volume != volumeMetadata);
  }

  
  public VolumeMetadata generateVolumeMetadata(long volumeId, long volumeSize, long segmentSize,
      int version) {
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, volumeSize, segmentSize,
        VolumeType.REGULAR, 0L, 0L);

    volumeMetadata.setVolumeId(volumeId);
    volumeMetadata.setRootVolumeId(volumeId);
    volumeMetadata.setChildVolumeId(null);
    volumeMetadata.setVolumeSize(volumeSize);
    volumeMetadata.setExtendingSize(0);
    volumeMetadata.setName("test");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Creating);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(volumeSize);
    volumeMetadata.setDeadTime(0L);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setPageWrappCount(128);
    volumeMetadata.setSegmentWrappCount(10);
    volumeMetadata.setVersion(version);
    SegId segId = new SegId(37002, 11);
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());
    volumeMetadata.addSegmentMetadata(segmentMetadata, TestUtils.generateMembership());

    return volumeMetadata;
  }
}
