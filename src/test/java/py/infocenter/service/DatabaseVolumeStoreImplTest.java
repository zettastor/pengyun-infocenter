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
import static py.volume.VolumeInAction.NULL;

import java.util.Date;
import java.util.List;
import org.hibernate.HibernateException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.infocenter.store.VolumeStore;
import py.infocenter.test.base.InformationCenterAppConfigTest;
import py.test.TestBase;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class DatabaseVolumeStoreImplTest extends TestBase {

  VolumeStore volumeStore = null;
  private VolumeMetadata volumeMetadata = new VolumeMetadata();

  
  @Before
  public void init() throws Exception {
    super.init();
    @SuppressWarnings("resource")
    ApplicationContext ctx = new AnnotationConfigApplicationContext(
        InformationCenterAppConfigTest.class);
    volumeStore = (VolumeStore) ctx.getBean("dbVolumeStore");
    volumeStore.clearData();
  }

  @Test
  public void testGetVolumeNotDeadByNameInDb() {
    volumeMetadata.setVolumeId(37002);
    volumeMetadata.setName("stdname");
    volumeMetadata.setVolumeType(VolumeType.REGULAR);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setAccountId(SUPERADMIN_ACCOUNT_ID);
    volumeMetadata.setSegmentSize(1008);
    volumeMetadata.setVolumeCreatedTime(new Date());
    volumeMetadata.setLastExtendedTime(new Date());
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setPageWrappCount(128);
    volumeMetadata.setSegmentWrappCount(10);
    volumeMetadata.setInAction(NULL);

    //test get a volumeMetadata from DB is OK
    volumeStore.saveVolume(volumeMetadata);
    VolumeMetadata volumeInDb = volumeStore.getVolumeNotDeadByName("stdname");
    Assert.assertNotNull(volumeInDb);

    volumeInDb = volumeStore.getVolumeNotDeadByName("name");
    Assert.assertNull(volumeInDb);

    //test get a volumeMetadata in dead status
    VolumeMetadata otherVolume = new VolumeMetadata();
    otherVolume.setVolumeId(371111);
    otherVolume.setName("aaaa");
    otherVolume.setVolumeType(VolumeType.REGULAR);
    otherVolume.setVolumeStatus(VolumeStatus.Dead);
    otherVolume.setAccountId(1862755152385798555L);
    otherVolume.setSegmentSize(1008);
    volumeStore.saveVolume(otherVolume);
    volumeInDb = volumeStore.getVolumeNotDeadByName("aaaa");
    Assert.assertNull(volumeInDb);
  }

  @Test
  public void testDatabaseStore() {
    VolumeMetadata volume = new VolumeMetadata();
    volume.setVolumeId(111111);
    volume.setRootVolumeId(111111);
    volume.setName("first");
    volume.setVolumeType(VolumeType.REGULAR);
    volume.setVolumeStatus(VolumeStatus.Available);
    volume.setAccountId(1862755152385798555L);
    volume.setSegmentSize(1008);
    volume.setVolumeCreatedTime(new Date());
    volume.setLastExtendedTime(new Date());
    volume.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volume.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume.setPageWrappCount(128);
    volume.setSegmentWrappCount(10);
    volume.setInAction(NULL);

    VolumeMetadata volume1 = new VolumeMetadata();
    volume1.setVolumeId(222222);
    volume1.setName("second");
    volume1.setVolumeType(VolumeType.REGULAR);
    volume1.setVolumeStatus(VolumeStatus.Dead);
    volume1.setAccountId(1862755152385798555L);
    volume1.setSegmentSize(1008);
    volume1.setVolumeCreatedTime(new Date());
    volume1.setLastExtendedTime(new Date());
    volume1.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volume1.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume1.setPageWrappCount(128);
    volume1.setSegmentWrappCount(10);
    volume1.setInAction(NULL);

    VolumeMetadata volume2 = new VolumeMetadata();
    volume2.setVolumeId(111111);
    volume2.setRootVolumeId(111111);
    volume2.setName("first");
    volume2.setVolumeType(VolumeType.LARGE);
    volume2.setVolumeStatus(VolumeStatus.Available);
    volume2.setAccountId(1862755152385798555L);
    volume2.setSegmentSize(1008);
    volume2.setVolumeCreatedTime(new Date());
    volume2.setLastExtendedTime(new Date());
    volume2.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volume2.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume2.setPageWrappCount(128);
    volume2.setSegmentWrappCount(10);
    volume2.setInAction(NULL);

    VolumeMetadata volume3 = new VolumeMetadata();
    volume3.setVolumeId(111111);
    volume3.setRootVolumeId(111111);
    volume3.setName("first");
    volume3.setVolumeType(VolumeType.LARGE);
    volume3.setVolumeStatus(VolumeStatus.Available);
    volume3.setAccountId(1862755152385798555L);
    volume3.setSegmentSize(1008);
    volume3.setVolumeCreatedTime(new Date());
    volume3.setLastExtendedTime(new Date());
    volume3.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    volume3.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volume3.setPageWrappCount(128);
    volume3.setSegmentWrappCount(10);
    volume3.setInAction(NULL);

    //save volume
    volumeStore.saveVolume(volume);
    volumeStore.saveVolume(volume1);

    try {
      volumeStore.saveVolume(volume2);
    } catch (HibernateException e) {
      Assert.assertTrue(true);
    }

    //list volume
    List<VolumeMetadata> volumes = volumeStore.listVolumes();
    Assert.assertNotNull(volumes);
    Assert.assertEquals(volumes.size(), 2);

    //get volume
    VolumeMetadata volumeGet = null;
    volumeGet = volumeStore.getVolume(111111L);
    Assert.assertNotNull(volumeGet);

    volumeGet = volumeStore.getVolume(111111L);
    Assert.assertEquals("first", volumeGet.getName());
    Assert.assertEquals("LARGE", volumeGet.getVolumeType().toString());
  }

  @After
  public void cleanUp() {
  }
}