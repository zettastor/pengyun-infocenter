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

package py.infocenter.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceInformation;
import py.test.TestBase;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class InstanceMaintenanceStoreImplTest extends TestBase {

  @Autowired
  InstanceMaintenanceDbStore instanceMaintenanceStore;

  @Before
  public void init() {
    instanceMaintenanceStore.clear();
  }

  /**
   * Test save a record and update it.
   */
  @Test
  public void testSaveAndUpdate() {
    assertEquals(0, instanceMaintenanceStore.listAll().size());
    InstanceMaintenanceInformation maintenanceInformation = new InstanceMaintenanceInformation(
        1L, 0L, 100L, "10.0.0.2");
    instanceMaintenanceStore.save(maintenanceInformation);
    InstanceMaintenanceInformation byId = instanceMaintenanceStore.getById(1L);
    assertNotNull(byId);
    assertEquals(0L, byId.getStartTime());
    assertEquals(100L, byId.getEndTime());
    maintenanceInformation.setStartTime(100L);
    maintenanceInformation.setEndTime(200L);
    instanceMaintenanceStore.save(maintenanceInformation);

    InstanceMaintenanceInformation byId1 = instanceMaintenanceStore.getById(1L);
    assertNotNull(byId1);
    assertEquals(100L, byId1.getStartTime());
    assertEquals(200L, byId1.getEndTime());
    assertEquals(1, instanceMaintenanceStore.listAll().size());
  }

  /**
   * Test save a record and later delete it.
   */
  @Test
  public void testDelete() {
    assertEquals(0, instanceMaintenanceStore.listAll().size());
    InstanceMaintenanceInformation maintenanceInformation = new InstanceMaintenanceInformation(
        1L, 0L, 100L, "10.0.0.2");

    //save
    instanceMaintenanceStore.save(maintenanceInformation);

    //get by id
    InstanceMaintenanceInformation byId = instanceMaintenanceStore.getById(1L);
    assertNotNull(byId);

    //delete and check
    instanceMaintenanceStore.delete(maintenanceInformation);
    byId = instanceMaintenanceStore.getById(1L);
    assertNull(byId);
    assertEquals(0, instanceMaintenanceStore.listAll().size());

    //save
    instanceMaintenanceStore.save(maintenanceInformation);

    //delete by id and check
    instanceMaintenanceStore.deleteById(1L);
    assertEquals(0, instanceMaintenanceStore.listAll().size());
  }

  @After
  public void clean() {
    instanceMaintenanceStore.clear();
  }
}