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

package py.infocenter.authorization;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.ResourceStore;
import py.test.TestBase;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class ResourceDbStoreTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ResourceDbStoreTest.class);

  @Autowired
  private ResourceStore resourceStore;

  @Before
  public void init() {
    resourceStore.cleanResources();
  }

  /**
   * Test save resource. * Test step: 1. Create a resource and save it; 2. Get resource by id and
   * also by name, compare resource get from database and the resource created.
   */
  @Test
  public void testSaveResource() {
    PyResource resource = new PyResource(1L, "volume1", PyResource.ResourceType.Volume.name());
    resourceStore.saveResource(resource);
    PyResource resourceGetById = resourceStore.getResourceById(1L);
    assertEquals(resource, resourceGetById);
    PyResource resourceGetByName = resourceStore.getResourceByName("volume1");
    assertEquals(resource, resourceGetByName);

  }

  @Test
  public void testDeleteResource() {

  }

  @After
  public void clean() {
    resourceStore.cleanResources();
  }
}
