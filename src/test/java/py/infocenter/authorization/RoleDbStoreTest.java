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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.Role;
import py.icshare.authorization.RoleStore;



@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class RoleDbStoreTest extends AuthorizationTestBase {

  private static final Logger logger = LoggerFactory.getLogger(RoleDbStoreTest.class);

  @Autowired
  RoleStore roleStore;

  @Autowired
  ApiStore apiStore;



  @Before
  public void init() {
    roleStore.cleanRoles();
    apiStore.cleanApis();
    for (ApiToAuthorize api : prepareApis()) {
      apiStore.saveApi(api);
    }
  }

  /**
   * Test save new role. * Test step: 1. create a role with two apis permissions, then list roles
   * hope to find just created role; 2. list all apis hope get the apis that the role just created
   * has.
   */
  @Test
  public void testSaveRole() {
    roleStore.saveRole(prepareRole());
    Role roleGet = roleStore.getRoleByName("test");
    assertNotNull(roleGet);
    assertEquals(2L, roleGet.getPermissions().size());

    List<ApiToAuthorize> apis = apiStore.listApis();
    assertEquals(2L, apis.size());
  }

  /**
   * Test update a role. * Test step: 1. create a role with two apis permissions; 2. get role just
   * created and remove all its permissions; 3. get this role again and check now permissions is
   * empty.
   */
  @Test
  public void testUpdateRole() {
    roleStore.saveRole(prepareRole());
    Role roleGet = roleStore.getRoleByName("test");
    assertNotNull(roleGet);
    assertEquals(2L, roleGet.getPermissions().size());
    roleGet.getPermissions().clear();
    roleStore.saveRole(roleGet);
    Role roleUpdated = roleStore.getRoleByName("test");
    assertTrue(roleUpdated.getPermissions().isEmpty());

    List<ApiToAuthorize> apis = apiStore.listApis();
    assertEquals(2L, apis.size());
  }

  /**
   * Test delete a role. * Test step: 1. create a role with two apis permissions; 2. delete just
   * created role; 3. get this role and hope get nothing.
   */
  @Test
  public void testDeleteRole() {
    roleStore.saveRole(prepareRole());
    Role roleGet = roleStore.getRoleByName("test");
    assertEquals(2L, roleGet.getPermissions().size());
    roleStore.deleteRole(roleGet);
    Role roleGetAfterDelete = roleStore.getRoleByName("test");
    assertNull(roleGetAfterDelete);

    List<ApiToAuthorize> apis = apiStore.listApis();
    assertEquals(2L, apis.size());
  }

  @Test
  public void testGetRoleNotExists() {
    Role roleGet = roleStore.getRoleByName("notExists");
    assertNull(roleGet);
    roleGet = roleStore.getRoleById(-1L);
    assertNull(roleGet);
  }

  /**
   * Test save two roles with same name but different id. Hope failed to save both.
   */
  @Test(expected = org.springframework.dao.DataIntegrityViolationException.class)
  public void testSaveRoleWithSameName() {
    Role role1 = prepareRole();
    Role role2 = prepareRole();
    role2.setId(2L);
    roleStore.saveRole(role1);
    roleStore.saveRole(role2);
  }

  /**
   * Test delete role is not existed, just nothing happened.
   */
  @Test
  public void testDeleteRoleNotExists() {
    roleStore.deleteRole(prepareRole());
  }

  @After
  public void clean() {
    roleStore.cleanRoles();
    apiStore.cleanApis();
  }
}
