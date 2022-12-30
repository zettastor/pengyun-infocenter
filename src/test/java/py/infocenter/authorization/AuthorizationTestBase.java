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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.Role;
import py.test.TestBase;
import py.thrift.infocenter.service.CreateRoleRequest;
import py.thrift.share.AccountTypeThrift;
import py.thrift.share.CreateAccountRequest;


public class AuthorizationTestBase extends TestBase {

  public static ApiToAuthorize prepareApi() {
    return new ApiToAuthorize("testAPI", ApiToAuthorize.ApiCategory.Other.name());
  }


  
  public static List<ApiToAuthorize> prepareApis() {
    List<ApiToAuthorize> apis = new ArrayList<>();
    apis.add(new ApiToAuthorize("api1", ApiToAuthorize.ApiCategory.Volume.name()));
    apis.add(new ApiToAuthorize("api2", ApiToAuthorize.ApiCategory.Domain.name()));
    return apis;
  }


  
  public static Role prepareRole() {
    Role role = new Role(1L, "test");
    for (ApiToAuthorize api : prepareApis()) {
      role.addPermission(api);
    }
    return role;
  }


  
  public static CreateAccountRequest prepareCreateAccountRequest(long creatingAccountId) {
    CreateAccountRequest createAccountRequest = new CreateAccountRequest("user", "312",
        AccountTypeThrift.Regular, Constants.SUPERADMIN_ACCOUNT_ID, new HashSet<Long>());
    createAccountRequest.setCreatingAccountId(creatingAccountId);
    return createAccountRequest;
  }

  public static CreateRoleRequest prepareCreateRoleRequest(Set<String> apisToAuthorize) {
    return new CreateRoleRequest(RequestIdBuilder.get(), Constants.SUPERADMIN_ACCOUNT_ID,
        "user", "user", apisToAuthorize);
  }
}
