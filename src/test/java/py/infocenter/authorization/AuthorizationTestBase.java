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
