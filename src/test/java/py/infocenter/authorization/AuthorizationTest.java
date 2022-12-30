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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.RoleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.InformationCenterAppEngine;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.control.OperationStore;
import py.instance.InstanceStatus;
import py.thrift.infocenter.service.AssignRolesRequest;
import py.thrift.infocenter.service.CreateRoleNameExistedExceptionThrift;
import py.thrift.infocenter.service.CreateRoleRequest;
import py.thrift.infocenter.service.CreateRoleResponse;
import py.thrift.infocenter.service.DeleteRolesRequest;
import py.thrift.infocenter.service.ListApisRequest;
import py.thrift.infocenter.service.ListApisResponse;
import py.thrift.infocenter.service.ListRolesRequest;
import py.thrift.infocenter.service.ListRolesResponse;
import py.thrift.infocenter.service.UpdateRoleRequest;
import py.thrift.infocenter.service.UpdateRoleResponse;
import py.thrift.share.AccountAlreadyExistsExceptionThrift;
import py.thrift.share.AccountMetadataThrift;
import py.thrift.share.AccountTypeThrift;
import py.thrift.share.AuthenticateAccountRequest;
import py.thrift.share.AuthenticationFailedExceptionThrift;
import py.thrift.share.CreateAccountRequest;
import py.thrift.share.CreateAccountResponse;
import py.thrift.share.DeleteAccountsRequest;
import py.thrift.share.ListAccountsRequest;
import py.thrift.share.ListAccountsResponse;
import py.thrift.share.OlderPasswordIncorrectExceptionThrift;
import py.thrift.share.ResetAccountPasswordRequest;
import py.thrift.share.RoleNotExistedExceptionThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.UpdatePasswordRequest;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class AuthorizationTest extends AuthorizationTestBase {

  private static final Logger logger = LoggerFactory.getLogger(AuthorizationTest.class);
  private static final long superAdminAccountId = Constants.SUPERADMIN_ACCOUNT_ID;
  private static final long regularUserAccountId = RequestIdBuilder.get();
  @Autowired
  private InformationCenterImpl informationCenter;

  @Autowired
  private PySecurityManager securityManager;

  @Autowired
  @Qualifier("memory")
  private AccountStore accountStore;

  @Autowired
  private RoleStore roleStore;

  @Autowired
  private ApiStore apiStore;

  @Autowired
  private OperationStore operationStore;

  @Mock
  private InformationCenterAppEngine informationCenterAppEngine;

  @Mock
  private InfoCenterAppContext infoCenterAppContext;


  
  @Before
  public void init() throws Exception {
    accountStore.deleteAllAccounts();
    roleStore.cleanRoles();
    apiStore.cleanApis();
    securityManager.initApiInDb();
    securityManager.createSuperAdminRole();
    securityManager.createSuperAdminAccount();
    operationStore.clearMemory();
    informationCenter.setInformationCenterAppEngine(informationCenterAppEngine);
    informationCenter.setAppContext(infoCenterAppContext);

    when(infoCenterAppContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
  }

  /**
   * Test init security manager so that in the system, there will be a superAdmin account with
   * superAdmin role who has authorization to all the apis, only with this account we can continue
   * using the system list create other account and role. * Test step: 1. list all the account in
   * the system hope to find only one superAdmin account; 2. check superAdmin account has role of
   * superAdmin; 3. check superAdmin role has all permissions.
   */
  @Test
  public void testSecurityManagerInitialized() throws Exception {
    // step 1
    ListAccountsRequest listAccountsRequest = new ListAccountsRequest(superAdminAccountId);
    ListAccountsResponse listAccountsResponse = informationCenter.listAccounts(listAccountsRequest);
    List<AccountMetadataThrift> allAccounts = listAccountsResponse.getAccounts();
    assertEquals(1L, allAccounts.size());
    AccountMetadataThrift account = allAccounts.get(0);
    assertEquals(superAdminAccountId, account.getAccountId());

    // step 2
    assertEquals(1L, account.getRolesSize());
    RoleThrift superAdmin = (RoleThrift) account.getRoles().toArray()[0];
    assertEquals("superadmin", superAdmin.getName());

    // step 3
    ListRolesRequest listRolesRequest = new ListRolesRequest(RequestIdBuilder.get(),
        superAdminAccountId);
    ListRolesResponse listRolesResponse = informationCenter.listRoles(listRolesRequest);
    assertEquals(0L, listRolesResponse.getRolesSize());
    ListApisRequest listApisRequest = new ListApisRequest(RequestIdBuilder.get(),
        superAdminAccountId);
    ListApisResponse listApisResponse = informationCenter.listApis(listApisRequest);
    assertEquals(superAdmin.getPermissions(), listApisResponse.getApis());
  }

  /**
   * Test create a account for further test grant permission. * Test step: 1. first create a user
   * account; 2. then list all accounts and hope to get two: one is superAdmin and second is user
   * just created; 3. create another account with the same account name with first one, it will fail
   * and catch exception {@link AccountAlreadyExistsExceptionThrift}.
   */
  @Test(expected = AccountAlreadyExistsExceptionThrift.class)
  public void testCreateUserAccount() throws Exception {
    String failMsg;
    // step 1
    informationCenter.createAccount(prepareCreateAccountRequest(regularUserAccountId));

    // step 2ss
    ListAccountsRequest listAccountsRequest = new ListAccountsRequest(superAdminAccountId);
    ListAccountsResponse listAccountsResponse = informationCenter.listAccounts(listAccountsRequest);
    List<AccountMetadataThrift> allAccounts = listAccountsResponse.getAccounts();
    assertEquals(2L, allAccounts.size());
    boolean foundJustCreatedAccount = false;
    for (AccountMetadataThrift accountMetadataThrift : allAccounts) {
      if (accountMetadataThrift.getAccountName().equals("user")) {
        foundJustCreatedAccount = true;
        break;
      }
    }
    if (!foundJustCreatedAccount) {
      failMsg = "Cannot found just created account, there must be some problem.";
      logger.error(failMsg);
      fail(failMsg);
    }

    // step 3
    informationCenter.createAccount(prepareCreateAccountRequest(regularUserAccountId));
  }

  /**
   * Test create a new user role. * Test step: 1. create a user role with createVolume and
   * deleteVolume permission; 2. list all roles and hope find two roles: one is superadmin and
   * another is user; 3. create another role with same role name with first one, it will fail and
   * catch exception {@link CreateRoleNameExistedExceptionThrift}.
   */
  @Test(expected = CreateRoleNameExistedExceptionThrift.class)
  public void testCreateUserRole() throws Exception {
    String failMsg;

    // step 1 create user role
    Set<String> apisToAuthorize = new HashSet<>();
    apisToAuthorize.add("createVolume");
    apisToAuthorize.add("deleteVolume");
    CreateRoleResponse createRoleResponse = informationCenter
        .createRole(prepareCreateRoleRequest(apisToAuthorize));

    // step 2 list all roles
    ListRolesRequest listRolesRequest = new ListRolesRequest(RequestIdBuilder.get(),
        superAdminAccountId);
    ListRolesResponse listRolesResponse = informationCenter.listRoles(listRolesRequest);
    assertEquals(1, listRolesResponse.getRolesSize());
    boolean foundJustCreatedRole = false;
    for (RoleThrift roleThrift : listRolesResponse.getRoles()) {
      if (roleThrift.getName().equals("user")) {
        assertEquals(apisToAuthorize.size(), roleThrift.getPermissionsSize());
        foundJustCreatedRole = true;
        break;
      }
    }
    if (!foundJustCreatedRole) {
      failMsg = "Cannot found just created role, there must be some problem.";
      logger.error(failMsg);
      fail(failMsg);
    }
    // step 3
    informationCenter.createRole(prepareCreateRoleRequest(apisToAuthorize));
  }

  /**
   * Test assign user role to user account. * Test step: 1. assign user role to user account; 2.
   * list user account user filter that controlcenter.listAccounts will only return the user
   * account; 3. check user account has one role which user role.
   */
  @Test
  public void testAssignRoleToUser() throws Exception {
    // step 1
    informationCenter.createAccount(prepareCreateAccountRequest(regularUserAccountId));
    Set<String> apisToAuthorize = new HashSet<>();
    apisToAuthorize.add("createVolume");
    apisToAuthorize.add("deleteVolume");
    CreateRoleResponse createRoleResponse = informationCenter
        .createRole(prepareCreateRoleRequest(apisToAuthorize));
    Set<Long> rolesToUser = new HashSet<>();
    rolesToUser.add(createRoleResponse.getCreatedRoleId());
    AssignRolesRequest assignRolesRequest = new AssignRolesRequest(RequestIdBuilder.get(),
        superAdminAccountId,
        regularUserAccountId, rolesToUser);
    informationCenter.assignRoles(assignRolesRequest);

    // step 2
    ListAccountsRequest listAccountsRequest = new ListAccountsRequest(superAdminAccountId);
    Set<Long> listAccounts = new HashSet<>();
    listAccounts.add(regularUserAccountId);
    listAccountsRequest.setListAccountIds(listAccounts);
    ListAccountsResponse listAccountsResponse = informationCenter.listAccounts(listAccountsRequest);

    // step 3
    List<AccountMetadataThrift> allAccounts = listAccountsResponse.getAccounts();
    assertEquals(1L, allAccounts.size());
    AccountMetadataThrift userAccount = listAccountsResponse.getAccounts().get(0);
    assertEquals(1L, userAccount.getRolesSize());
    for (RoleThrift role : userAccount.getRoles()) {
      assertEquals("user", role.getName());
    }
  }

  /**
   * Test update role. * Test step: 1. update user role with one more permission: listRoles; 2. list
   * role again and check user role now has the newest permission list; 3. use user account to list
   * role, this time it will success because user role has permission; 4. update role which is not
   * existed in the system, it will fail and catch {@link RoleNotExistedExceptionThrift}
   */
  @Test(expected = RoleNotExistedExceptionThrift.class)
  public void testUpdateRole() throws Exception {
    // step 1
    final long newRoleId;
    informationCenter.createAccount(prepareCreateAccountRequest(regularUserAccountId));
    Set<String> apisToAuthorize = new HashSet<>();
    apisToAuthorize.add("createVolume");
    apisToAuthorize.add("deleteVolume");
    CreateRoleResponse createRoleResponse = informationCenter
        .createRole(prepareCreateRoleRequest(apisToAuthorize));
    newRoleId = createRoleResponse.getCreatedRoleId();
    Set<Long> rolesToUser = new HashSet<>();
    rolesToUser.add(newRoleId);
    AssignRolesRequest assignRolesRequest = new AssignRolesRequest(RequestIdBuilder.get(),
        superAdminAccountId,
        regularUserAccountId, rolesToUser);
    informationCenter.assignRoles(assignRolesRequest);
    ListRolesRequest listRolesRequest = new ListRolesRequest(RequestIdBuilder.get(),
        superAdminAccountId);
    ListRolesResponse listRolesResponse = informationCenter.listRoles(listRolesRequest);
    assertEquals(1, listRolesResponse.getRolesSize());
    UpdateRoleRequest updateRoleRequest = new UpdateRoleRequest(RequestIdBuilder.get(),
        superAdminAccountId,
        newRoleId, "user", "user", apisToAuthorize);
    UpdateRoleResponse updateRoleResponse = informationCenter.updateRole(updateRoleRequest);

    // step 2 list all roles
    listRolesRequest = new ListRolesRequest(RequestIdBuilder.get(), superAdminAccountId);
    listRolesResponse = informationCenter.listRoles(listRolesRequest);
    assertEquals(1, listRolesResponse.getRolesSize());
    for (RoleThrift roleThrift : listRolesResponse.getRoles()) {
      if (roleThrift.getName().equals("user")) {
        assertEquals(apisToAuthorize.size(), roleThrift.getPermissionsSize());
        break;
      }
    }

    // step 3
    listRolesRequest.setAccountId(regularUserAccountId);
    informationCenter.listRoles(listRolesRequest);

    // step 4
    updateRoleRequest.setRoleId(-1L);
    informationCenter.updateRole(updateRoleRequest);
  }

  @Test
  public void testDeleteAccount() throws Exception {
    String failMsg;
    // step 1 create an normal account
    CreateAccountRequest createAccountRequest =
        new CreateAccountRequest("userDelAcc", "312",
            AccountTypeThrift.Admin, Constants.SUPERADMIN_ACCOUNT_ID, new HashSet<Long>());
    CreateAccountResponse createAccountResponse = informationCenter
        .createAccount(createAccountRequest);
    long accountIdDelAcc = createAccountResponse.getAccountId();

    Set<Long> deletingAccounts = new HashSet<>();
    deletingAccounts.add(accountIdDelAcc);
    DeleteAccountsRequest request = new DeleteAccountsRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID,
        deletingAccounts);
    informationCenter.deleteAccounts(request);

    // step 2ss
    ListAccountsRequest listAccountsRequest = new ListAccountsRequest(superAdminAccountId);
    ListAccountsResponse listAccountsResponse = informationCenter.listAccounts(listAccountsRequest);
    List<AccountMetadataThrift> allAccounts = listAccountsResponse.getAccounts();
    boolean foundJustCreatedAccount = false;
    for (AccountMetadataThrift accountMetadataThrift : allAccounts) {
      if (accountMetadataThrift.getAccountName().equals("userDelAcc")) {
        foundJustCreatedAccount = true;
        break;
      }
    }
    if (foundJustCreatedAccount) {
      failMsg = "Found just created account, the deleting not success.";
      logger.error(failMsg);
      fail(failMsg);
    }
  }

  /**
   * Test update account. * Test step: 1. create an normal account; 2. step 2 create a role; 3.
   * assign the role to the account; 4. test the account if updated with the right password 5. test
   * the account if updated with the wrong password {@link OlderPasswordIncorrectExceptionThrift}
   */
  @Test(expected = OlderPasswordIncorrectExceptionThrift.class)
  public void testUpdatePassword() throws Exception {

    // step 1 create an normal account
    CreateAccountRequest createAccountRequest =
        new CreateAccountRequest("userUpAcc", "312",
            AccountTypeThrift.Admin, Constants.SUPERADMIN_ACCOUNT_ID, new HashSet<Long>());
    CreateAccountResponse createAccountResponse = informationCenter
        .createAccount(createAccountRequest);
    long accountIdUpAcc = createAccountResponse.getAccountId();

    Set<String> apisToAuthorize = new HashSet<>();

    // step 2 create a role
    CreateRoleRequest createRoleRequest = new CreateRoleRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID,
        "userUpAcc", "userUpAcc", apisToAuthorize);
    CreateRoleResponse createRoleResponse = informationCenter.createRole(createRoleRequest);
    long newRoleId = createRoleResponse.getCreatedRoleId();
    Set<Long> rolesToUser = new HashSet<>();

    ListRolesRequest listRolesRequest = new ListRolesRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID);
    ListRolesResponse listRolesResponse = informationCenter.listRoles(listRolesRequest);
    for (RoleThrift roleThrift : listRolesResponse.getRoles()) {
      if (roleThrift.getName().equals("userUpAcc")) {
        newRoleId = roleThrift.getId();
        break;
      }
    }

    // step 3 assign the role to the account
    rolesToUser.add(newRoleId);
    AssignRolesRequest assignRolesRequest = new AssignRolesRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID,
        accountIdUpAcc, rolesToUser);
    informationCenter.assignRoles(assignRolesRequest);

    // step 4 test the account if updated with the right password
    UpdatePasswordRequest updateAccountRequest = new UpdatePasswordRequest("userUpAcc",
        "312", "123", accountIdUpAcc);
    informationCenter.updatePassword(updateAccountRequest);

    AuthenticateAccountRequest authenticateAccountRequest =
        new AuthenticateAccountRequest("userUpAcc", "123");
    informationCenter.authenticateAccount(authenticateAccountRequest);

    // step 5 test the account if updated with the wrong password
    UpdatePasswordRequest updateAccountRequestNew = new UpdatePasswordRequest("userUpAcc",
        "312", "122", accountIdUpAcc);
    informationCenter.updatePassword(updateAccountRequestNew);

    // step 6 delete the account and the role
    Set<Long> deletingAccounts = new HashSet<>();
    deletingAccounts.add(accountIdUpAcc);
    DeleteAccountsRequest request = new DeleteAccountsRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID,
        deletingAccounts);
    informationCenter.deleteAccounts(request);

    DeleteRolesRequest request1 = new DeleteRolesRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID,
        rolesToUser);
    informationCenter.deleteRoles(request1);
  }

  /**
   * Test update resetAccountPassword. * Test step: 1. create an normal account; 2. reset the
   * password; 3. test the account if reset with the right password; 4. test the account if reset
   * with the wrong password; 5. delete the account {@link AuthenticationFailedExceptionThrift}
   */
  @Test(expected = AuthenticationFailedExceptionThrift.class)
  public void testResetAccountPassword() throws Exception {

    // step 1 create an normal account
    CreateAccountRequest createAccountRequest =
        new CreateAccountRequest("userResPwd", "312",
            AccountTypeThrift.Admin, Constants.SUPERADMIN_ACCOUNT_ID, new HashSet<Long>());
    CreateAccountResponse createAccountResponse = informationCenter
        .createAccount(createAccountRequest);
    long accountIdUpAcc = createAccountResponse.getAccountId();

    // step 2 reset the password
    ResetAccountPasswordRequest request = new ResetAccountPasswordRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID, accountIdUpAcc, AccountTypeThrift.SuperAdmin);

    informationCenter.resetAccountPassword(request);

    // step 3 test the account if reset with the right password
    AuthenticateAccountRequest authenticateAccountRequest =
        new AuthenticateAccountRequest("userResPwd", "pengyun");
    informationCenter.authenticateAccount(authenticateAccountRequest);

    // step 4 test the account if reset with the wrong password
    AuthenticateAccountRequest authenticateAccountRequest1 =
        new AuthenticateAccountRequest("userResPwd", "pengyun1");
    informationCenter.authenticateAccount(authenticateAccountRequest1);

    // step 5 delete the account
    Set<Long> deletingAccounts = new HashSet<>();
    deletingAccounts.add(accountIdUpAcc);
    DeleteAccountsRequest request2 = new DeleteAccountsRequest(RequestIdBuilder.get(),
        Constants.SUPERADMIN_ACCOUNT_ID,
        deletingAccounts);
    informationCenter.deleteAccounts(request2);
  }


  
  @After
  public void clean() {
    accountStore.deleteAllAccounts();
    roleStore.cleanRoles();
    apiStore.cleanApis();
  }
}