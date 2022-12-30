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

import static py.common.Constants.DEFAULT_PASSWORD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.icshare.AccountMetadata;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.Role;
import py.icshare.authorization.RoleStore;
import py.thrift.infocenter.service.CreateRoleNameExistedExceptionThrift;
import py.thrift.share.AccessDeniedExceptionThrift;
import py.thrift.share.AccountAlreadyExistsExceptionThrift;
import py.thrift.share.AccountNotFoundExceptionThrift;
import py.thrift.share.CrudBuiltInRoleExceptionThrift;
import py.thrift.share.CrudSuperAdminAccountExceptionThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.OlderPasswordIncorrectExceptionThrift;
import py.thrift.share.PermissionNotGrantExceptionThrift;
import py.thrift.share.ResourceNotExistsExceptionThrift;
import py.thrift.share.RoleIsAssignedToAccountsExceptionThrift;
import py.thrift.share.RoleNotExistedExceptionThrift;



public class PySecurityManager {

  private static final Logger logger = LoggerFactory.getLogger(PySecurityManager.class);
  private RoleStore roleStore;
  private AccountStore accountStore;
  private ApiStore apiStore;
  private Map<String, ApiToAuthorize> apiId2ApiToAuthorize = new HashMap<>();
  private Set<ApiToAuthorize> apisToInit;
  private ResourceStore resourceStore;
  private Set<Role> builtInRoles;


  public PySecurityManager(Set<ApiToAuthorize> apis) {
    this.apisToInit = apis;
  }


  public void initApiInDb() {
    List<ApiToAuthorize> apisInDb = apiStore.listApis();
    Set<ApiToAuthorize> removedApis = new HashSet<>();
    // remove all the apisToInit already in database
    for (ApiToAuthorize api : apisInDb) {
      for (ApiToAuthorize apiInMemory : apisToInit) {
        if (apiInMemory.getApiName().equals(api.getApiName())) {
          removedApis.add(api);
          break;
        }
      }
    }

    apisToInit.removeAll(removedApis);

    for (ApiToAuthorize api : apisToInit) {
      apiStore.saveApi(api);
    }
    apisToInit.addAll(removedApis);

    for (ApiToAuthorize api : apisToInit) {
      apiId2ApiToAuthorize.put(api.getApiName(), api);
    }
  }


  public boolean hasPermission(Long accountId, String permission)
      throws PermissionNotGrantExceptionThrift, AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    for (Role role : account.getRoles()) {
      for (ApiToAuthorize api : role.getPermissions()) {
        if (api.getApiName().contains(permission)) {
          return true;
        }
      }
    }
    String msg = account + " doesn't have permission: " + permission;
    logger.error(msg);
    throw new PermissionNotGrantExceptionThrift().setDetail(msg);
  }


  public Set<ApiToAuthorize> collectPermissionsByAccountId(Long accountId)
      throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    Set<ApiToAuthorize> allPermissions = new HashSet<>();
    for (Role role : account.getRoles()) {
      allPermissions.addAll(role.getPermissions());
    }
    return allPermissions;
  }


  public Role createRole(String name, String description, Set<String> apis, boolean builtIn,
      boolean superAdmin)
      throws CreateRoleNameExistedExceptionThrift {
    if (null == name || null == description || apis == null) {
      throw new IllegalArgumentException();
    }
    if (null != roleStore.getRoleByName(name)) {
      logger.warn("Role with name {} is already existed, create failed.", name);

      throw new CreateRoleNameExistedExceptionThrift();
    }
    Role newRole = new Role(RequestIdBuilder.get(), name, builtIn, superAdmin);
    newRole.setDescription(description);
    Set<ApiToAuthorize> permissions = new HashSet<>();
    newRole.setPermissions(permissions);
    for (String apiName : apis) {
      ApiToAuthorize apiToAuthorize = apiId2ApiToAuthorize.get(apiName);
      permissions.add(apiToAuthorize);
    }
    roleStore.saveRole(newRole);
    return newRole;
  }


  public AccountMetadata createSuperAdminAccount() throws Exception {
    AccountMetadata account = getAccount(Constants.SUPERADMIN_DEFAULT_ACCOUNT_NAME);
    if (account == null) {
      logger.warn("now create default account: admin ");
      try {
        Role superAdmin = roleStore.getRoleByName(Constants.SUPERADMIN_ACCOUNT_TYPE.toLowerCase());
        Set<Long> roleIdsToAccount = new HashSet<>();
        roleIdsToAccount.add(superAdmin.getId());
        account = createAccount(Constants.SUPERADMIN_DEFAULT_ACCOUNT_NAME,
            Constants.SUPERADMIN_DEFAULT_ACCOUNT_PASSWORD, Constants.SUPERADMIN_ACCOUNT_TYPE,
            Constants.SUPERADMIN_ACCOUNT_ID, roleIdsToAccount);
      } catch (Exception e) {
        logger.warn("create admin account failure", e);
        // maybe other information center has created admin account
        // successfully;
        account = getAccount(Constants.SUPERADMIN_DEFAULT_ACCOUNT_NAME);
      }

      if (account == null) {
        String errMsg =
            "cannot create default account targetAccountId:" + Constants.SUPERADMIN_ACCOUNT_ID;
        logger.error(errMsg);
        throw new Exception(errMsg);
      }
    }
    return account;
  }


  public Role updateRole(long roleId, String name, String description, Set<String> apis)
      throws RoleNotExistedExceptionThrift, CrudBuiltInRoleExceptionThrift {
    if (null == name || null == description || apis == null) {
      throw new IllegalArgumentException();
    }
    Role role = roleStore.getRoleById(roleId);
    if (null == role) {
      throw new RoleNotExistedExceptionThrift();
    }
    if (role.isBuiltIn()) {
      throw new CrudBuiltInRoleExceptionThrift().setDetail("Cannot update role " + role.getName());
    }
    role.setName(name);
    role.setDescription(description);
    Set<ApiToAuthorize> permissions = new HashSet<>();
    for (String apiName : apis) {
      ApiToAuthorize apiToAuthorize = apiId2ApiToAuthorize.get(apiName);
      if (apiToAuthorize != null) {
        permissions.add(apiToAuthorize);
      }
    }
    role.setPermissions(permissions);
    roleStore.saveRole(role);

    // also update in memory account that assigned this role
    List<AccountMetadata> modifiedAccounts = new ArrayList<>();
    for (AccountMetadata account : accountStore.listAccounts()) {
      Set<Role> oldRoles = account.getRoles();
      if (oldRoles.remove(role)) {
        oldRoles.add(role);
        modifiedAccounts.add(account);
      }
    }
    for (AccountMetadata account : modifiedAccounts) {
      accountStore.updateAccount(account);
    }
    return role;
  }


  public Role deleteRole(long roleId)
      throws RoleNotExistedExceptionThrift, CrudBuiltInRoleExceptionThrift,
      RoleIsAssignedToAccountsExceptionThrift {
    Role role = roleStore.getRoleById(roleId);
    if (null == role) {
      throw new RoleNotExistedExceptionThrift();
    }
    if (role.isBuiltIn()) {
      throw new CrudBuiltInRoleExceptionThrift().setDetail("Cannot delete role" + role.getName());
    }
    // also update in memory account that assigned this role
    for (AccountMetadata account : accountStore.listAccounts()) {
      Set<Role> oldRoles = account.getRoles();
      if (oldRoles.contains(role)) {
        throw new RoleIsAssignedToAccountsExceptionThrift()
            .setDetail("Cannot delete role" + role.getName());
      }
    }
    roleStore.deleteRole(role);
    return role;
  }


  public List<Role> assignRoles(long accountId, Set<Long> roleIds)
      throws AccountNotFoundExceptionThrift, CrudSuperAdminAccountExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    if (Constants.SUPERADMIN_ACCOUNT_ID == account.getAccountId()) {
      throw new CrudSuperAdminAccountExceptionThrift()
          .setDetail("Cannot assign roles to super admin account.");
    }
    Set<Role> roles = new HashSet<>();
    List<Role> assignedRoles = new ArrayList<>();
    account.setRoles(roles);
    for (Role role : roleStore.listRoles()) {
      if (roleIds.contains(role.getId()) && !role.isSuperAdmin()) {
        roles.add(role);
        assignedRoles.add(role);
      }
    }
    accountStore.updateAccount(account);
    return assignedRoles;
  }


  public AccountMetadata createAccount(String accountName, String password, String accountType,
      long accountId,
      Set<Long> roleIds) throws InvalidInputExceptionThrift, AccountAlreadyExistsExceptionThrift {
    if (accountName == null || accountName.length() > 64) {
      logger.error("account name is null or too long");
      throw new InvalidInputExceptionThrift();
    }
    if (password == null || password.length() > 16) {
      logger.error("password is null or too long");
      throw new InvalidInputExceptionThrift();
    }

    AccountMetadata account = accountStore.getAccount(accountName);
    if (null != account) {
      logger.error("already exist account: {}", account);
      throw new AccountAlreadyExistsExceptionThrift();
    } else {
      if (0 == accountId) {
        accountId = AccountMetadata.randomId();
      }
      logger.debug("account does not exist " + accountStore);
      Set<Role> rolesToAccount = new HashSet<>();
      for (Long roleId : roleIds) {
        rolesToAccount.add(roleStore.getRoleById(roleId));
      }
      account = accountStore
          .createAccount(accountName, password, accountType, accountId, rolesToAccount);
    }
    return account;
  }


  public AccountMetadata deleteAccount(long deletingAccountId)
      throws CrudSuperAdminAccountExceptionThrift {
    if (Constants.SUPERADMIN_ACCOUNT_ID == deletingAccountId) {
      throw new CrudSuperAdminAccountExceptionThrift()
          .setDetail("Cannot delete super admin account.");
    }
    return accountStore.deleteAccount(deletingAccountId);
  }


  public AccountMetadata authenticateAccount(String accountName, String password) {
    return accountStore.authenticateAccount(accountName, password);
  }


  public AccountMetadata updatePassword(String accountName, String newPassword, String oldPassword,
      String accountType)
      throws OlderPasswordIncorrectExceptionThrift, AccountNotFoundExceptionThrift {

    AccountMetadata account = accountStore.getAccount(accountName);
    if (account == null) {
      throw new AccountNotFoundExceptionThrift();
    }

    AccountMetadata old = accountStore.authenticateAccount(accountName, oldPassword);
    if (old != null) {
      AccountMetadata newAccount = new AccountMetadata(account.getAccountName(), newPassword,
          accountType,
          old.getAccountId());
      old.setSalt(newAccount.getSalt());
      old.setHashedPassword(newAccount.getHashedPassword());
      accountStore.updateAccount(old);
    } else {
      throw new OlderPasswordIncorrectExceptionThrift();
    }










    return account;
  }


  public Role createSuperAdminRole() {
    Role superAdmin = getRole(Constants.SUPERADMIN_ACCOUNT_TYPE.toLowerCase());
    if (null != superAdmin) {
      Set<ApiToAuthorize> permission = superAdmin.getPermissions();
      List<ApiToAuthorize> apisInDb = listApis();
      for (ApiToAuthorize apiInDb : apisInDb) {
        if (!permission.contains(apiInDb)) {
          permission.add(apiInDb);
        }
      }
      saveRole(superAdmin);
    } else {
      List<ApiToAuthorize> apisInDb = listApis();
      Set<String> allApiNames = new HashSet<>();
      for (ApiToAuthorize apiInDb : apisInDb) {
        allApiNames.add(apiInDb.getApiName());
      }
      try {
        superAdmin = createRole(Constants.SUPERADMIN_ACCOUNT_TYPE.toLowerCase(),
            Constants.SUPERADMIN_ACCOUNT_TYPE.toLowerCase(), allApiNames, true, true);
      } catch (CreateRoleNameExistedExceptionThrift e) {
        superAdmin = getRole(Constants.SUPERADMIN_ACCOUNT_TYPE.toLowerCase());

      }
    }
    return superAdmin;
  }


  public void createBuiltInRoles() {
    for (Role builtInRole : builtInRoles) {
      Role roleInDb = roleStore.getRoleByName(builtInRole.getName());
      if (roleInDb == null) {
        roleStore.saveRole(builtInRole);
      } else {
        roleInDb.setDescription(builtInRole.getDescription());
        roleInDb.setPermissions(builtInRole.getPermissions());
        roleStore.saveRole(roleInDb);
      }
    }
  }


  public AccountMetadata getAccount(String accountName) {
    return accountStore.getAccount(accountName);
  }


  public AccountMetadata getAccountById(long accountId) {
    return accountStore.getAccountById(accountId);
  }


  public List<AccountMetadata> listAccounts(long accountId) throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (account == null) {
      throw new AccountNotFoundExceptionThrift();
    }
    List<AccountMetadata> accounts = new ArrayList<>();
    if (account.getAccountType().equals("SuperAdmin")) {
      for (AccountMetadata accountToList : accountStore.listAccounts()) {
        accounts.add(accountToList);
      }
    } else {
      accounts.add(account);
    }
    return accounts;
  }


  public AccountMetadata resetAccountPassword(long targetAccountId)
      throws AccountNotFoundExceptionThrift {

    AccountMetadata account = accountStore.getAccountById(targetAccountId);
    if (account == null) {
      throw new AccountNotFoundExceptionThrift();
    }
    AccountMetadata newAccount = new AccountMetadata(account.getAccountName(), DEFAULT_PASSWORD,
        account.getAccountType(), AccountMetadata.randomId());
    account.setSalt(newAccount.getSalt());
    account.setHashedPassword(newAccount.getHashedPassword());
    accountStore.updateAccount(account);
    return account;
  }


  public Role getRole(String roleName) {
    return roleStore.getRoleByName(roleName);
  }


  public Role getRoleById(long roleId) {
    return roleStore.getRoleById(roleId);
  }


  public void saveRole(Role role) {
    roleStore.saveRole(role);
  }


  public void hasRightToAccess(long accountId, long resourceId)
      throws AccountNotFoundExceptionThrift, AccessDeniedExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    // super admin has all resources in the system so that no need to manage
    if (account.getAccountType().equals("SuperAdmin")) {
      return;
    }
    for (PyResource resource : account.getResources()) {
      if (resourceId == resource.getResourceId()) {
        return;
      }
    }

    throw new AccessDeniedExceptionThrift()
        .setDetail(accountId + " does not have access right to resource: " + resourceId);
  }


  public Set<Long> getAccessibleResourcesByType(long accountId,
      PyResource.ResourceType resourceType)
      throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    // super admin has all resources in the system so that no need to manage
    Set<Long> accessibleResources = new HashSet<>();
    if (account.getAccountType().equals("SuperAdmin")) {
      for (PyResource resource : listResources()) {
        if (resource.getResourceType().equals(resourceType.name())) {
          accessibleResources.add(resource.getResourceId());
        }
      }
    } else {
      for (PyResource resource : account.getResources()) {
        if (resource.getResourceType().equals(resourceType.name())) {
          accessibleResources.add(resource.getResourceId());
        }
      }
    }
    return accessibleResources;
  }


  public List<String> assignResources(long accountId, Set<Long> assigningResourceIds)
      throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    // super admin has all resources in the system so that no need to manage
    if (account.getAccountType().equals("SuperAdmin")) {
      return new ArrayList<>();
    }

    Set<PyResource> resources = new HashSet<>();
    List<String> assignedResourceNames = new ArrayList<>();
    account.setResources(resources);
    for (PyResource resource : resourceStore.listResources()) {
      if (assigningResourceIds.contains(resource.getResourceId())) {
        resources.add(resource);
        assignedResourceNames.add(resource.getResourceName());
      }
    }
    accountStore.updateAccount(account);
    return assignedResourceNames;
  }


  public void addResource(long accountId, PyResource newResource)
      throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    // super admin has all resources in the system so that no need to manage
    if (account.getAccountType().equals("SuperAdmin")) {
      return;
    }
    account.getResources().add(newResource);
    accountStore.updateAccount(account);
  }


  public void addResources(long accountId, Set<PyResource> newResources)
      throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    // super admin has all resources in the system so that no need to manage
    if (account.getAccountType().equals("SuperAdmin")) {
      return;
    }
    account.getResources().addAll(newResources);
    accountStore.updateAccount(account);
  }


  public void cancelResources(long accountId, Set<PyResource> cancelResources)
      throws AccountNotFoundExceptionThrift {
    AccountMetadata account = accountStore.getAccountById(accountId);
    if (null == account) {
      throw new AccountNotFoundExceptionThrift();
    }
    // super admin has all resources in the system so that no need to manage
    if (account.getAccountType().equals("SuperAdmin")) {
      return;
    }
    account.getResources().removeAll(cancelResources);
    accountStore.updateAccount(account);
  }


  public PyResource getResource(long resourceId) throws Exception {
    PyResource resource = resourceStore.getResourceById(resourceId);
    if (null == resource) {
      throw new Exception();
    }
    return resource;
  }


  public PyResource getResourceById(long resourceId) throws Exception {
    PyResource resource = resourceStore.getResourceById(resourceId);
    return resource;
  }


  public void saveResource(PyResource resource) {
    logger.warn("saveResource :{}", resource);
    resourceStore.saveResource(resource);
  }


  public void updateResourceName(long resourceId, String resourceName) throws Exception {
    PyResource oldResource = resourceStore.getResourceById(resourceId);
    if (oldResource == null) {
      throw new Exception();
    }
    oldResource.setResourceName(resourceName);
    logger.warn("updateResourceName :{}", oldResource);
    resourceStore.saveResource(oldResource);
  }


  public void unbindResource(long unbindingResourceId) throws ResourceNotExistsExceptionThrift {
    PyResource unbindingResource = resourceStore.getResourceById(unbindingResourceId);
    logger.warn("unbindingResource is: {}", unbindingResource);
    if (null == unbindingResource) {
      throw new ResourceNotExistsExceptionThrift()
          .setDetail("Cannot find resource with id: " + unbindingResource);

    }
    List<AccountMetadata> modifiedAccounts = new ArrayList<>();
    for (AccountMetadata account : accountStore.listAccounts()) {
      Set<PyResource> oldResources = account.getResources();
      Iterator<PyResource> iterator = oldResources.iterator();
      while (iterator.hasNext()) {
        PyResource pyResource = iterator.next();
        if (pyResource.getResourceId() == unbindingResourceId) {
          logger.warn("account is: {}, removed resource is: {}", account, pyResource);
          iterator.remove();
          modifiedAccounts.add(account);
        }
      }
    }
    for (AccountMetadata account : modifiedAccounts) {
      accountStore.updateAccount(account);
    }
  }


  public void removeResource(long removingResource) {
    resourceStore.deleteResourceById(removingResource);
  }


  public List<PyResource> listResources() {
    return resourceStore.listResources();
  }


  public List<ApiToAuthorize> listApis() {
    return apiStore.listApis();
  }


  public List<Role> listRoles() {
    return roleStore.listRoles();
  }


  public void setRoleStore(RoleStore roleStore) {
    this.roleStore = roleStore;
  }


  public void setApiStore(ApiStore apiStore) {
    this.apiStore = apiStore;
  }


  public void setAccountStore(AccountStore accountStore) {
    this.accountStore = accountStore;
  }


  public ResourceStore getResourceStore() {
    return resourceStore;
  }


  public void setResourceStore(ResourceStore resourceStore) {
    this.resourceStore = resourceStore;
  }


  public Set<Role> getBuiltInRoles() {
    return builtInRoles;
  }


  public void setBuiltInRoles(Set<Role> builtInRoles) {
    this.builtInRoles = builtInRoles;
  }
}
