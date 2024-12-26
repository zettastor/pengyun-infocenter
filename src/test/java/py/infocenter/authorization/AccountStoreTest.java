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

import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.icshare.AccountMetadata;
import py.icshare.AccountMetadata.AccountType;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.Role;
import py.thrift.share.AccountTypeThrift;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class AccountStoreTest {

  private static final Logger logger = LoggerFactory.getLogger(AccountStoreTest.class);

  @Autowired
  @Qualifier("memory")
  AccountStore accountStore;

  @Before
  public void init() {
    accountStore.deleteAllAccounts();
  }

  /**
   * Test deleteAccount api. Test step: 1. create a account; 2. delete the account.
   */
  @Test
  public void testDeleteAccount() {
    accountStore.createAccount("DelAcc", "123", AccountTypeThrift.Admin.name(),
        889L, new HashSet<Role>());
    accountStore.deleteAccount(889L);
    Collection<AccountMetadata> accList = accountStore.listAccounts();
    assertTrue(accList.isEmpty());
  }

  /**
   * Test authenticateAccount api. Test step: 1. create a account; 2. authenticate the account; 3.
   * delete the account.
   */
  @Test
  public void testAuthenticateAccount() {
    accountStore.createAccount("AutAcc", "123",
        AccountTypeThrift.Admin.name(), 889L, new HashSet<Role>());
    accountStore.authenticateAccount("AutAcc", "123");
    accountStore.deleteAccount(889L);
  }

  /**
   * Test deleteAccount api. Test step: 1. create a account; 2. update the account; 3. delete the
   * account.
   */
  @Test
  public void testUpdatePassword() {
    accountStore.createAccount("UpAcc", "123",
        AccountType.Admin.name(), 889L, new HashSet<>());
    AccountMetadata oldAccount = accountStore
        .authenticateAccount("UpAcc", "123");
    AccountMetadata newAccount = new AccountMetadata("UpAcc", "312",
        AccountType.Admin.name(), 889L);
    oldAccount.setSalt(newAccount.getSalt());
    oldAccount.setHashedPassword(newAccount.getHashedPassword());
    accountStore.updateAccount(oldAccount);
    accountStore.authenticateAccount("UpAcc", "312");
    accountStore.deleteAccount(889L);
  }

  /**
   * Test save an account with role not exists.
   *
   * @throws org.springframework.dao.DataIntegrityViolationException because the role is not existed
   *                                                                 in database before save the
   *                                                                 account, save account will
   *                                                                 violate the foreign key
   *                                                                 relation.
   */
  @Test(expected = org.springframework.dao.DataIntegrityViolationException.class)
  public void testSaveAccountWithRoleNotExists() {
    Set<Role> roles = new HashSet<>();
    roles.add(new Role(1L, "NotExists"));
    accountStore.createAccount("test", "312", AccountType.SuperAdmin.name(),
        1L, roles);
  }

  @After
  public void clean() {
    accountStore.deleteAllAccounts();
  }
}