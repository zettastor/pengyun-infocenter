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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Test;
import py.icshare.AccountMetadata;
import py.icshare.authorization.InMemoryAccountStoreImpl;
import py.test.TestBase;


public class InMemoryAccountStoreTest extends TestBase {

  InMemoryAccountStoreImpl inMemoryAccountStore;

  @Before
  public void init() throws Exception {
    super.init();
    inMemoryAccountStore = new InMemoryAccountStoreImpl();
  }

  @Test
  public void testCreateAccount() throws Exception {
    String accountName = "abc";
    String passwd = "aaaa";
    String accountType = "Admin";
    Long accountId = 1L;

    AccountMetadata account = inMemoryAccountStore
        .createAccount(accountName, passwd, accountType, accountId,
            new HashSet<>());

    AccountMetadata accountByName = inMemoryAccountStore.getAccount(accountName);
    assertEquals(account, accountByName);

    AccountMetadata accountById = inMemoryAccountStore.getAccountById(accountId);
    assertEquals(account, accountById);

    String accountName2 = "aaaa";
    Long accountId2 = 2L;
    final AccountMetadata account2 = inMemoryAccountStore
        .createAccount(accountName2, passwd, accountType, accountId2,
            new HashSet<>());

    Collection<AccountMetadata> accounts = inMemoryAccountStore.listAccounts();
    assertEquals(2, accounts.size());
    Iterator<AccountMetadata> iterator = accounts.iterator();
    assertEquals(accountId, iterator.next().getAccountId());
    assertEquals(accountId2, iterator.next().getAccountId());

    account2.setAccountName("new account");
    assertNull(inMemoryAccountStore.updateAccount(account2));

    account2.setAccountName("aaaa");
    account2.setHashedPassword("newpassword");
    assertNotNull(inMemoryAccountStore.updateAccount(account2));
    assertEquals("newpassword",
        inMemoryAccountStore.getAccountById(accountId2).getHashedPassword());

    accounts = inMemoryAccountStore.listAccounts();
    assertEquals(2, accounts.size());

    inMemoryAccountStore.deleteAccount(accountId2);
    assertNull(inMemoryAccountStore.getAccountById(accountId2));
  }
}
