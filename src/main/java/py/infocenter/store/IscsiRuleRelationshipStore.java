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

package py.infocenter.store;

import java.util.List;
import py.icshare.DriverKey;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;


public interface IscsiRuleRelationshipStore {

  public void update(IscsiRuleRelationshipInformation iscsiRelationshipInformation);

  public void save(IscsiRuleRelationshipInformation iscsiRelationshipInformation);

  public List<IscsiRuleRelationshipInformation> getByDriverKey(DriverKey driverKey);

  public List<IscsiRuleRelationshipInformation> getByRuleId(long ruleId);

  public List<IscsiRuleRelationshipInformation> list();

  public int deleteByDriverKey(DriverKey driverKey);

  public int deleteByRuleId(long ruleId);

  public int deleteByRuleIdandDriverKey(DriverKey driverKey, long ruleId);
}
