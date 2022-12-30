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
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.AccessRuleInformation;


@Transactional
public class AccessRuleStoreImpl implements AccessRuleStore {

  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(AccessRuleInformation accessRuleInformation) {
    sessionFactory.getCurrentSession().update(accessRuleInformation);
  }

  @Override
  public void save(AccessRuleInformation accessRuleInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(accessRuleInformation);
  }

  @Override
  public AccessRuleInformation get(long ruleId) {
    return (AccessRuleInformation) sessionFactory.getCurrentSession()
        .get(AccessRuleInformation.class, ruleId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<AccessRuleInformation> list() {
    return sessionFactory.getCurrentSession().createQuery("from AccessRuleInformation").list();
  }

  @Override
  public int delete(long ruleId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete AccessRuleInformation where ruleId = :id");
    query.setParameter("id", ruleId);
    return query.executeUpdate();
  }
}
