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

package py.infocenter.store;

import java.util.List;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;



@Transactional
public class IscsiAccessRuleStoreImpl implements IscsiAccessRuleStore {

  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(IscsiAccessRuleInformation accessRuleInformation) {
    sessionFactory.getCurrentSession().update(accessRuleInformation);
  }

  @Override
  public void save(IscsiAccessRuleInformation accessRuleInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(accessRuleInformation);
  }

  @Override
  public IscsiAccessRuleInformation get(long ruleId) {
    return (IscsiAccessRuleInformation) sessionFactory.getCurrentSession()
        .get(IscsiAccessRuleInformation.class, ruleId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IscsiAccessRuleInformation> list() {
    return sessionFactory.getCurrentSession().createQuery("from IscsiAccessRuleInformation").list();
  }

  @Override
  public int delete(long ruleId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete IscsiAccessRuleInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }
}
