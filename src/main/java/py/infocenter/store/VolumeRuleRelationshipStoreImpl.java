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
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.VolumeRuleRelationshipInformation;


@Transactional
public class VolumeRuleRelationshipStoreImpl implements VolumeRuleRelationshipStore {

  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(VolumeRuleRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().update(relationshipInformation);
  }

  @Override
  public void save(VolumeRuleRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(relationshipInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<VolumeRuleRelationshipInformation> getByVolumeId(long volumeId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from VolumeRuleRelationshipInformation where volumeId = :id");
    query.setParameter("id", volumeId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<VolumeRuleRelationshipInformation> getByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from VolumeRuleRelationshipInformation where ruleId = :id");
    query.setParameter("id", ruleId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<VolumeRuleRelationshipInformation> list() {
    return sessionFactory.getCurrentSession().createQuery("from VolumeRuleRelationshipInformation")
        .list();
  }

  @Override
  public int deleteByVolumeId(long volumeId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete VolumeRuleRelationshipInformation where volumeId = :id");
    query.setParameter("id", volumeId);
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete VolumeRuleRelationshipInformation where ruleId = :id");
    query.setParameter("id", ruleId);
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleIdandVolumeId(long volumeId, long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete VolumeRuleRelationshipInformation where ruleId = :id and volumeId = :vid");
    query.setParameter("id", ruleId);
    query.setParameter("vid", volumeId);
    return query.executeUpdate();
  }
}
