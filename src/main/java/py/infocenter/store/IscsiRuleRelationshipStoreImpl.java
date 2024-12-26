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
import py.icshare.DriverKey;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;


@Transactional
public class IscsiRuleRelationshipStoreImpl implements IscsiRuleRelationshipStore {

  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(IscsiRuleRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().update(relationshipInformation);
  }

  @Override
  public void save(IscsiRuleRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(relationshipInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IscsiRuleRelationshipInformation> getByDriverKey(DriverKey driverKey) {

    Query query = sessionFactory.getCurrentSession().createQuery(
        "from IscsiRuleRelationshipInformation where driverContainerId = :did and volumeId = :vid"
            + " and snapshotId = :sid and driverType = :type");
    query.setLong("did", driverKey.getDriverContainerId());
    query.setLong("vid", driverKey.getVolumeId());
    query.setInteger("sid", driverKey.getSnapshotId());
    query.setString("type", driverKey.getDriverType().name());

    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IscsiRuleRelationshipInformation> getByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from IscsiRuleRelationshipInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IscsiRuleRelationshipInformation> list() {
    return sessionFactory.getCurrentSession().createQuery("from IscsiRuleRelationshipInformation")
        .list();
  }

  @Override
  public int deleteByDriverKey(DriverKey driverKey) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete IscsiRuleRelationshipInformation where driverContainerId = :did and volumeId = "
            + ":vid and snapshotId = :sid and driverType = :type");

    query.setLong("did", driverKey.getDriverContainerId());
    query.setLong("vid", driverKey.getVolumeId());
    query.setInteger("sid", driverKey.getSnapshotId());
    query.setString("type", driverKey.getDriverType().name());

    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete IscsiRuleRelationshipInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleIdandDriverKey(DriverKey driverKey, long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete IscsiRuleRelationshipInformation where ruleId = :id and driverContainerId = :did "
            + "and volumeId = :vid and snapshotId = :sid and driverType = :type");
    query.setLong("id", ruleId);
    query.setLong("did", driverKey.getDriverContainerId());
    query.setLong("vid", driverKey.getVolumeId());
    query.setInteger("sid", driverKey.getSnapshotId());
    query.setString("type", driverKey.getDriverType().name());
    return query.executeUpdate();
  }
}
