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
import py.icshare.ArchiveInformation;


@Transactional
public class ArchiveStoreImpl implements ArchiveStore {

  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(ArchiveInformation archiveInformation) {
    sessionFactory.getCurrentSession().update(archiveInformation);
  }

  @Override
  public void save(ArchiveInformation archiveInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(archiveInformation);
  }

  public ArchiveInformation get(long archiveId) {
    return (ArchiveInformation) sessionFactory.getCurrentSession()
        .get(ArchiveInformation.class, archiveId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<ArchiveInformation> getByInstanceId(long instanceId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from ArchiveInformation where instanceId = :id");
    query.setLong("id", instanceId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<ArchiveInformation> list() {
    return sessionFactory.getCurrentSession().createQuery("from ArchiveInformation").list();
  }

  @Override
  public int deleteByInstanceId(long instanceId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete ArchiveInformation where instanceId = :id");
    query.setLong("id", instanceId);
    return query.executeUpdate();
  }

  @Override
  public int delete(long archiveId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete ArchiveInformation where archiveId = :id");
    query.setLong("id", archiveId);
    return query.executeUpdate();
  }

  @Override
  public void saveAll(List<ArchiveInformation> archiveInformations) {

  }

}
