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
