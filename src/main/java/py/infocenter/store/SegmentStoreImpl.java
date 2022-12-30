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
import py.icshare.SegmentId;
import py.icshare.SegmentUnitInformation;


@Transactional
public class SegmentStoreImpl implements SegmentStore {

  SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(SegmentUnitInformation segmentInformation) {
    sessionFactory.getCurrentSession().update(segmentInformation);
  }

  @Override
  public void save(SegmentUnitInformation segmentInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(segmentInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<SegmentUnitInformation> getByVolumeId(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from SegmentUnitInformation where segmentId.volumeId = :id");
    query.setLong("id", volumeId);
    return query.list();
  }

  @Override
  public SegmentUnitInformation getBySegmentId(SegmentId segmentId) {
    return (SegmentUnitInformation) sessionFactory.getCurrentSession()
        .get(SegmentUnitInformation.class, segmentId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<SegmentUnitInformation> list() {
    return sessionFactory.getCurrentSession().createQuery("from SegmentUnitInformation").list();
  }

  @Override
  public int deleteByVolumeId(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete SegmentUnitInformation where segmentId.volumeId = :id");
    query.setLong("id", volumeId);
    return query.executeUpdate();
  }

  @Override
  public int delete(SegmentId segmentId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete SegmentUnitInformation where segmentId = :id");
    query.setParameter("id", segmentId);


    return query.executeUpdate();
  }

}
