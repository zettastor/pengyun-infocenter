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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.VolumeDeleteDelayInformation;


@Transactional
public class VolumeDelayStoreImpl implements VolumeDelayStore {

  private static final Logger logger = LoggerFactory.getLogger(DbVolumeStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public int deleteVolumeDelayInfo(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete VolumeDeleteDelayInformation where volumeId = :volumeId");
    query.setLong("volumeId", volumeId);
    return query.executeUpdate();
  }

  @Override
  public void saveVolumeDelayInfo(VolumeDeleteDelayInformation volumeDeleteDelayInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(volumeDeleteDelayInformation);
  }

  @Override
  public List<VolumeDeleteDelayInformation> listVolumesDelayInfo() {
    return sessionFactory.getCurrentSession().createQuery("from VolumeDeleteDelayInformation")
        .list();
  }

  @Override
  public VolumeDeleteDelayInformation getVolumeDelayInfo(long volumeId) {
    return (VolumeDeleteDelayInformation) sessionFactory.getCurrentSession()
        .get(VolumeDeleteDelayInformation.class, volumeId);
  }

  @Override
  public long updateVolumeDelayTimeInfo(long volumeId, long timeForDelay) {
    org.hibernate.query.Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeDeleteDelayInformation set timeForDelay = :timeForDelay where volumeId = "
            + ":volumeId");
    query.setParameter("volumeId", volumeId);
    query.setParameter("timeForDelay", timeForDelay);
    query.executeUpdate();
    return timeForDelay;
  }

  @Override
  public boolean updateVolumeDelayStatusInfo(long volumeId, boolean stopDelay) {
    org.hibernate.query.Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeDeleteDelayInformation set stopDelay = :stopDelay where volumeId = "
            + ":volumeId");
    query.setParameter("volumeId", volumeId);
    query.setParameter("stopDelay", stopDelay);
    query.executeUpdate();
    return stopDelay;
  }

  @Override
  public void clearAll() {
    sessionFactory.getCurrentSession().createQuery("delete from VolumeDeleteDelayInformation")
        .executeUpdate();
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}
