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
import py.icshare.VolumeRecycleInformation;


@Transactional
public class VolumeRecycleStoreImpl implements VolumeRecycleStore {

  private static final Logger logger = LoggerFactory.getLogger(DbVolumeStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public int deleteVolumeRecycleInfo(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete VolumeRecycleInformation where volumeId = :volumeId");
    query.setLong("volumeId", volumeId);
    return query.executeUpdate();
  }

  @Override
  public void saveVolumeRecycleInfo(VolumeRecycleInformation volumeRecycleInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(volumeRecycleInformation);
  }

  @Override
  public List<VolumeRecycleInformation> listVolumesRecycleInfo() {
    return sessionFactory.getCurrentSession().createQuery("from VolumeRecycleInformation").list();
  }

  @Override
  public VolumeRecycleInformation getVolumeRecycleInfo(Long volumeId) {
    return (VolumeRecycleInformation) sessionFactory.getCurrentSession()
        .get(VolumeRecycleInformation.class, volumeId);
  }

  @Override
  public void clearAll() {
    sessionFactory.getCurrentSession().createQuery("delete from VolumeRecycleInformation")
        .executeUpdate();
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}
