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
