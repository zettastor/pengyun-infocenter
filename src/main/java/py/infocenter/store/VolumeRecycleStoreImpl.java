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
