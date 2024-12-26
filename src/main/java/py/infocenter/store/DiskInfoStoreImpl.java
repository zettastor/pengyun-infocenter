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

import java.sql.Blob;
import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.DiskInfo;


@Transactional
public class DiskInfoStoreImpl implements DiskInfoStore {

  private static final Logger logger = LoggerFactory.getLogger(DiskInfoStore.class);
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void clearDb() {
    Query query = sessionFactory.getCurrentSession().createQuery("delete DiskInfo where 1=1 ");
    query.executeUpdate();
  }

  @Override
  public List<DiskInfo> listDiskInfos() {
    List<DiskInfo> diskInfoList = sessionFactory.getCurrentSession().createQuery("from DiskInfo")
        .list();
    return diskInfoList;
  }

  @Override
  public DiskInfo listDiskInfoById(String id) {
    DiskInfo diskInfo = sessionFactory.getCurrentSession().get(DiskInfo.class, id);
    return diskInfo;
  }

  @Override
  public void updateDiskInfoLightStatusById(String id, String status) {
    DiskInfo diskInfo = sessionFactory.getCurrentSession().get(DiskInfo.class, id);
    diskInfo.setSwith(status);
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return this.sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }
}
