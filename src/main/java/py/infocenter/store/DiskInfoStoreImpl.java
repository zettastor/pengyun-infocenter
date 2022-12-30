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
