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

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;



@Transactional
public class InstanceVolumesInformationStoreImpl implements InstanceVolumesInformationStore {

  private static final Logger logger = LoggerFactory
      .getLogger(InstanceVolumesInformationStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void saveInstanceVolumesInformationToDb(
      InstanceVolumesInformation instanceVolumesInformation) {

    logger.info("saveInstanceVolumesInformationToDB :{}", instanceVolumesInformation);

    InstanceVolumesInformationDb instanceVolumesInformationDb =
        instanceVolumesInformation.toInstanceVolumesInformationDb(this);

    if (instanceVolumesInformationDb == null) {
      logger.warn("Invalid param, please check them");
      return;
    }

    sessionFactory.getCurrentSession().saveOrUpdate(instanceVolumesInformationDb);
  }

  @Override
  public int deleteInstanceVolumesInformationFromDb(Long instanceId) {
    logger.info("deleteInstanceVolumesInformationFromDB id :{}", instanceId);
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete InstanceVolumesInformationDb where instanceId = :id");
    query.setLong("id", instanceId);
    return query.executeUpdate();
  }

  @Override
  public InstanceVolumesInformation getInstanceVolumesInformationFromDb(Long instanceId)
      throws SQLException, IOException {
    logger.info("getInstanceVolumesInformationFromDB id :{}", instanceId);
    InstanceVolumesInformationDb instanceVolumesInformationDb = sessionFactory
        .getCurrentSession().get(InstanceVolumesInformationDb.class, instanceId);

    return instanceVolumesInformationDb.toInstanceVolumesInformation();
  }

  @Override
  public List<InstanceVolumesInformation> reloadAllInstanceVolumesInformationFromDb()
      throws SQLException, IOException {
    logger.info("reloadAllInstanceVolumesInformationFromDB ");
    List<InstanceVolumesInformationDb> instanceVolumesInformationDbList = sessionFactory
        .getCurrentSession().createQuery("from InstanceVolumesInformationDb").list();

    List<InstanceVolumesInformation> instanceVolumesInformationList = new ArrayList<>();
    for (InstanceVolumesInformationDb value : instanceVolumesInformationDbList) {

      InstanceVolumesInformation instanceVolumesInformation = value.toInstanceVolumesInformation();
      if (instanceVolumesInformation != null) {
        instanceVolumesInformationList.add(instanceVolumesInformation);
      }
    }

    return instanceVolumesInformationList;
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return this.sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}
