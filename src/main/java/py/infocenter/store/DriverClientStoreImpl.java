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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.driver.DriverType;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKey;
import py.icshare.DriverClientKeyInformation;


@Transactional
public class DriverClientStoreImpl implements DriverClientDbStore, DriverClientStore {

  private static final Logger logger = LoggerFactory.getLogger(DriverClientStoreImpl.class);

  private SessionFactory sessionFactory;

  private Multimap<DriverClientKey, DriverClientInformation> driverMap = Multimaps
      .synchronizedSetMultimap(HashMultimap.<DriverClientKey, DriverClientInformation>create());

  private static DriverClientKey getKey(DriverClientInformation driverClientInformation) {
    return new DriverClientKey(driverClientInformation.getDriverClientKeyInformation());
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DriverClientInformation> getByVolumeIdFromDb(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery(
            "from DriverClientInformation where driverClientKeyInformation.volumeId = :id");
    query.setParameter("id", volumeId);
    return (List<DriverClientInformation>) query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DriverClientInformation> getByDriverKeyFromDb(DriverClientKey driverClientKey) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from DriverClientInformation where driverClientKeyInformation.volumeId = :id and "
            + "driverClientKeyInformation.driverType = :driverType "

            + "and driverClientKeyInformation.snapshotId = :sid and driverClientKeyInformation"
            + ".driverContainerId = :did and driverClientKeyInformation.clientInfo = :clientInfo");
    query.setParameter("id", driverClientKey.getVolumeId());
    query.setParameter("driverType", driverClientKey.getDriverType().name());
    query.setParameter("sid", driverClientKey.getSnapshotId());
    query.setParameter("did", driverClientKey.getDriverContainerId());
    query.setParameter("clientInfo", driverClientKey.getClientInfo());
    return (List<DriverClientInformation>) query.list();
  }

  @Override
  public List<DriverClientInformation> getByDriverContainerIdFromDb(long driverContainerId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from DriverClientInformation where driverClientKeyInformation.driverContainerId = :id");
    query.setParameter("id", driverContainerId);

    return (List<DriverClientInformation>) query.list();
  }

  @Override
  public void saveToDb(DriverClientInformation driverClientInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(driverClientInformation);
  }

  @Override
  public void updateToDb(DriverClientInformation driverClientInformation) {
    sessionFactory.getCurrentSession().update(driverClientInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DriverClientInformation> listFromDb() {
    return sessionFactory.getCurrentSession().createQuery("from DriverClientInformation").list();
  }

  @Override
  public int deleteFromDb(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery(
            "delete DriverClientInformation where driverClientKeyInformation.volumeId = :id");
    query.setParameter("id", volumeId);
    return query.executeUpdate();
  }

  @Override
  public int deleteFromDb(long volumeId, DriverType driverType, int snapshotId,
      long driverContainerId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete DriverClientInformation where driverClientKeyInformation.volumeId = :id and "
            + "driverClientKeyInformation.driverType = :driverType "

            + "and driverClientKeyInformation.snapshotId = :sid and driverClientKeyInformation"
            + ".driverContainerId = :did");
    query.setParameter("id", volumeId);
    query.setParameter("driverType", driverType.name());
    query.setParameter("sid", snapshotId);
    query.setParameter("did", driverContainerId);
    return query.executeUpdate();
  }

  @Override
  public int deleteFromDb(DriverClientKey driverClientKey) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete DriverClientInformation where driverClientKeyInformation.volumeId = :id and "
            + "driverClientKeyInformation.driverType = :driverType "

            + "and driverClientKeyInformation.snapshotId = :sid and driverClientKeyInformation"
            + ".driverContainerId = :did and driverClientKeyInformation.clientInfo = :clientInfo");
    query.setParameter("id", driverClientKey.getVolumeId());
    query.setParameter("driverType", driverClientKey.getDriverType().name());
    query.setParameter("sid", driverClientKey.getSnapshotId());
    query.setParameter("did", driverClientKey.getDriverContainerId());
    query.setParameter("clientInfo", driverClientKey.getClientInfo());
    return query.executeUpdate();
  }

  @Override
  public int deleteFromDb(DriverClientInformation driverClientInformation) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete DriverClientInformation where driverClientKeyInformation.volumeId = :id and "
            + "driverClientKeyInformation.driverType = :driverType "

            + "and driverClientKeyInformation.snapshotId = :sid and driverClientKeyInformation"
            + ".driverContainerId = :did "

            + "and driverClientKeyInformation.clientInfo = :clientInfo and "
            + "driverClientKeyInformation.time = :time");

    DriverClientKeyInformation driverClientKeyInformation = driverClientInformation
        .getDriverClientKeyInformation();
    query.setParameter("id", driverClientKeyInformation.getVolumeId());
    query.setParameter("driverType", driverClientKeyInformation.getDriverType());
    query.setParameter("sid", driverClientKeyInformation.getSnapshotId());
    query.setParameter("did", driverClientKeyInformation.getDriverContainerId());
    query.setParameter("clientInfo", driverClientKeyInformation.getClientInfo());
    query.setParameter("time", driverClientKeyInformation.getTime());
    return query.executeUpdate();
  }

  @Override
  public DriverClientInformation getLastTimeValue(DriverClientKey driverClientKey) {
    Collection<DriverClientInformation> driverClientInformations = driverMap.get(driverClientKey);
    List<DriverClientInformation> driverClientInformationList = new LinkedList<>();

    // request driver doesn't exist in memory and get from db
    if (driverClientInformations == null || driverClientInformations.isEmpty()) {
      driverClientInformationList = getByDriverKeyFromDb(driverClientKey);
      for (DriverClientInformation driverClientInformation : driverClientInformationList) {
        driverMap.put(driverClientKey, driverClientInformation);
      }
    } else {
      driverClientInformationList = new LinkedList<>(driverClientInformations);
    }

    if (driverClientInformations != null && !driverClientInformations.isEmpty()) {
      driverClientInformationList.sort(new Comparator<DriverClientInformation>() {
        @Override
        public int compare(DriverClientInformation o1, DriverClientInformation o2) {
          return (int) (o2.getDriverClientKeyInformation().getTime() - o1
              .getDriverClientKeyInformation().getTime());
        }
      });

      return driverClientInformationList.get(0);
    }

    return null;
  }

  @Override
  public List<DriverClientInformation> list() {
    List<DriverClientInformation> driverClientInformationArrayList = new ArrayList<>(
        driverMap.values());

    // if memory is empty, get driver meta data from database
    if (driverClientInformationArrayList.size() == 0) {
      driverClientInformationArrayList = listFromDb();
      if (driverClientInformationArrayList != null && driverClientInformationArrayList.size() > 0) {
        for (DriverClientInformation driverInformation : driverClientInformationArrayList) {
          // put driver meta data in memory
          driverMap.put(getKey(driverInformation), driverInformation);
        }
      }
    }

    return driverClientInformationArrayList;
  }

  @Override
  public Multimap<DriverClientKey, DriverClientInformation> listDriverKey() {
    Multimap<DriverClientKey, DriverClientInformation> multimap = LinkedListMultimap.create();
    Set<DriverClientKey> driverClientKeys = new HashSet<>(driverMap.keySet());
    for (DriverClientKey driverClientKey : driverClientKeys) {
      Collection<DriverClientInformation> driverClientInformations = driverMap.get(driverClientKey);
      if (driverClientInformations != null && !driverClientInformations.isEmpty()) {
        multimap.putAll(driverClientKey, driverClientInformations);
      }
    }
    return multimap;
  }

  @Override
  public void loadToMemory() {
    List<DriverClientInformation> driverMetadataFromDb = listFromDb();
    if (driverMetadataFromDb != null && driverMetadataFromDb.size() > 0) {
      for (DriverClientInformation driverInformation : driverMetadataFromDb) {
        // put driver meta data in memory
        driverMap.put(getKey(driverInformation), driverInformation);
      }
    }
  }

  @Override
  public void delete(DriverClientKey driverClientKey) {
    // delete from memory
    driverMap.removeAll(driverClientKey);

    // delete from db
    deleteFromDb(driverClientKey);
  }

  @Override
  public void deleteValue(DriverClientInformation driverClientInformation) {
    DriverClientKey driverClientKey = getKey(driverClientInformation);
    Collection<DriverClientInformation> driverClientInformations = driverMap.get(driverClientKey);
    driverClientInformations.remove(driverClientInformation);

    deleteFromDb(driverClientInformation);
  }

  @Override
  public void save(DriverClientInformation driverClientInformation) {
    // save to mem
    driverMap.put(getKey(driverClientInformation), driverClientInformation);
    // save to db
    saveToDb(driverClientInformation);
  }

  public void clearMemoryData() {
    driverMap.clear();
  }
}
