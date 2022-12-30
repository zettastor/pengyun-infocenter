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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.DriverKeyForScsi;
import py.icshare.ScsiDriverMetadata;


@Transactional
public class ScsiDriverStoreImpl implements ScsiDriverStore {

  private static final Logger logger = LoggerFactory.getLogger(ScsiDriverStoreImpl.class);

  private SessionFactory sessionFactory;

  private Map<DriverKeyForScsi, ScsiDriverMetadata> driverMap =
      new ConcurrentHashMap<DriverKeyForScsi, ScsiDriverMetadata>();

  private static DriverKeyForScsi getKey(long drivercontainerId, long volumeId, int snapshotId) {
    return new DriverKeyForScsi(drivercontainerId, volumeId, snapshotId);
  }

  private static DriverKeyForScsi getKey(DriverKeyForScsi driverKeyForScsi) {
    return new DriverKeyForScsi(driverKeyForScsi.getDrivercontainerId(),
        driverKeyForScsi.getVolumeId(), driverKeyForScsi.getSnapshotId());
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public List<ScsiDriverMetadata> get(long volumeId) {

    List<ScsiDriverMetadata> driverMetadatas = new ArrayList<>();

    // get driver from memory firstly if exist
    for (ScsiDriverMetadata scsiDriverMetadata : driverMap.values()) {
      if (scsiDriverMetadata.getDriverKeyForScsi().getVolumeId() == volumeId) {
        driverMetadatas.add(scsiDriverMetadata);
      }
    }

    if (driverMetadatas.size() == 0) {
      // get driver metadata from database
      List<ScsiDriverMetadata> driverMetadataFromDb = getByVolumeIdFromDb(volumeId);
      if (driverMetadataFromDb != null && driverMetadataFromDb.size() > 0) {
        for (ScsiDriverMetadata driverInformation : driverMetadataFromDb) {
          if (driverInformation == null) {
            continue;
          }

          // put driver metadata in memory
          driverMap.put(getKey(driverInformation.getDriverKeyForScsi()), driverInformation);
          // return the driver metadata
          driverMetadatas.add(driverInformation);
        }
      }
    }

    return driverMetadatas;
  }

  @Override
  public List<ScsiDriverMetadata> get(long volumeId, int snapshotId) {
    return null;
  }

  @Override
  public ScsiDriverMetadata get(DriverKeyForScsi driverKeyForScsi) {
    ScsiDriverMetadata scsiDriverMetadata = driverMap.get(driverKeyForScsi);

    // request driver doesn't exist in memory and get from db
    if (scsiDriverMetadata == null) {
      List<ScsiDriverMetadata> driverInformations = getByDriverKeyFromDb(
          driverKeyForScsi.getDrivercontainerId(),
          driverKeyForScsi.getVolumeId(), driverKeyForScsi.getSnapshotId());
      // record corresponding driver key is 1 vs 1
      if (driverInformations != null && driverInformations.size() == 1) {
        driverMap.put(getKey(driverKeyForScsi), driverInformations.get(0));
      }
    }

    return scsiDriverMetadata;
  }

  @Override
  public List<ScsiDriverMetadata> list() {
    List<ScsiDriverMetadata> driverMetadatas = new ArrayList<>(driverMap.values());

    // if memory is empty, get driver meta data from database
    if (driverMetadatas.size() == 0) {
      List<ScsiDriverMetadata> driverMetadataFromDb = listFromDb();
      if (driverMetadataFromDb != null && driverMetadataFromDb.size() > 0) {
        for (ScsiDriverMetadata driverInformation : driverMetadataFromDb) {
          // put driver meta data in memory
          driverMap.put(getKey(driverInformation.getDriverKeyForScsi()), driverInformation);
          // return the driver metadata
          driverMetadatas.add(driverInformation);
        }
      }
    }

    return driverMetadatas;
  }

  @Override
  public List<ScsiDriverMetadata> getByDriverContainerId(long drivercontainerId) {
    List<ScsiDriverMetadata> scsiDriverMetadataList = new ArrayList<>();

    // get driver from memory firstly if exist
    for (ScsiDriverMetadata scsiDriverMetadata : driverMap.values()) {
      if (drivercontainerId == scsiDriverMetadata.getDriverKeyForScsi().getDrivercontainerId()) {
        scsiDriverMetadataList.add(scsiDriverMetadata);
      }
    }

    /*
     * driver with specified driver container id doesn't exist in memory,
     * get it from database firstly and put it in memory at the same time
     */
    if (scsiDriverMetadataList.size() == 0) {
      // get driver metadata from database
      List<ScsiDriverMetadata> scsiDriverMetadataFromDb = getByDriverContainerIdFromDb(
          drivercontainerId);
      if (scsiDriverMetadataFromDb != null && scsiDriverMetadataFromDb.size() > 0) {
        for (ScsiDriverMetadata scsiDriverMetadata : scsiDriverMetadataFromDb) {
          if (scsiDriverMetadata == null) {
            continue;
          }

          // put driver metadata in memory
          driverMap.put(getKey(scsiDriverMetadata.getDriverKeyForScsi()), scsiDriverMetadata);
          // return the driver metadata
          scsiDriverMetadataList.add(scsiDriverMetadata);
        }
      }
    }

    return scsiDriverMetadataList;
  }

  @Override
  public void delete(long volumeId) {
    // delete from memory
    for (ScsiDriverMetadata scsiDriverMetadata : get(volumeId)) {
      driverMap.remove(getKey(scsiDriverMetadata.getDriverKeyForScsi()));
    }

    // delete from DB
    deleteFromDb(volumeId);
  }

  @Override
  public void delete(long drivercontainerId, long volumeId, int snapshotId) {
    logger.warn("SCSIDriverStore before delete: {}", list());
    // delete from memory
    driverMap.remove(getKey(drivercontainerId, volumeId, snapshotId));

    // delete from db
    deleteFromDb(drivercontainerId, volumeId, snapshotId);
    logger.warn("SCSIDriverStore after delete: {}", list());
  }

  @Override
  public void delete(long volumeId, int snapshotId) {

  }

  @Override
  public void save(ScsiDriverMetadata scsiDriverMetadata) {
    driverMap.put(getKey(scsiDriverMetadata.getDriverKeyForScsi()), scsiDriverMetadata);

    // save to db
    saveToDb(scsiDriverMetadata);
  }

  @Override
  public void clearMemoryData() {
    driverMap.clear();
  }


  
  @SuppressWarnings("unchecked")
  public List<ScsiDriverMetadata> getByVolumeIdFromDb(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from ScsiDriverMetadata where driverKeyForScsi.volumeId = :id");
    query.setParameter("id", volumeId);
    return (List<ScsiDriverMetadata>) query.list();
  }

  @Override
  public List<ScsiDriverMetadata> getByDriverContainerIdFromDb(long drivercontainerId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from ScsiDriverMetadata where driverKeyForScsi.drivercontainerId = :id");
    query.setParameter("id", drivercontainerId);
    return (List<ScsiDriverMetadata>) query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<ScsiDriverMetadata> getByDriverKeyFromDb(long drivercontainerId, long volumeId,
      int snapshotId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from ScsiDriverMetadata where driverKeyForScsi.volumeId = :id and driverKeyForScsi"
            + ".drivercontainerId = :drivercontainerId and driverKeyForScsi.snapshotId = :sid");
    query.setParameter("id", volumeId);
    query.setParameter("drivercontainerId", drivercontainerId);
    query.setParameter("sid", snapshotId);

    return (List<ScsiDriverMetadata>) query.list();
  }

  @SuppressWarnings("unchecked")
  public List<ScsiDriverMetadata> listFromDb() {
    return sessionFactory.getCurrentSession().createQuery("from ScsiDriverMetadata").list();
  }


  
  public int deleteFromDb(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete ScsiDriverMetadata where driverKeyForScsi.volumeId = :id");
    query.setParameter("id", volumeId);
    return query.executeUpdate();
  }

  @Override
  public int deleteFromDb(long drivercontainerId, long volumeId, int snapshotId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete ScsiDriverMetadata where driverKeyForScsi.volumeId = :id and driverKeyForScsi"
            + ".drivercontainerId = :drivercontainerId"

            + " and driverKeyForScsi.snapshotId = :sid");
    query.setParameter("id", volumeId);
    query.setParameter("drivercontainerId", drivercontainerId);
    query.setParameter("sid", snapshotId);
    return query.executeUpdate();
  }

  public void saveToDb(ScsiDriverMetadata scsiDriverMetadata) {
    sessionFactory.getCurrentSession().saveOrUpdate(scsiDriverMetadata);
  }
}
