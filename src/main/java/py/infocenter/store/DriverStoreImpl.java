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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.driver.DriverMetadata;
import py.driver.DriverType;
import py.icshare.DriverInformation;
import py.icshare.DriverKey;
import py.icshare.DriverKeyInformation;
import py.io.qos.IoLimitation;
import py.thrift.share.AlreadyExistStaticLimitationExceptionThrift;
import py.thrift.share.DynamicIoLimitationTimeInterleavingExceptionThrift;


@Transactional
public class DriverStoreImpl implements DriverDbStore, DriverStore {

  private static final Logger logger = LoggerFactory.getLogger(DriverStoreImpl.class);

  private SessionFactory sessionFactory;

  private Map<DriverKey, DriverMetadata> driverMap = new ConcurrentHashMap<DriverKey,
      DriverMetadata>();

  private static DriverKey getKey(DriverMetadata driverMetadata) {
    return new DriverKey(driverMetadata.getDriverContainerId(), driverMetadata.getVolumeId(),
        driverMetadata.getSnapshotId(), driverMetadata.getDriverType());
  }

  private static DriverKey getKey(long driverContainerId, long volumeId, int snapshotId,
      DriverType driverType) {
    return new DriverKey(driverContainerId, volumeId, snapshotId, driverType);
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DriverInformation> getByVolumeIdFromDb(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from DriverInformation where driverKeyInfo.volumeId = :id");
    query.setParameter("id", volumeId);
    return (List<DriverInformation>) query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DriverInformation> getByDriverKeyFromDb(long volumeId, DriverType driverType,
      int snapshotId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from DriverInformation where driverKeyInfo.volumeId = :id and driverKeyInfo.driverType ="
            + " :driverType and driverKeyInfo.snapshotId = :sid");
    query.setParameter("id", volumeId);
    query.setParameter("driverType", driverType.name());
    query.setParameter("sid", snapshotId);

    return (List<DriverInformation>) query.list();
  }

  @Override
  public List<DriverInformation> getByDriverContainerIdFromDb(long driverContainerId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from DriverInformation where driverKeyInfo.driverContainerId = :id");
    query.setParameter("id", driverContainerId);

    return (List<DriverInformation>) query.list();
  }

  @Override
  public void saveToDb(DriverInformation driverInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(driverInformation);
  }

  @Override
  public int updateStatusToDb(long volumeId, DriverType driverType, int snapshotId, String status) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update DriverInformation set driverStatus = :status where driverKeyInfo.volumeId = :id "
            + "and driverKeyInfo.driverType = :driverType and driverKeyInfo.snapshotId = :sid");
    query.setParameter("id", volumeId);
    query.setParameter("driverType", driverType.name());
    query.setParameter("sid", snapshotId);
    query.setParameter("status", status);
    return query.executeUpdate();
  }

  @Override
  public int updateMakeUnmountDriverForCsiToDb(long driverContainerId, long volumeId,
      DriverType driverType, int snapshotId, boolean makeUnmountForCsi) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update DriverInformation set makeUnmountForCsi = :makeUnmountForCsi where driverKeyInfo"
            + ".volumeId = :id "

            + "and driverKeyInfo.driverType = :driverType and driverKeyInfo.snapshotId = :sid and "
            + "driverKeyInfo.driverContainerId = :driverContainerId");
    query.setParameter("id", volumeId);
    query.setParameter("driverType", driverType.name());
    query.setParameter("sid", snapshotId);
    query.setParameter("driverContainerId", driverContainerId);
    query.setParameter("makeUnmountForCsi", makeUnmountForCsi);
    return query.executeUpdate();
  }

  @Override
  public void updateToDb(DriverInformation driverInformation) {
    sessionFactory.getCurrentSession().update(driverInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<DriverInformation> listFromDb() {
    return sessionFactory.getCurrentSession().createQuery("from DriverInformation").list();
  }

  @Override
  public int deleteFromDb(long volumeId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete DriverInformation where driverKeyInfo.volumeId = :id");
    query.setParameter("id", volumeId);
    return query.executeUpdate();
  }

  @Override
  public int deleteFromDb(long volumeId, DriverType driverType, int snapshotId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete DriverInformation where driverKeyInfo.volumeId = :id and driverKeyInfo.driverType"
            + " = :driverType and driverKeyInfo.snapshotId = :sid");
    query.setParameter("id", volumeId);
    query.setParameter("driverType", driverType.name());
    query.setParameter("sid", snapshotId);
    return query.executeUpdate();
  }

  @Override
  public List<DriverMetadata> get(long volumeId) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    // get driver from memory firstly if exist
    for (DriverMetadata driverMetadata : driverMap.values()) {
      if (driverMetadata.getVolumeId() == volumeId) {
        driverMetadatas.add(driverMetadata);
      }
    }

    /*
     * driver binding to volume with specified id doesn't exist in memory,
     * get it from database firstly and put it in memory at the same time
     */
    if (driverMetadatas.size() == 0) {
      // get driver metadata from database
      List<DriverInformation> driverMetadataFromDb = getByVolumeIdFromDb(volumeId);
      if (driverMetadataFromDb != null && driverMetadataFromDb.size() > 0) {
        for (DriverInformation driverInformation : driverMetadataFromDb) {
          if (driverInformation == null) {
            continue;
          }

          DriverMetadata driverMetadata = driverInformation.toDriverMetadata();
          // put driver metadata in memory
          driverMap.put(getKey(driverMetadata), driverMetadata);
          // return the driver metadata
          driverMetadatas.add(driverMetadata);
        }
      }
    }

    return driverMetadatas;
  }

  @Override
  public DriverMetadata get(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId) {
    DriverMetadata driverMetadata = driverMap
        .get(getKey(driverContainerId, volumeId, snapshotId, driverType));

    // request driver doesn't exist in memory and get from db
    if (driverMetadata == null) {
      List<DriverInformation> driverInformations = getByDriverKeyFromDb(volumeId, driverType,
          snapshotId);
      // record corresponding driver key is 1 vs 1
      if (driverInformations != null && driverInformations.size() == 1) {
        driverMetadata = driverInformations.get(0).toDriverMetadata();
        driverMap.put(getKey(driverMetadata), driverMetadata);
      }
    }

    return driverMetadata;
  }

  @Override
  public List<DriverMetadata> get(long volumeId, int snapshotId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<DriverMetadata> getByDriverContainerId(long driverContainerId) {
    List<DriverMetadata> driverMetadatas = new ArrayList<>();

    // get driver from memory firstly if exist
    for (DriverMetadata driverMetadata : driverMap.values()) {
      if (driverMetadata.getDriverContainerId() == driverContainerId) {
        driverMetadatas.add(driverMetadata);
      }
    }

    /*
     * driver with specified driver container id doesn't exist in memory,
     * get it from database firstly and put it in memory at the same time
     */
    if (driverMetadatas.size() == 0) {
      // get driver metadata from database
      List<DriverInformation> driverMetadataFromDb = getByDriverContainerIdFromDb(
          driverContainerId);
      if (driverMetadataFromDb != null && driverMetadataFromDb.size() > 0) {
        for (DriverInformation driverInformation : driverMetadataFromDb) {
          if (driverInformation == null) {
            continue;
          }

          DriverMetadata driverMetadata = driverInformation.toDriverMetadata();
          // put driver metadata in memory
          driverMap.put(getKey(driverMetadata), driverMetadata);
          // return the driver metadata
          driverMetadatas.add(driverMetadata);
        }
      }
    }

    return driverMetadatas;
  }

  @Override
  public List<DriverMetadata> list() {
    List<DriverMetadata> driverMetadatas = new ArrayList<>(driverMap.values());

    // if memory is empty, get driver meta data from database
    if (driverMetadatas.size() == 0) {
      List<DriverInformation> driverMetadataFromDb = listFromDb();
      if (driverMetadataFromDb != null && driverMetadataFromDb.size() > 0) {
        for (DriverInformation driverInformation : driverMetadataFromDb) {
          DriverMetadata driverMetadata = driverInformation.toDriverMetadata();
          // put driver meta data in memory
          driverMap.put(getKey(driverMetadata), driverMetadata);
          // return the driver metadata
          driverMetadatas.add(driverMetadata);
        }
      }

    }

    return driverMetadatas;
  }

  private Blob createBlob(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return this.sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }

  @Override
  public void delete(long volumeId) {
    // delete from memory
    for (DriverMetadata driverMetadata : get(volumeId)) {
      driverMap.remove(getKey(driverMetadata));
    }

    // delete from DB
    deleteFromDb(volumeId);
  }

  @Override
  public void delete(long driverContainerId, long volumeId, DriverType driverType, int snapshotId) {
    logger.warn("driverStore before delete: {}", list(), new Throwable());
    // delete from memory
    driverMap.remove(getKey(driverContainerId, volumeId, snapshotId, driverType));

    // delete from db
    deleteFromDb(volumeId, driverType, snapshotId);
    logger.warn("driverStore after delete: {}", list(), new Throwable());
  }

  @Override

  public void save(DriverMetadata driverMetadata) {
    // save to mem
    driverMap.put(getKey(driverMetadata), driverMetadata);

    // save to db
    saveToDb(buildDriverInformation(driverMetadata));
  }

  private DriverInformation buildDriverInformation(DriverMetadata driverMetadata) {

    DriverInformation driverInformation = new DriverInformation();
    DriverKeyInformation driverKeyInformation = new DriverKeyInformation(
        driverMetadata.getDriverContainerId(),
        driverMetadata.getVolumeId(), driverMetadata.getSnapshotId(),
        driverMetadata.getDriverType().name());


    driverInformation.setHostName(driverMetadata.getHostName());
    driverInformation.setPortalType(driverMetadata.getPortalType().name());
    driverInformation.setIpv6Addr(driverMetadata.getIpv6Addr());
    driverInformation.setNetIfaceName(driverMetadata.getNicName());
    driverInformation.setDriverName(driverMetadata.getDriverName());
    driverInformation.setDriverKeyInfo(driverKeyInformation);
    driverInformation.setPort(driverMetadata.getPort());
    driverInformation.setCoordinatorPort(driverMetadata.getCoordinatorPort());
    if (driverMetadata.getDriverStatus() != null) {
      driverInformation.setDriverStatus(driverMetadata.getDriverStatus().name());
    }
    driverInformation.setStaticIoLimitationId(driverMetadata.getStaticIoLimitationId());
    driverInformation.setDynamicIoLimitationId(driverMetadata.getDynamicIoLimitationId());
    driverInformation.setChapControl(driverMetadata.getChapControl());
    driverInformation.setCreateTime(driverMetadata.getCreateTime());
    driverInformation.setVolumeName(driverMetadata.getVolumeName());
    driverInformation.setMakeUnmountForCsi(driverMetadata.isMakeUnmountForCsi());
    return driverInformation;
  }

  public void clearMemoryData() {
    driverMap.clear();
  }

  @Override
  public int updateIoLimit(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId,
      IoLimitation updateIoLimitation)
      throws AlreadyExistStaticLimitationExceptionThrift,
      DynamicIoLimitationTimeInterleavingExceptionThrift {

    return 1;
  }

  @Override
  public int deleteIoLimit(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId,
      long limitId) {

    return 1;
  }

  @Override
  public int changeLimitType(long driverContainerId, long volumeId, DriverType driverType,
      int snapshotId,
      long limitId, boolean staticLimit) {

    return 0;
  }

  @Override
  public int updateMakeUnmountDriverForCsi(long driverContainerId, long volumeId,
      DriverType driverType, int snapshotId, boolean makeUnmountForCsi) {
    DriverMetadata driverMetadata = driverMap
        .get(getKey(driverContainerId, volumeId, snapshotId, driverType));
    if (driverMetadata != null) {
      driverMetadata.setMakeUnmountForCsi(makeUnmountForCsi);
    }

    return updateMakeUnmountDriverForCsiToDb(driverContainerId, volumeId, driverType, snapshotId,
        makeUnmountForCsi);
  }
}
