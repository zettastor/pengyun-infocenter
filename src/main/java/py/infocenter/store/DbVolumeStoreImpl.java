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
import org.hibernate.HibernateException;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.VolumeInformation;
import py.volume.CacheType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


@Transactional
public class DbVolumeStoreImpl implements VolumeStore {

  private static final Logger logger = LoggerFactory.getLogger(DbVolumeStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void deleteVolume(VolumeMetadata volumeMetadata) throws HibernateException {
    VolumeInformation volume = buildVolumeInformation(volumeMetadata);
    sessionFactory.getCurrentSession().delete(volume);
   
   
  }

  @Override
  public void saveVolume(VolumeMetadata volumeMetadata) throws HibernateException {
    VolumeInformation volume = buildVolumeInformation(volumeMetadata);
    // save to volume DB
    logger.warn("saveVolume to database, the volume id :{}, and value :{}", volume.getVolumeId(),
        volume);
    sessionFactory.getCurrentSession().saveOrUpdate(volume);
   
  }

  // Only volumes in memory are read,don`t read all volumes from database
  // this method has not been used right now
  @Override
  public List<VolumeMetadata> listVolumes() {
    List<VolumeMetadata> volumeMetadatas = new ArrayList<>();
    @SuppressWarnings("unchecked")
    List<VolumeInformation> volumeList = sessionFactory.getCurrentSession()
        .createQuery("from VolumeInformation").list();

    if (volumeList == null) {
      logger.warn("can`t get delete volume request from database");
      return volumeMetadatas;
    }

    for (VolumeInformation volumeInfo : volumeList) {
      VolumeMetadata volume = new VolumeMetadata();
      volume.setVolumeId(volumeInfo.getVolumeId());
      volume.setRootVolumeId(volumeInfo.getRootVolumeId());
      volume.setChildVolumeId(volumeInfo.getChildVolumeId());
      volume.setVolumeSize(volumeInfo.getVolumeSize());
      volume.setSegmentSize(volumeInfo.getSegmentSize());
      volume.setExtendingSize(volumeInfo.getExtendingSize());
      volume.setName(volumeInfo.getName());
      volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
      volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
      volume.setAccountId(volumeInfo.getAccountId());
      volume.setSegmentSize(volumeInfo.getSegmentSize());
      volume.setStoragePoolId(volumeInfo.getStoragePoolId());
      volume.setDomainId(volumeInfo.getDomainId());
      if (volumeInfo.getDeadTime() != null) {
        volume.setDeadTime(volumeInfo.getDeadTime());
      }
      volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
      volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
      volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
      volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
      volume.setInAction(VolumeInAction.valueOf(volumeInfo.getInAction()));
      volume.setPageWrappCount(volumeInfo.getPageWrappCount());
      volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
      volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
      volume.getRebalanceInfo().setRebalanceVersion(volumeInfo.getRebalanceVersion());
      volume.setEachTimeExtendVolumeSize(volumeInfo.getEachTimeExtendVolumeSize());
      volume.setMarkDelete(volumeInfo.isMarkDelete());
      volume.setVolumeDescription(volumeInfo.getVolumeDescription());
      volume.setClientLastConnectTime(volumeInfo.getClientLastConnectTime());

      volumeMetadatas.add(volume);
    }
    return volumeMetadatas;
  }

  @Override
  public VolumeMetadata getVolume(Long volumeId) {

    VolumeInformation volumeInfo = sessionFactory.getCurrentSession().get(
        VolumeInformation.class, volumeId.longValue());
    // TODO: because segment units have not been saved to database, so don`t care segment unit
    if (volumeInfo != null) {

      VolumeMetadata volume = new VolumeMetadata();
      volume.setVolumeId(volumeInfo.getVolumeId());
      volume.setRootVolumeId(volumeInfo.getRootVolumeId());

      volume.setChildVolumeId(volumeInfo.getChildVolumeId());

      volume.setVolumeSize(volumeInfo.getVolumeSize());
      volume.setSegmentSize(volumeInfo.getSegmentSize());
      volume.setExtendingSize(volumeInfo.getExtendingSize());
      volume.setName(volumeInfo.getName());
      volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
      volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
      volume.setAccountId(volumeInfo.getAccountId());
      volume.setStoragePoolId(volumeInfo.getStoragePoolId());

      volume.setVolumeLayout(volumeInfo.getVolumeLayout());

      if (volumeInfo.getDeadTime() != null) {
        volume.setDeadTime(volumeInfo.getDeadTime());
      }
      if (volumeInfo.getDomainId() != null) {
        volume.setDomainId(volumeInfo.getDomainId());
      }

      volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
      volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
      volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
      volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
      volume.setInAction(VolumeInAction.valueOf(volumeInfo.getInAction()));
      volume.setPageWrappCount(volumeInfo.getPageWrappCount());
      volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
      volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
      volume.getRebalanceInfo().setRebalanceVersion(volumeInfo.getRebalanceVersion());
      volume.getRebalanceInfo().setRebalanceVersion(volumeInfo.getRebalanceVersion());
      volume.setEachTimeExtendVolumeSize(volumeInfo.getEachTimeExtendVolumeSize());
      volume.setMarkDelete(volumeInfo.isMarkDelete());
      volume.setVolumeDescription(volumeInfo.getVolumeDescription());
      volume.setClientLastConnectTime(volumeInfo.getClientLastConnectTime());

      return volume;
    }
    return null;
  }

  @Override
  public int updateStatusAndVolumeInAction(long volumeId, String volumeStatus,
      String volumeInAction) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeInformation set volumeStatus = :volumeStatus, inAction = :inAction where "
            + "volumeId = :id");
    query.setParameter("id", volumeId);
    query.setParameter("volumeStatus", volumeStatus);
    query.setParameter("inAction", volumeInAction);

    return query.executeUpdate();
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount,
      String volumeLayout) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeInformation set volumeLayout = :volumeLayout where volumeId = :id");
    query.setParameter("id", volumeId);
    query.setParameter("volumeLayout", volumeLayout);
    query.executeUpdate();
    return null;
  }

  @Override
  public void updateDescription(long volumeId, String description) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeInformation set volumeDescription = :volumeDescription where volumeId = :id");
    query.setParameter("id", volumeId);
    query.setParameter("volumeDescription", description);
    query.executeUpdate();
  }

  @Override
  public void updateClientLastConnectTime(long volumeId, long time) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeInformation set clientLastConnectTime = :clientLastConnectTime where "
            + "volumeId = :id");
    query.setParameter("id", volumeId);
    query.setParameter("clientLastConnectTime", time);
    query.executeUpdate();
  }

  @Override
  public VolumeMetadata followerGetVolume(Long volumeId) {
    //the follower get volume must form db
    return getVolume(volumeId);
  }

  @Override
  public void clearData() {
    logger.error("needn't to remove the data from databsse, only used for test");
    sessionFactory.getCurrentSession().createQuery("delete from VolumeInformation").executeUpdate();
    /*  throw new NotImplementedException("needn't to remove the data from database");*/
  }

  private Blob createBlob(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }

  @Override
  public VolumeMetadata getVolumeNotDeadByName(String name) {
    @SuppressWarnings("unchecked")
    List<VolumeInformation> volumeList = sessionFactory.getCurrentSession()
        .createQuery("from VolumeInformation where name = :name " + "and volumeStatus != :status "
            + "and markDelete is not true")
        .setParameter("name", name).setParameter("status", "Dead").list();
    if (0 == volumeList.size()) {
      return null;
    }
    VolumeInformation volumeInfo = volumeList.get(0);
    VolumeMetadata volume = new VolumeMetadata();
    volume.setVolumeId(volumeInfo.getVolumeId());
    volume.setRootVolumeId(volumeInfo.getRootVolumeId());
    volume.setChildVolumeId(volumeInfo.getChildVolumeId());
    volume.setVolumeSize(volumeInfo.getVolumeSize());
    volume.setSegmentSize(volumeInfo.getSegmentSize());
    volume.setExtendingSize(volumeInfo.getExtendingSize());
    volume.setName(volumeInfo.getName());
    volume.setVolumeType(VolumeType.valueOf(volumeInfo.getVolumeType()));
    volume.setVolumeStatus(VolumeStatus.findByValue(volumeInfo.getVolumeStatus()));
    volume.setAccountId(volumeInfo.getAccountId());
    volume.setSegmentSize(volumeInfo.getSegmentSize());
    volume.setStoragePoolId(volumeInfo.getStoragePoolId());
    if (volumeInfo.getDeadTime() != null) {
      volume.setDeadTime(volumeInfo.getDeadTime());
    }
    volume.setVolumeLayout(volumeInfo.getVolumeLayout());

    volume.setVolumeCreatedTime(volumeInfo.getVolumeCreatedTime());
    volume.setLastExtendedTime(volumeInfo.getLastExtendedTime());
    volume.setVolumeSource(VolumeMetadata.VolumeSourceType.valueOf(volumeInfo.getVolumeSource()));
    volume.setReadWrite(VolumeMetadata.ReadWriteType.valueOf(volumeInfo.getReadWrite()));
    volume.setInAction(VolumeInAction.valueOf(volumeInfo.getInAction()));
    volume.setPageWrappCount(volumeInfo.getPageWrappCount());
    volume.setSegmentWrappCount(volumeInfo.getSegmentWrappCount());
    volume.setEnableLaunchMultiDrivers(volumeInfo.isEnableLaunchMultiDrivers());
    volume.getRebalanceInfo().setRebalanceVersion(volumeInfo.getRebalanceVersion());
    volume.setEachTimeExtendVolumeSize(volumeInfo.getEachTimeExtendVolumeSize());
    volume.setMarkDelete(volumeInfo.isMarkDelete());
    volume.setVolumeDescription(volumeInfo.getVolumeDescription());
    volume.setClientLastConnectTime(volumeInfo.getClientLastConnectTime());

    return volume;
  }

  @Override
  public void deleteVolumeForReport(VolumeMetadata volumeMetadata) {

  }

  @Override
  public VolumeMetadata getVolumeForReport(Long volumeId) {
    return null;
  }

  @Override
  public String updateVolumeLayoutForReport(long volumeId, int startSegmentIndex,
      int newSegmentsCount, String volumeLayout) {
    return null;
  }

  @Override
  public void clearDataForReport() {

  }

  @Override
  public VolumeMetadata listVolumesByIdForReport(long rootId) {
    return null;
  }

  @Override
  public long updateRebalanceVersion(long volumeId, long version) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeInformation set rebalanceVersion = :rebalanceVersion where volumeId = :id");
    query.setParameter("id", volumeId);
    query.setParameter("rebalanceVersion", version);
    query.executeUpdate();
    return version;
  }

  @Override
  public long incrementRebalanceVersion(long volumeId) {
    return 0;
  }

  @Override
  public long getRebalanceVersion(long volumeId) {
    return 0;
  }

  @Override
  public void updateRebalanceInfo(long volumeId, VolumeRebalanceInfo rebalanceInfo) {

  }

  @Override
  public void updateStableTime(long volumeId, long timeMs) {

  }

  @Override
  public int updateDeadTimeForReport(long volumeId, long deadTime) {
    return 0;
  }

  @Override
  public int updateStatusAndVolumeInActionForReport(long volumeId, String status,
      String volumeInAction) {
    return 0;
  }

  @Override
  public int updateVolumeExtendStatusForReport(long volumeId, String status) {
    return 0;
  }

  @Override
  public int updateExtendingSizeForReport(long volumeId, long extendingSize,
      long thisTimeExtendSize) {
    return 0;
  }

  @Override
  public void saveVolumeForReport(VolumeMetadata volumeMetadata) {

  }

  @Override
  public List<VolumeMetadata> listVolumesForReport() {
    return null;
  }

  @Override
  public void updateVolumeForReport(VolumeMetadata volumeMetadata) {
  }

  @Override
  public List<VolumeMetadata> listVolumesWhichToDeleteForReport() {
    return null;
  }

  @Override
  public void saveVolumesWhichToDeleteForReport(VolumeMetadata volumeMetadata) {

  }

  @Override
  public void deleteVolumesWhichToDeleteForReport(Long volumeId) {

  }

  @Override
  public void markVolumeDelete(Long volumeId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update VolumeInformation set markDelete = :markDelete where volumeId = :id");
    query.setParameter("id", volumeId);
    query.setParameter("markDelete", true);
    query.executeUpdate();
  }

  @Override
  public void loadVolumeFromDbToMemory() {
  }

  @Override
  public void loadVolumeInDb() {
  }


  
  public VolumeInformation buildVolumeInformation(VolumeMetadata volumeMetadata) {

    VolumeInformation volumeInfo = new VolumeInformation();

    volumeInfo.setVolumeId(volumeMetadata.getVolumeId());
    volumeInfo.setRootVolumeId(volumeMetadata.getRootVolumeId());

    volumeInfo.setChildVolumeId(volumeMetadata.getChildVolumeId());

    volumeInfo.setVolumeSize(volumeMetadata.getVolumeSize());

    volumeInfo.setExtendingSize(volumeMetadata.getExtendingSize());

    volumeInfo.setName(volumeMetadata.getName());

    if (volumeMetadata.getVolumeType() != null) {
      volumeInfo.setVolumeType(volumeMetadata.getVolumeType().name());
    }
    if (volumeMetadata.getVolumeStatus() != null) {
      volumeInfo.setVolumeStatus(volumeMetadata.getVolumeStatus().name());
    }
    volumeInfo.setAccountId(volumeMetadata.getAccountId());

    volumeInfo.setSegmentSize(volumeMetadata.getSegmentSize());

    volumeInfo.setDeadTime(volumeMetadata.getDeadTime());
    volumeInfo.setDomainId(volumeMetadata.getDomainId());
    volumeInfo.setStoragePoolId(volumeMetadata.getStoragePoolId());

    volumeInfo.setVolumeLayout(volumeMetadata.getVolumeLayout());

    volumeInfo.setVolumeCreatedTime(volumeMetadata.getVolumeCreatedTime());
    volumeInfo.setLastExtendedTime(volumeMetadata.getLastExtendedTime());

    volumeInfo.setPageWrappCount(volumeMetadata.getPageWrappCount());
    volumeInfo.setSegmentWrappCount(volumeMetadata.getSegmentWrappCount());

    if (volumeMetadata.getVolumeSource() != null) {
      volumeInfo.setVolumeSource(volumeMetadata.getVolumeSource().name());
    }

    if (volumeMetadata.getReadWrite() != null) {
      volumeInfo.setReadWrite(volumeMetadata.getReadWrite().name());
    }

    if (volumeMetadata.getInAction() != null) {
      volumeInfo.setInAction(volumeMetadata.getInAction().name());
    }
    volumeInfo.setEnableLaunchMultiDrivers(volumeMetadata.isEnableLaunchMultiDrivers());
    volumeInfo.setRebalanceVersion(volumeMetadata.getRebalanceInfo().getRebalanceVersion());

    volumeInfo.setEachTimeExtendVolumeSize(volumeMetadata.getEachTimeExtendVolumeSize());
    volumeInfo.setMarkDelete(volumeMetadata.isMarkDelete());
    volumeInfo.setVolumeDescription(volumeMetadata.getVolumeDescription());
    volumeInfo.setClientLastConnectTime(volumeMetadata.getClientLastConnectTime());

    return volumeInfo;
  }

}