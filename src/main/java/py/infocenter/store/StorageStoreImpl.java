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
import py.icshare.ArchiveInformation;
import py.icshare.InstanceMetadata;
import py.icshare.StorageInformation;


public class StorageStoreImpl implements StorageDbStore, StorageStore {

  private static final Logger logger = LoggerFactory.getLogger(StorageStoreImpl.class);
  private SessionFactory sessionFactory;
  private Map<Long, InstanceMetadata> instanceMap = new ConcurrentHashMap<>();
  private ArchiveStore archiveStore;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  public ArchiveStore getArchiveStore() {
    return archiveStore;
  }

  public void setArchiveStore(ArchiveStore archiveStore) {
    this.archiveStore = archiveStore;
  }

  @Override
  @Transactional
  public void saveToDb(StorageInformation storageInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(storageInformation);
  }

  @Override
  @Transactional
  public void updateToDb(StorageInformation storageInformation) {
    sessionFactory.getCurrentSession().update(storageInformation);
  }

  @Override
  @Transactional
  public StorageInformation getByInstanceIdFromDb(long instanceId) {
    return (StorageInformation) sessionFactory.getCurrentSession()
        .get(StorageInformation.class, instanceId);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Transactional
  public List<StorageInformation> listFromDb() {
    return sessionFactory.getCurrentSession().createQuery("from StorageInformation").list();
  }

  @Override
  @Transactional
  public int deleteFromDb(long instanceId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete StorageInformation where instanceId = :id");
    query.setParameter("id", instanceId);
    return query.executeUpdate();
  }

  @Override
  @Transactional
  public void save(InstanceMetadata instanceMetadata) {

    logger.debug("storage saved {}", instanceMetadata);
    instanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);

    // save to storage DB
    saveToDb(instanceMetadata.toStorageInformation());

    // save to archive DB
    for (ArchiveInformation archive : instanceMetadata.toArchivesInformation()) {
      logger.info("archiveStore save {}", archive.getArchiveId());
      archiveStore.save(archive);
    }
  }

  @Override
  public InstanceMetadata get(long instanceId) {
    return instanceMap.get(instanceId);
  }

  @Override
  public List<InstanceMetadata> list() {
    return new ArrayList<InstanceMetadata>(instanceMap.values());
  }

  @Override
  @Transactional
  public void delete(long instanceId) {
    // delete from storage DB
    deleteFromDb(instanceId);
    // delete from archive DB
    archiveStore.deleteByInstanceId(instanceId);
    // delete from memory
    instanceMap.remove(instanceId);
  }

  @Override
  public int size() {
    return instanceMap.size();
  }

  public void clearMemoryData() {
    instanceMap.clear();
  }

  @Override
  public void saveAll(List<InstanceMetadata> instanceMetadatas) {

  }
}
