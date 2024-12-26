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
