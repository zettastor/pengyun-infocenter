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

package py.infocenter.rebalance;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import py.icshare.StoragePoolStoreImpl;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePoolStore;

@Configuration
@ImportResource({"classpath:spring-config/hibernate.xml"})
class InformationCenterAppConfigTest {

  @Autowired
  private SessionFactory sessionFactory;

  @Bean
  public RebalanceRuleStore rebalanceRuleStore() {
    RebalanceRuleStoreImpl rebalanceRuleStore = new RebalanceRuleStoreImpl();
    rebalanceRuleStore.setSessionFactory(sessionFactory);
    return rebalanceRuleStore;
  }

  @Bean
  public StoragePoolStore storagePoolStore() {
    StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
    storagePoolStore.setSessionFactory(sessionFactory);
    return storagePoolStore;
  }

  @Bean
  public VolumeStore inMemoryVolumeStore() {
    return new MemoryVolumeStoreImpl();
  }

  @Bean
  public VolumeStore twoLevelVolumeStore() {
    return new TwoLevelVolumeStoreImpl(inMemoryVolumeStore(), dbVolumeStore());
  }

  @Bean
  public VolumeStore dbVolumeStore() {
    DbVolumeStoreImpl volumeStoreImpl = new DbVolumeStoreImpl();
    volumeStoreImpl.setSessionFactory(sessionFactory);
    return volumeStoreImpl;
  }
}
