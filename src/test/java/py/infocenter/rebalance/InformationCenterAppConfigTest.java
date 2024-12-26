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
