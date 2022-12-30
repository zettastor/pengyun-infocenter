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

package py.infocenter.authorization;

import java.util.Set;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import py.app.context.AppContext;
import py.icshare.DomainStore;
import py.icshare.DomainStoreImpl;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceStoreImpl;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.AccountStoreDbImpl;
import py.icshare.authorization.ApiDbStoreImpl;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.InMemoryAccountStoreImpl;
import py.icshare.authorization.ResourceDbStoreImpl;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.RoleDbStoreImpl;
import py.icshare.authorization.RoleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.OperationStoreImpl;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.store.control.VolumeJobStoreImpl;

@Configuration
@ImportResource({"classpath:spring-config/hibernate.xml",
    "classpath:spring-config/authorization.xml"})
public class InformationCenterDbConfigTest {

  @Autowired
  SessionFactory sessionFactory;
  @Autowired
  Set<ApiToAuthorize> apiSet;



  @Bean
  @Qualifier("memory")
  public AccountStore inMemAccountStore() {
    InMemoryAccountStoreImpl inMemoryAccountStore = new InMemoryAccountStoreImpl();
    inMemoryAccountStore.setAccountStore(dbAccountStore());
    return inMemoryAccountStore;
  }



  @Bean
  @Qualifier("database")
  public AccountStore dbAccountStore() {
    AccountStoreDbImpl dbAccountStore = new AccountStoreDbImpl();
    dbAccountStore.setSessionFactory(sessionFactory);
    return dbAccountStore;
  }



  @Bean
  public ApiStore apiStore() {
    ApiDbStoreImpl apiStore = new ApiDbStoreImpl();
    apiStore.setSessionFactory(sessionFactory);
    return apiStore;
  }



  @Bean
  public RoleStore roleStore() {
    RoleDbStoreImpl roleDbStore = new RoleDbStoreImpl();
    roleDbStore.setSessionFactory(sessionFactory);
    return roleDbStore;
  }



  @Bean
  public PySecurityManager securityManager() {
    PySecurityManager securityManager = new PySecurityManager(apiSet);
    securityManager.setRoleStore(roleStore());
    securityManager.setAccountStore(inMemAccountStore());
    securityManager.setApiStore(apiStore());
    return securityManager;
  }



  @Bean
  public InformationCenterImpl informationCenter() {
    InformationCenterImpl informationCenter = new InformationCenterImpl();
    AppContext appContext = new InfoCenterAppContext("TestService");
    informationCenter.setAppContext((InfoCenterAppContext) appContext);
    informationCenter.setSecurityManager(securityManager());
    informationCenter.setOperationStore(operationStore());
    return informationCenter;
  }



  @Bean
  public OperationStore operationStore() {
    OperationStoreImpl operationStore = new OperationStoreImpl();
    operationStore.setSessionFactory(sessionFactory);
    return operationStore;
  }



  @Bean
  public ResourceStore resourceStore() {
    ResourceDbStoreImpl resourceStore = new ResourceDbStoreImpl();
    resourceStore.setSessionFactory(sessionFactory);
    return resourceStore;
  }


  @Bean
  public VolumeJobStoreDb volumeJobStoreDb() throws Exception {
    VolumeJobStoreImpl volumeJobStoreImpl = new VolumeJobStoreImpl();
    volumeJobStoreImpl.setSessionFactory(sessionFactory);
    return volumeJobStoreImpl;
  }

  @Bean
  public InstanceMaintenanceDbStore instanceMaintenanceStore() {
    InstanceMaintenanceStoreImpl instanceMaintenanceStore = new InstanceMaintenanceStoreImpl();
    instanceMaintenanceStore.setSessionFactory(sessionFactory);
    return instanceMaintenanceStore;
  }


  @Bean
  public DomainStore domainStore() {
    DomainStoreImpl domainStore = new DomainStoreImpl();
    domainStore.setSessionFactory(sessionFactory);
    return domainStore;
  }

}
