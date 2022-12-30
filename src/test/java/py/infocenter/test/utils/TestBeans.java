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

package py.infocenter.test.utils;

import java.util.Properties;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.icshare.authorization.ApiDbStoreImpl;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ResourceDbStoreImpl;
import py.icshare.authorization.ResourceStore;
import py.icshare.qos.IoLimitationRelationshipStore;
import py.icshare.qos.IoLimitationRelationshipStoreImpl;
import py.icshare.qos.IoLimitationStoreImpl;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.MigrationRuleStoreImpl;
import py.infocenter.driver.client.manger.DriverClientManger;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.AccessRuleStoreImpl;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.DriverClientStore;
import py.infocenter.store.DriverClientStoreImpl;
import py.infocenter.store.DriverStore;
import py.infocenter.store.DriverStoreImpl;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.InstanceVolumesInformationStoreImpl;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiAccessRuleStoreImpl;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.IscsiRuleRelationshipStoreImpl;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeRuleRelationshipStoreImpl;
import py.infocenter.store.VolumeStore;
import py.io.qos.IoLimitationStore;


@Configuration
@ImportResource({"classpath:spring-config/hibernate.xml"})
public class TestBeans {

  @Autowired
  private SessionFactory sessionFactory;


  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }


  @Bean
  public Logger logger() {
    Properties log4jProperties = new Properties();
    log4jProperties.put("log4j.rootLogger", "DEBUG, stdout, InfoCenter");
    log4jProperties.put("log4j.appender.InfoCenter", "org.apache.log4j.RollingFileAppender");
    log4jProperties.put("log4j.appender.InfoCenter.Threshold", "DEBUG");
    log4jProperties.put("log4j.appender.InfoCenter.File", "logs/infomation-center-test.log");
    log4jProperties.put("log4j.appender.InfoCenter.layout", "org.apache.log4j.PatternLayout");
    log4jProperties
        .put("log4j.appender.InfoCenter.layout.ConversionPattern", "%-5p[%d][%t]%C(%L):%m%n");
    log4jProperties.put("log4j.appender.InfoCenter.MaxBackupIndex", "10");
    log4jProperties.put("log4j.appender.InfoCenter.MaxFileSize", "400MB");
    log4jProperties.put("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
    log4jProperties.put("log4j.appender.stdout.Threshold", "DEBUG");
    log4jProperties.put("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
    log4jProperties
        .put("log4j.appender.stdout.layout.ConversionPattern", "%-5p[%d][%t]%C(%L):%m%n");

    log4jProperties.put("log4j.logger.org.hibernate", "ERROR, stdout, InfoCenter");
    log4jProperties.put("log4j.logger.org.springframework", "ERROR, stdout, InfoCenter");
    log4jProperties.put("log4j.logger.com.opensymphony", "ERROR, stdout, InfoCenter");
    log4jProperties.put("log4j.logger.org.apache", "ERROR, stdout, InfoCenter");
    log4jProperties.put("log4j.logger.com.googlecode", "ERROR, stdout, InfoCenter");
    log4jProperties.put("log4j.logger.com.twitter.common.stats", "ERROR, stdout, InfoCenter");
    log4jProperties.put("log4j.logger.com.mchange", "ERROR, stdout, InfoCenter");
    PropertyConfigurator.configure(log4jProperties);

    return null;
  }


  @Bean
  public DriverStore driverStore() {
    DriverStoreImpl driverStore = new DriverStoreImpl();
    driverStore.setSessionFactory(sessionFactory);
    return driverStore;
  }


  @Bean
  public AccessRuleStore dbAccessRuleStore() {
    AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
    accessRuleStore.setSessionFactory(sessionFactory);
    return accessRuleStore;
  }


  @Bean
  public VolumeRuleRelationshipStore dbVolumeRuleRelationshipStore() {
    VolumeRuleRelationshipStoreImpl volumeRuleRelationshipStore =
        new VolumeRuleRelationshipStoreImpl();
    volumeRuleRelationshipStore.setSessionFactory(sessionFactory);
    return volumeRuleRelationshipStore;
  }


  @Bean
  public IscsiAccessRuleStore dbIscsiAccessRuleStore() {
    IscsiAccessRuleStoreImpl iscsiAccessRuleStore = new IscsiAccessRuleStoreImpl();
    iscsiAccessRuleStore.setSessionFactory(sessionFactory);
    return iscsiAccessRuleStore;
  }


  @Bean
  public IscsiRuleRelationshipStore dbIscsiRuleRelationshipStore() {
    IscsiRuleRelationshipStoreImpl iscsiRuleRelationshipStore =
        new IscsiRuleRelationshipStoreImpl();
    iscsiRuleRelationshipStore.setSessionFactory(sessionFactory);
    return iscsiRuleRelationshipStore;
  }


  @Bean
  public IoLimitationStore ioLimitationStore() {
    IoLimitationStoreImpl ioLimitationStore = new IoLimitationStoreImpl();
    ioLimitationStore.setSessionFactory(sessionFactory);
    return ioLimitationStore;
  }


  @Bean
  public IoLimitationRelationshipStore ioLimitationRelationshipStore() {
    IoLimitationRelationshipStoreImpl ioLimitationRelationshipStore =
        new IoLimitationRelationshipStoreImpl();
    ioLimitationRelationshipStore.setSessionFactory(sessionFactory);
    return ioLimitationRelationshipStore;
  }


  @Bean
  public MigrationRuleStore migrationSpeedRuleStore() {
    MigrationRuleStoreImpl migrationSpeedRuleStore = new MigrationRuleStoreImpl();
    migrationSpeedRuleStore.setSessionFactory(sessionFactory);
    return migrationSpeedRuleStore;
  }


  @Bean
  public ApiStore apiStore() {
    ApiDbStoreImpl apiStore = new ApiDbStoreImpl();
    apiStore.setSessionFactory(sessionFactory);
    return apiStore;
  }


  @Bean
  public ResourceStore resourceStore() {
    ResourceDbStoreImpl resourceStore = new ResourceDbStoreImpl();
    resourceStore.setSessionFactory(sessionFactory);
    return resourceStore;
  }


  @Bean
  public DriverClientStore driverClientStore() throws Exception {
    DriverClientStoreImpl driverClientStore = new DriverClientStoreImpl();
    driverClientStore.setSessionFactory(sessionFactory);
    return driverClientStore;
  }


  @Bean
  InstanceVolumesInformationStore instanceVolumesInformationStoreTest() {
    InstanceVolumesInformationStoreImpl instanceVolumesInformationStore =
        new InstanceVolumesInformationStoreImpl();
    instanceVolumesInformationStore.setSessionFactory(sessionFactory);
    return instanceVolumesInformationStore;
  }


  @Bean
  public DriverClientManger driverClientManger() throws Exception {
    DriverClientManger driverClientManger = new DriverClientManger(driverClientStore(), 10, 30);
    return driverClientManger;
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
