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

package py.infocenter;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.scheduling.annotation.EnableScheduling;
import py.app.NetworkConfiguration;
import py.app.context.InstanceIdFileStore;
import py.app.healthcheck.CheckMasterHealthCheckerImpl;
import py.app.healthcheck.DihClientBuilder;
import py.app.healthcheck.DihClientBuilderImpl;
import py.app.healthcheck.HealthChecker;
import py.app.healthcheck.HealthCheckerWithThriftImpl;
import py.app.thrift.ThriftAppEngine;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.dih.client.worker.CheckMasterHeartBeatWorkerFactory;
import py.dih.client.worker.DihClientBuildWorkerFactory;
import py.dih.client.worker.HeartBeatWorkerFactory;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.icshare.CapacityRecordStore;
import py.icshare.CapacityRecordStoreImpl;
import py.icshare.DomainStore;
import py.icshare.DomainStoreImpl;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceStoreImpl;
import py.icshare.RecoverDbSentryStoreImpl;
import py.icshare.ScsiClientStore;
import py.icshare.ScsiClientStoreImpl;
import py.icshare.StoragePoolStoreImpl;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.AccountStoreDbImpl;
import py.icshare.authorization.ApiDbStoreImpl;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.InMemoryAccountStoreImpl;
import py.icshare.authorization.ResourceDbStoreImpl;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.Role;
import py.icshare.authorization.RoleDbStoreImpl;
import py.icshare.authorization.RoleStore;
import py.icshare.qos.IoLimitationStoreImpl;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.MigrationRuleStoreImpl;
import py.icshare.qos.RebalanceRuleStore;
import py.icshare.qos.RebalanceRuleStoreImpl;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.common.InfoCenterConstants;
import py.infocenter.create.volume.CreateVolumeManager;
import py.infocenter.create.volume.ReserveInformation;
import py.infocenter.driver.client.manger.DriverClientManger;
import py.infocenter.engine.DataBaseTaskEngineFactory;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.HaInstanceMangerWithZk;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.job.ScsiDriverStatusUpdate;
import py.infocenter.job.UpdateOperationProcessor;
import py.infocenter.job.UpdateOperationProcessorImpl;
import py.infocenter.job.VolumeProcessor;
import py.infocenter.job.VolumeProcessorImpl;
import py.infocenter.rebalance.RebalanceConfiguration;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.rebalance.old.RebalanceTaskExecutor;
import py.infocenter.rebalance.old.RebalanceTaskExecutorImpl;
import py.infocenter.reportvolume.ReportVolumeManager;
import py.infocenter.service.ExceptionForOperation;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.service.ServerStatusCheck;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.infocenter.service.selection.FrugalInstanceSelectionStrategy;
import py.infocenter.service.selection.InstanceSelectionStrategy;
import py.infocenter.service.selection.RandomSelectionStrategy;
import py.infocenter.service.selection.SelectionStrategy;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.AccessRuleStoreImpl;
import py.infocenter.store.ArchiveStore;
import py.infocenter.store.ArchiveStoreImpl;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.DiskInfoStore;
import py.infocenter.store.DiskInfoStoreImpl;
import py.infocenter.store.DriverClientStore;
import py.infocenter.store.DriverClientStoreImpl;
import py.infocenter.store.DriverStore;
import py.infocenter.store.DriverStoreImpl;
import py.infocenter.store.InfoCenterStoreSet;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.InstanceVolumesInformationStoreImpl;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiAccessRuleStoreImpl;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.IscsiRuleRelationshipStoreImpl;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.OrphanVolumeStoreImpl;
import py.infocenter.store.ScsiDriverStore;
import py.infocenter.store.ScsiDriverStoreImpl;
import py.infocenter.store.SegmentStore;
import py.infocenter.store.SegmentStoreImpl;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.SegmentUnitTimeoutStoreImpl;
import py.infocenter.store.ServerNodeStore;
import py.infocenter.store.ServerNodeStoreImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.StorageStoreImpl;
import py.infocenter.store.TaskStore;
import py.infocenter.store.TaskStoreImpl;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeDelayStoreImpl;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRecycleStoreImpl;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.VolumeRuleRelationshipStoreImpl;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStatusTransitionStoreImpl;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.OperationStoreImpl;
import py.infocenter.store.control.VolumeJobProcess;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.store.control.VolumeJobStoreImpl;
import py.infocenter.volume.recycle.VolumeRecycleManager;
import py.infocenter.worker.CreateSegmentUnitWorkThread;
import py.infocenter.worker.DriverClientManagerSweeperFactory;
import py.infocenter.worker.DriverStoreSweeperFactory;
import py.infocenter.worker.GetRebalanceTaskSweeper;
import py.infocenter.worker.GetRebalanceTaskSweeperFactory;
import py.infocenter.worker.HaInstanceCheckSweeperFactory;
import py.infocenter.worker.HaInstanceEquilibriumSweeperFactory;
import py.infocenter.worker.ReportVolumeInfoSweeperFactory;
import py.infocenter.worker.ServerNodeAlertCheckerFactory;
import py.infocenter.worker.ServerStatusCheckFactory;
import py.infocenter.worker.StorageStoreSweeperFactory;
import py.infocenter.worker.TimeoutSweeperFactory;
import py.infocenter.worker.VolumeActionSweeperFactory;
import py.infocenter.worker.VolumeDeleteDelayCheckerFactory;
import py.infocenter.worker.VolumeSweeperFactory;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.io.qos.IoLimitationStore;
import py.monitor.common.OperationName;
import py.monitor.jmx.configuration.JmxAgentConfiguration;
import py.monitor.jmx.server.JmxAgent;
import py.periodic.UnableToStartException;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.storage.StorageConfiguration;
import py.thrift.datanode.service.DataNodeService;


@Configuration
@EnableScheduling
@ImportResource({"classpath:spring-config/authorization.xml",
    "classpath:spring-config/defaultMigrationRule.xml",
    "classpath:spring-config/hibernate.xml"})
@PropertySource("classpath:config/infocenter.properties")
@Import({StorageConfiguration.class, NetworkConfiguration.class, JmxAgentConfiguration.class,
    InfoCenterConfiguration.class})
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class InformationCenterAppConfig {

  private static final Logger logger = LoggerFactory.getLogger(InformationCenterAppConfig.class);
  @Value("${thrift.client.timeout}")
  private int thriftClientTimeout;

  @Value("${app.name}")
  private String appName;

  @Value("${driver.report.timeout.ms}")
  private long driverReportTimeout;

  @Value("${instance.metadata.to.remove:120000}")
  private int instanceMetadataTimeToRemove;

  @Value("${driver.sweeper.rate:1000}")
  private int driverSweeperRate;

  @Value("${volume.sweeper.rate}")
  private int volumeSweeperRate;

  @Value("${timeout.sweeper.rate:1000}")
  private int timeoutSweeper;

  @Value("${alarm.sweeper.rate:5000}")
  private int alarmSweeperRate;

  @Value("${servernode.alert.checker.rate:10000}")
  private int serverNodeAlertCheckerRate;

  //10s
  @Value("${server.status.checker.rate:10000}")
  private int serverStatusCheckRate;

  @Value("${instance.metadata.sweeper.rate:5000}")
  private int instanceMetadataSweeperRate;

  @Value("${app.main.endpoint}")
  private String mainEndPoint;

  @Value("${health.checker.rate}")
  private int healthCheckerRate;

  @Value("${dih.endpoint}")
  private String dihEndPoint;

  @Value("${app.location}")
  private String appLocation;

  @Value("${zookeeper.connection.string}")
  private String zookeeperConnectionString;

  @Value("${zookeeper.session.timeout.ms:10000}")
  private int zookeeperSessionTimeout;

  @Value("${zookeeper.election.switch:false}")
  private boolean zookeeperElectionSwitch;

  @Value("${zookeeper.lock.directory}")
  private String lockDirectory;

  @Value("${dead.volume.to.remove.second:15552000}")
  private int deadVolumeToRemove;

  @Value("${segment.unit.report.timeout.second:90}")
  private int segmentUnitReportTimeout;

  @Value("${volume.tobecreated.timeout.second:90}")
  private int volumeTobeCreatedTimeout;

  @Value("${volume.becreating.timeout.second:1800}")
  private int volumeBeCreatingTimeout;

  @Value("${fix.volume.timeout.second:600}")
  private int fixVolumeTimeoutSec = 600;

  @Value("${group.count:6}")
  private int groupCount;

  @Value("${next.action.time.interval.ms:150000}")
  private int nextActionTimeIntervalMs = 150000;

  @Value("${log.level}")
  private String logLevel;

  @Value("${actual.free.space.refresh.period.time}")
  private long refreshPeriodTime;

  @Value("${log.output.file}")
  private String logOutputFile;

  @Value("${volume.tobeorphan.time:600000}")
  private long volumeToBeOrphanTime;

  @Value("${factor.for.selector: 2}")
  private int factorForSelector = 2;

  @Value("${remainder.for.selector: 0}")
  private int remainderForSelector = 0;

  @Value("${store.capacity.record.count: 7}")
  private int storeCapacityRecordCount = 7;

  @Value("${take.sample.for.capacity.interval.second: 86400}")
  private int takeSampleInterValSecond = 86400;

  @Value("${zookeeper.launcher}")
  private String zooKeeperLauncher;

  @Value("${round.time.interval.ms: 30000}")
  private long roundTimeInterval = 30000;

  @Value("${max.backup.database.count: 3}")
  private int maxBackupCount = 3;

  // the max rebalance task count in volume per datanode
  @Value("${max.rebalance.task.count.volume.datanode: 15}")
  private int maxRebalanceTaskCountPerVolumeOfDatanode;

  // the max volume count in pool for rebalance
  @Value("${max.rebalance.volume.count.pool: 1}")
  private int maxRebalanceVolumeCountPerPool;

  @Value("${max.rebalance.task.count: 10}")
  private int maxRebalanceTaskCount;

  @Value("${rebalance.pressure.threshold: 0.1}")
  private double rebalancePressureThreshold;

  @Value("${rebalance.pressure.threshold.accuracy: 3}")
  private long rebalancePressureThresholdAccuracy;

  @Value("${rebalance.pressure.addend: 0}")
  private int rebalancePressureAddend;

  @Value("${rebalance.trigger.period.seconds: 60}")
  private long rebalanceTriggerPeriod = 60;

  @Value("${rebalance.trigger.volume.segment.count.min: 20}")
  private int minOfSegmentCountCanDoRebalance = 20;

  @Value("${rebalance.task.expire.time.seconds: 1800}")
  private int rebalanceTaskExpireTimeSeconds = 1800;

  @Value("${get.rebalance.task.delay.rate.ms: 5000}")
  private int getRebalanceTaskDelayRateMs = 5000;

  @Value("${get.rebalance.task.expired.time.ms: 30000}")
  private int getRebalanceTaskExpireTimeMs = 30000;

  @Value("${page.wrapp.count: 128}")
  private int pageWrappCount = 128;

  @Value("${segment.wrapp.count: 10}")
  private int segmentWrappCount = 10;

  @Value("${servernode.report.overtime.second: 30}")
  private int serverNodeReportOverTimeSecond;

  @Value("${wait.collect.volume.info.second: 30}")
  private int waitCollectVolumeInfoSecond = 30;

  @Value("${instance.time.out.second: 10}")
  private int instanceTimeOutCheck = 10;

  @Value("${license.storage.type}")
  private String type;

  @Value("${create.segmentunit.passel.count:100}")
  private int passelCount;

  @Value("${create.segment.unit.threadpool.core.size:2}")
  private int createSegmentUnitThreadPoolCoreSize;

  @Value("${create.segment.unit.threadpool.max.size:50}")
  private int createSegmentUnitThreadPoolMaxSize;

  @Value("${save.operation.days:30}")
  private int saveOperationDays = 30;
  @Value("${rebalance.threadpool.core.size:2}")
  private int rebalanceThreadPoolCoreSize;

  @Value("${rebalance.threadpool.max.size:5}")
  private int rebalanceThreadPoolMaxSize;

  @Value("${rebalance.task.count:5}")
  private int rebalanceTaskCount;

  @Value("${rebalance.start.by.default:false}")
  private boolean rebalanceStartByDefault;

  @Value("${rollback.passtime.second:86400}")
  private int rollbackPassTimeSecond;

  @Value("${operation.passtime.second:600}")
  private int operationPasstimeSecond;

  @Value("${network.checksum.algorithm:DUMMY}")
  private String networkChecksumAlgorithm = "DUMMY";

  @Value("${create.volume.timeout.second:600}")
  private int createVolumeTimeOut;

  @Value("${enable.instance.equilibrium.volume:false}")
  private boolean enableInstanceEquilibriumVolume;

  @Value("${instance.equilibrium.volume.time:600000}")
  private int instanceEquilibriumVolumeTime; //ms

  @Value("${follow.report.delay:10000}")
  private int followReportDelay = 10000;

  //the all segment number which master or follower can save
  @Value("${all.segment.number.to.save:10000}")

  private long allSegmentNumberToSave = 10000;

  //the all segment number which master can save = all.segment.number.to.save * percent.number
  // .master.segment
  @Value("${percent.number.master.segment:0.6}")
  private double percentNumberSegmentNumberMasterSave = 0.6;

  @Value("${DB.task.core.pool.size:5}")
  private int dbTaskCorePoolSize;
  @Value("${DB.task.max.pool.size:20}")
  private int dbTaskMaxPoolSize;
  @Value("${DB.task.max.concurrent.size:20}")
  private int dbTaskMaxConcurrentSize;
  @Value("${DB.task.worker.rate.ms:5000}")
  private int dbTaskWorkerRateMs;

  @Value("${volume.delete.delay.rate:10}")
  private long volumeDeleteDelayRate;

  //20 day
  @Value("${volume.keep.recycle.time.second:1728000}")
  private long volumeKeepRecycleTimeSecond;

  //Driver Client Information 30 day
  @Value("${driver.client.info.keep.time.second:2592000}")
  private long driverClientInfoKeepTime;

  @Value("${driver.client.info.keep.number:100}")
  private int driverClientInfoKeepNumber;

  @Value("${driver.manager.client.sweeper.rate:30000}")
  private int driverClientInfoSweeperRate;

  @Autowired
  private InfoCenterConfiguration infoCenterConfiguration;

  @Autowired
  private Set<ApiToAuthorize> apiSet;

  @Autowired
  private Set<Role> builtInRoles;

  @Autowired
  private SessionFactory sessionFactory;

  @Autowired
  private StorageConfiguration storageConfiguration;

  @Autowired
  private NetworkConfiguration networkConfiguration;

  @Autowired
  private JmxAgentConfiguration jmxAgentConfiguration;

  @Autowired
  private Set<MigrationRule> defaultMigrationRuleSet;


  //---------------network size----------------------
  //datanode max thrift network frame size
  @Value("${max.network.frame.size:17000000}")
  private int maxNetworkFrameSize = 17 * 1000 * 1000;

  /********************* other.  *******/
  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  public int getMaxNetworkFrameSize() {
    return maxNetworkFrameSize;
  }

  public void setMaxNetworkFrameSize(int maxNetworkFrameSize) {
    this.maxNetworkFrameSize = maxNetworkFrameSize;
  }

  public boolean getZookeeperElectionSwitch() {
    return zookeeperElectionSwitch;
  }

  public int getZookeeperSessionTimeout() {
    return zookeeperSessionTimeout;
  }

  public String getZooKeeperLauncher() {
    return zooKeeperLauncher;
  }

  public void setZooKeeperLauncher(String zooKeeperLauncher) {
    this.zooKeeperLauncher = zooKeeperLauncher;
  }

  public boolean isEnableInstanceEquilibriumVolume() {
    return enableInstanceEquilibriumVolume;
  }

  public void setEnableInstanceEquilibriumVolume(boolean enableInstanceEquilibriumVolume) {
    this.enableInstanceEquilibriumVolume = enableInstanceEquilibriumVolume;
  }

  public int getInstanceEquilibriumVolumeTime() {
    return instanceEquilibriumVolumeTime;
  }

  public void setInstanceEquilibriumVolumeTime(int instanceEquilibriumVolumeTime) {
    this.instanceEquilibriumVolumeTime = instanceEquilibriumVolumeTime;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getGetRebalanceTaskDelayRateMs() {
    return getRebalanceTaskDelayRateMs;
  }

  public int getGetRebalanceTaskExpireTimeMs() {
    return getRebalanceTaskExpireTimeMs;
  }

  public String getZookeeperConnectionString() {
    return zookeeperConnectionString;
  }


  @Bean
  public ThriftAppEngine appEngine() throws Exception {
    initInfoCenterConstants();
    InformationCenterAppEngine appEngine = new InformationCenterAppEngine(informationCenter());
    appEngine.setContext(appContext());
    appEngine.setHealthChecker(healthChecker());
    appEngine.setCheckMasterHealthChecker(checkMasterHealthChecker());
    appEngine.setDihClientBuilder(dihClientBuilder());
    appEngine.setDriverStoreSweeperExecutor(driverStoreSweeperExecutor());
    appEngine.setVolumeSweeperExecutor(volumeSweeperExecutor());
    appEngine.setVolumeActionSweeperExecutor(volumeActionSweeperExecutor());
    appEngine.setTimeoutSweeperExecutor(timeoutSweeperExecutor());
    appEngine.setHaInstanceCheckSweeperExecutor(haInstanceCheckSweeperExecutor());
    appEngine.setHaInstanceMoveVolumeSweeperExecutor(haInstanceMoveVolumeSweeperExecutor());

    appEngine.setInstanceMetadataStoreSweeperExecutor(instanceMetadataStoreSweeperExecutor());
    appEngine.setServerNodeAlertCheckerExecutor(serverNodeAlertCheckerExecutor());
    appEngine.setReportVolumeInfoSweeperExecutor(reportVolumeInfoSweeperExecutor());
    appEngine.setInformationCenterAppConfig(this);
    appEngine.setMaxNetworkFrameSize(getMaxNetworkFrameSize());
    appEngine.setMigrationRuleStore(migrationRuleStore());
    appEngine.setRecoverDbSentryStore(recoverDbSentryStore());
    appEngine.setDefaultMigrationRuleSet(defaultMigrationRuleSet);
    appEngine.setCreateSegmentUnitWorkThread(createSegmentUnitWorkThread());
    appEngine.setSecurityManager(securityManager());
    appEngine.setInstanceStore(instanceStore());

    appEngine.setRebalanceTaskExecutor(rebalanceTaskExecutor());
    appEngine.setStartRebalance(rebalanceStartByDefault);
    appEngine.setDataBaseTaskFactory(dataBaseTaskEngineFactory());
    appEngine.setGetRebalanceSweeperExecutor(getRebalanceTaskSweeperExecutor());
    appEngine.setVolumeDeleteDelayCheckerExecutor(volumeDeleteDelayCheckerExecutor());
    appEngine.setDriverClientManagerSweeperExecutor(driverClientManagerSweeperExecutor());
    appEngine.setServerStatusCheckExecutor(serverStatusCheckExecutor());

    appEngine.setInfoCenterAppContext(appContext());
    appEngine.setLockDirectory(lockDirectory);
    appEngine.setZookeeperConnectionString(zookeeperConnectionString);
    appEngine.setZookeeperSessionTimeout(zookeeperSessionTimeout);
    return appEngine;
  }



  @Bean
  public JmxAgent jmxAgent() {
    JmxAgent jmxAgent = JmxAgent.getInstance();
    jmxAgent.setAppContext(appContext());
    return jmxAgent;
  }



  @Bean
  public InformationCenterImpl informationCenter() throws Exception {
    InformationCenterImpl informationCenter = new InformationCenterImpl();
    informationCenter.setAppContext(appContext());
    informationCenter.setVolumeStore(twoLevelVolumeStore());
    informationCenter.setVolumeStatusStore(volumeStatusTransitionStore());
    informationCenter.setSegmentUnitTimeoutStore(segmentUnitTimeoutStore());
    informationCenter.setDriverStore(driverStore());
    informationCenter.setStorageStore(storageStore());
    informationCenter.setInstanceStore(instanceStore());
    informationCenter.setAccessRuleStore(accessRuleStore());
    informationCenter.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore());
    informationCenter.setIscsiAccessRuleStore(iscsiAccessRuleStore());
    informationCenter.setIscsiRuleRelationshipStore(iscsiRuleRelationshipStore());
    informationCenter.setServerNodeStore(serverNodeStore());
    informationCenter.setDiskInfoStore(diskInfoStore());
    informationCenter.setDomainStore(domainStore());
    informationCenter.setStoragePoolStore(storagePoolStore());
    informationCenter.setDriverContainerSelectionStrategy(driverContainerSelectionStrategy());
    informationCenter.setSegmentSize(storageConfiguration.getSegmentSizeByte());
    informationCenter.setPageSize(storageConfiguration.getPageSizeByte());
    informationCenter.setDeadVolumeToRemove(deadVolumeToRemove);
    informationCenter.setGroupCount(groupCount);
    informationCenter.setSelectionStrategy(selectionStrategy());
    informationCenter.setOrphanVolumes(orphanVolumeStore());
    informationCenter.setAccountStore(inMemAccountStore());
    informationCenter.setRoleStore(roleStore());
    informationCenter.setResourceStore(resourceStore());
    informationCenter.setApiStore(apiStore());
    informationCenter.setCoordinatorClientFactory(coordinatorClientFactory());
    informationCenter.setNextActionTimeIntervalMs(nextActionTimeIntervalMs);
    informationCenter.setCapacityRecordStore(capacityRecordStore());
    informationCenter.setNetworkConfiguration(networkConfiguration);
    informationCenter.setIoLimitationStore(ioLimitationStore());
    informationCenter.setMigrationRuleStore(migrationRuleStore());
    informationCenter.setRebalanceRuleStore(rebalanceRuleStore());

    informationCenter.setVolumeDelayStore(volumeDelayStore());
    informationCenter.setVolumeRecycleStore(volumeRecycleStore());
    informationCenter.setVolumeJobStoreDb(volumeJobStoreDb());
    informationCenter.setRecoverDbSentryStore(recoverDbSentryStore());

    informationCenter.initBackupDbManager(roundTimeInterval, maxBackupCount);
    informationCenter.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager());
    informationCenter.setGetRebalanceTaskSweeper(
        (GetRebalanceTaskSweeper) getRebalanceTaskSweeperFactory().createWorker());
    informationCenter.setPageWrappCount(pageWrappCount);
    informationCenter.setSegmentWrappCount(segmentWrappCount);
    informationCenter.setInstanceMaintenanceDbStore(instanceMaintenanceStore());
    informationCenter.setWaitCollectVolumeInfoSecond(waitCollectVolumeInfoSecond);
    informationCenter.setInfoCenterConfiguration(infoCenterConfiguration);

    appContext().setServerNodeReportTimeMap(informationCenter.getServerNodeReportTimeMap());

    informationCenter.setDriverContainerClientFactory(driverContainerClientFactory());
    informationCenter.setDataNodeClientFactory(dataNodeClientFactory());
    informationCenter.setTimeout(thriftClientTimeout);
    informationCenter.setOperationStore(operationStore());
    informationCenter.setInstanceMaintenanceStore(instanceMaintenanceStore());
    informationCenter.setSecurityManager(securityManager());
    informationCenter.setCreateSegmentUnitWorkThread(createSegmentUnitWorkThread());
    informationCenter.setVolumeInformationManger(volumeInformationManger());
    informationCenter.setCreateVolumeManager(createVolumeManager());
    informationCenter.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    informationCenter.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger());
    informationCenter.setExceptionForOperation(exceptionForOperation());
    informationCenter.setLockForSaveVolumeInfo(lockForSaveVolumeInfo());
    informationCenter.setReportVolumeManager(reportVolumeManager());
    informationCenter.setServerStatusCheck(serverStatusCheck());

    informationCenter.setScsiClientStore(scsiClientStore());
    informationCenter.setTaskStore(taskStore());
    informationCenter.setScsiDriverStore(scsiDriverStore());
    informationCenter.setVolumeRecycleManager(volumeRecycleManager());
    informationCenter.setDriverClientManger(driverClientManger());

    if (zookeeperElectionSwitch) {
      appContext().setStatus(InstanceStatus.SUSPEND);
    } else {
      appContext().setStatus(InstanceStatus.HEALTHY);
    }

    return informationCenter;
  }



  @Bean
  public ReserveInformation reserveInformation() throws Exception {
    ReserveInformation reserveInformation = new ReserveInformation();
    reserveInformation.setStorageStore(storageStore());
    reserveInformation.setVolumeStore(twoLevelVolumeStore());
    reserveInformation.setSegmentUnitsDistributionManager(segmentUnitsDistributionManager());
    reserveInformation.setSegmentWrappCount(segmentWrappCount);
    return reserveInformation;
  }

  @Bean
  public InstanceSelectionStrategy instanceSelectionStrategy() {
    return new FrugalInstanceSelectionStrategy();
  }



  @Bean
  public HealthChecker healthChecker() {
    HealthCheckerWithThriftImpl healthChecker = new HealthCheckerWithThriftImpl(appContext());
    healthChecker.setCheckingRate(healthCheckerRate);
    healthChecker.setServiceClientClazz(py.thrift.infocenter.service.InformationCenter.Iface.class);
    healthChecker.setHeartBeatWorkerFactory(heartBeatWorkerFactory());
    return healthChecker;
  }



  @Bean
  public HeartBeatWorkerFactory heartBeatWorkerFactory() {
    HeartBeatWorkerFactory factory = new HeartBeatWorkerFactory();
    factory.setRequestTimeout(thriftClientTimeout);
    factory.setAppContext(appContext());
    factory.setLocalDihEndPoint(localDihEp());
    factory.setDihClientFactory(dihClientFactory());
    return factory;
  }



  @Bean
  public HealthChecker checkMasterHealthChecker() throws Exception {
    CheckMasterHealthCheckerImpl healthChecker = new CheckMasterHealthCheckerImpl(healthCheckerRate,
        checkMasterHeartBeatWorkerFactory());
    return healthChecker;
  }



  @Bean
  public CheckMasterHeartBeatWorkerFactory checkMasterHeartBeatWorkerFactory() throws Exception {
    CheckMasterHeartBeatWorkerFactory factory = new CheckMasterHeartBeatWorkerFactory();
    factory.setInstanceStore(instanceStore());
    factory.setAppContext(appContext());
    return factory;
  }

  @Bean
  public DihClientBuildWorkerFactory dihClientBuildWorkerFactory() {
    DihClientBuildWorkerFactory dihClientBuildWorkerFactory = new DihClientBuildWorkerFactory();
    dihClientBuildWorkerFactory.setDihClientFactory(dihClientFactory());
    dihClientBuildWorkerFactory.setLocalDihEndPoint(localDihEp());
    dihClientBuildWorkerFactory.setRequestTimeout(thriftClientTimeout);
    return dihClientBuildWorkerFactory;
  }

  @Bean
  public DihClientBuilder dihClientBuilder() {
    DihClientBuilderImpl dihClientBuilder = new DihClientBuilderImpl();
    dihClientBuilder.setDihClientBuildWorkerFactory(dihClientBuildWorkerFactory());
    return dihClientBuilder;
  }



  @Bean
  public DriverContainerSelectionStrategy driverContainerSelectionStrategy() {
    BalancedDriverContainerSelectionStrategy balancedDriverContainerSelectionStrategy =
        new BalancedDriverContainerSelectionStrategy();
    return balancedDriverContainerSelectionStrategy;
  }



  @Bean
  public InstanceStore instanceStore() throws Exception {
    Object instanceStore = DihInstanceStore.getSingleton();
    ((DihInstanceStore) instanceStore).setDihClientFactory(dihClientFactory());
    ((DihInstanceStore) instanceStore).setDihEndPoint(localDihEp());
    ((DihInstanceStore) instanceStore).init();
    return (InstanceStore) instanceStore;
  }



  @Bean
  public ScsiClientStore scsiClientStore() {
    ScsiClientStoreImpl scsiClientStore = new ScsiClientStoreImpl();
    scsiClientStore.setSessionFactory(sessionFactory);
    return scsiClientStore;
  }



  @Bean
  public TaskStore taskStore() {
    TaskStoreImpl taskStore = new TaskStoreImpl();
    taskStore.setSessionFactory(sessionFactory);
    return taskStore;
  }



  @Bean
  public ScsiDriverStore scsiDriverStore() {
    ScsiDriverStoreImpl scsiDriverStore = new ScsiDriverStoreImpl();
    scsiDriverStore.setSessionFactory(sessionFactory);
    return scsiDriverStore;
  }



  @Bean
  public SelectionStrategy selectionStrategy() {
    RandomSelectionStrategy selectionStrategy = new RandomSelectionStrategy();
    selectionStrategy.setFactor(factorForSelector);
    selectionStrategy.setRemainder(remainderForSelector);

    return selectionStrategy;
  }

  @Bean
  public EndPoint localDihEp() {
    return EndPointParser
        .parseLocalEndPoint(dihEndPoint, appContext().getMainEndPoint().getHostName());
  }

  @Bean
  public ExceptionForOperation exceptionForOperation() {
    return new ExceptionForOperation();
  }

  @Bean
  public LockForSaveVolumeInfo lockForSaveVolumeInfo() {
    LockForSaveVolumeInfo lockForSaveVolumeInfo = new LockForSaveVolumeInfo();
    return lockForSaveVolumeInfo;
  }



  @Bean
  public ServerStatusCheck serverStatusCheck() {
    ServerStatusCheck serverStatusCheck = new ServerStatusCheck(zookeeperConnectionString);
    return serverStatusCheck;
  }

  /**  memory store. *****/
  @Bean
  public VolumeStatusTransitionStore volumeStatusTransitionStore() {
    return new VolumeStatusTransitionStoreImpl();
  }

  @Bean
  public SegmentUnitTimeoutStore segmentUnitTimeoutStore() {
    return new SegmentUnitTimeoutStoreImpl(this.segmentUnitReportTimeout);
  }

  /**
   * store.
   */
  @Bean
  public InfoCenterStoreSet infoCenterStoreSet() {
    InfoCenterStoreSet infoCenterStoreSet = new InfoCenterStoreSet();

    infoCenterStoreSet.setDomainStore(domainStore());
    infoCenterStoreSet.setDriverStore(driverStore());
    infoCenterStoreSet.setStoragePoolStore(storagePoolStore());
    infoCenterStoreSet.setStorageStore(storageStore());
    infoCenterStoreSet.setVolumeStore(twoLevelVolumeStore());

    return infoCenterStoreSet;
  }



  @Bean
  public OrphanVolumeStoreImpl orphanVolumeStore() {
    OrphanVolumeStoreImpl orphanVolumeStore = new OrphanVolumeStoreImpl();
    orphanVolumeStore.setVolumeToBeOrphanTime(volumeToBeOrphanTime);
    return orphanVolumeStore;
  }



  @Bean
  public DriverStore driverStore() {
    DriverStoreImpl driverStore = new DriverStoreImpl();
    driverStore.setSessionFactory(sessionFactory);
    return driverStore;
  }


  @Bean
  public DomainStore domainStore() {
    DomainStoreImpl domainStore = new DomainStoreImpl();
    domainStore.setSessionFactory(sessionFactory);
    return domainStore;
  }



  @Bean
  public StoragePoolStore storagePoolStore() {
    StoragePoolStoreImpl storagePoolStore = new StoragePoolStoreImpl();
    storagePoolStore.setSessionFactory(sessionFactory);
    return storagePoolStore;
  }



  @Bean
  public CapacityRecordStore capacityRecordStore() {
    CapacityRecordStoreImpl capacityRecordStore = new CapacityRecordStoreImpl();
    capacityRecordStore.setSessionFactory(sessionFactory);
    return capacityRecordStore;
  }



  @Bean
  public VolumeRuleRelationshipStore volumeRuleRelationshipStore() {
    VolumeRuleRelationshipStoreImpl volumeRuleRelationshipStore =
            new VolumeRuleRelationshipStoreImpl();
    volumeRuleRelationshipStore.setSessionFactory(sessionFactory);
    return volumeRuleRelationshipStore;
  }



  @Bean
  public AccessRuleStore accessRuleStore() {
    AccessRuleStoreImpl accessRuleStore = new AccessRuleStoreImpl();
    accessRuleStore.setSessionFactory(sessionFactory);
    return accessRuleStore;
  }



  @Bean
  public IscsiRuleRelationshipStore iscsiRuleRelationshipStore() {
    IscsiRuleRelationshipStoreImpl iscsiRuleRelationshipStore =
            new IscsiRuleRelationshipStoreImpl();
    iscsiRuleRelationshipStore.setSessionFactory(sessionFactory);
    return iscsiRuleRelationshipStore;
  }



  @Bean
  public IscsiAccessRuleStore iscsiAccessRuleStore() {
    IscsiAccessRuleStoreImpl iscsiAccessRuleStore = new IscsiAccessRuleStoreImpl();
    iscsiAccessRuleStore.setSessionFactory(sessionFactory);
    return iscsiAccessRuleStore;
  }



  @Bean
  public IoLimitationStore ioLimitationStore() {
    IoLimitationStoreImpl ioLimitationStore = new IoLimitationStoreImpl();
    ioLimitationStore.setSessionFactory(sessionFactory);
    return ioLimitationStore;
  }



  @Bean
  public MigrationRuleStore migrationRuleStore() {
    MigrationRuleStoreImpl migrationRuleStore = new MigrationRuleStoreImpl();
    migrationRuleStore.setSessionFactory(sessionFactory);
    return migrationRuleStore;
  }



  @Bean
  public RebalanceRuleStore rebalanceRuleStore() {
    RebalanceRuleStoreImpl rebalanceRuleStore = new RebalanceRuleStoreImpl();
    rebalanceRuleStore.setSessionFactory(sessionFactory);
    return rebalanceRuleStore;
  }



  @Bean
  public ServerNodeStore serverNodeStore() {
    ServerNodeStoreImpl serverNodeStore = new ServerNodeStoreImpl();
    serverNodeStore.setSessionFactory(sessionFactory);
    return serverNodeStore;
  }



  @Bean
  public DiskInfoStore diskInfoStore() {
    DiskInfoStoreImpl diskInfoStore = new DiskInfoStoreImpl();
    diskInfoStore.setSessionFactory(sessionFactory);
    return diskInfoStore;
  }



  @Bean
  public StorageStore storageStore() {
    StorageStoreImpl storageStoreImpl = new StorageStoreImpl();
    storageStoreImpl.setSessionFactory(sessionFactory);
    storageStoreImpl.setArchiveStore(dbArchiveStore());
    return storageStoreImpl;
  }

  @Bean
  InstanceVolumesInformationStore instanceVolumesInformationStore() {
    InstanceVolumesInformationStoreImpl instanceVolumesInformationStore =
            new InstanceVolumesInformationStoreImpl();
    instanceVolumesInformationStore.setSessionFactory(sessionFactory);
    return instanceVolumesInformationStore;
  }



  @Bean
  public InfoCenterAppContext appContext() {
    InfoCenterAppContext appContext = new InfoCenterAppContext(appName);

    EndPoint endpointOfControlStream = EndPointParser.parseInSubnet(mainEndPoint,
        networkConfiguration.getControlFlowSubnet());
    EndPoint endpointOfMonitorStream = EndPointParser
        .parseInSubnet(jmxAgentConfiguration.getJmxAgentPort(),
            networkConfiguration.getMonitorFlowSubnet());
    appContext.putEndPoint(PortType.CONTROL, endpointOfControlStream);
    appContext.putEndPoint(PortType.MONITOR, endpointOfMonitorStream);

    appContext.setLocation(appLocation);
    appContext.setInstanceIdStore(
        new InstanceIdFileStore(appName, appName, endpointOfControlStream.getPort()));
    appContext.setStoreSet(infoCenterStoreSet());
    appContext.setServerNodeStore(serverNodeStore());
    try {
      appContext.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger());
      appContext.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
      appContext.setOperationStore(operationStore());
      appContext.setDriverClientStore(driverClientStore());
      appContext.setInMemoryAccountStore(inMemAccountStoreImpl());
    } catch (Exception e) {
      logger.warn("when appContext, find exception:", e);
    }
    return appContext;
  }



  @Bean
  public ArchiveStore dbArchiveStore() {
    ArchiveStoreImpl archiveStoreImpl = new ArchiveStoreImpl();
    archiveStoreImpl.setSessionFactory(sessionFactory);
    return archiveStoreImpl;
  }



  @Bean
  public VolumeStore dbVolumeStore() {
    DbVolumeStoreImpl volumeStoreImpl = new DbVolumeStoreImpl();
    volumeStoreImpl.setSessionFactory(sessionFactory);
    return volumeStoreImpl;
  }



  @Bean
  public VolumeDelayStore volumeDelayStore() {
    VolumeDelayStoreImpl volumeDelayStore = new VolumeDelayStoreImpl();
    volumeDelayStore.setSessionFactory(sessionFactory);
    return volumeDelayStore;
  }



  @Bean
  public VolumeRecycleStore volumeRecycleStore() {
    VolumeRecycleStoreImpl volumeRecycleStore = new VolumeRecycleStoreImpl();
    volumeRecycleStore.setSessionFactory(sessionFactory);
    return volumeRecycleStore;
  }



  @Bean
  public SegmentStore dbSegmentStore() {
    SegmentStoreImpl segmentStore = new SegmentStoreImpl();
    segmentStore.setSessionFactory(sessionFactory);
    return segmentStore;
  }



  @Bean
  public AccountStore inMemAccountStore() {
    return inMemAccountStoreImpl();
  }



  @Bean
  public InMemoryAccountStoreImpl inMemAccountStoreImpl() {
    InMemoryAccountStoreImpl inMemoryAccountStore = new InMemoryAccountStoreImpl();
    inMemoryAccountStore.setAccountStore(dbAccountStore());
    return inMemoryAccountStore;
  }



  @Bean
  public AccountStore dbAccountStore() {
    AccountStoreDbImpl accountStore = new AccountStoreDbImpl();
    accountStore.setSessionFactory(sessionFactory);
    return accountStore;
  }



  @Bean
  public RoleStore roleStore() {
    RoleDbStoreImpl roleDbStore = new RoleDbStoreImpl();
    roleDbStore.setSessionFactory(sessionFactory);
    return roleDbStore;
  }



  @Bean
  public ApiStore apiStore() {
    ApiDbStoreImpl apiStore = new ApiDbStoreImpl();
    apiStore.setSessionFactory(sessionFactory);
    return apiStore;
  }



  @Bean
  public ResourceStore resourceStore() {
    ResourceDbStoreImpl resourceDbStore = new ResourceDbStoreImpl();
    resourceDbStore.setSessionFactory(sessionFactory);
    return resourceDbStore;
  }



  @Bean
  public InstanceMaintenanceDbStore instanceMaintenanceStore() {
    InstanceMaintenanceStoreImpl instanceMaintenanceStore = new InstanceMaintenanceStoreImpl();
    instanceMaintenanceStore.setSessionFactory(sessionFactory);
    return instanceMaintenanceStore;
  }

  @Bean
  public RecoverDbSentryStoreImpl recoverDbSentryStore() {
    RecoverDbSentryStoreImpl instanceMaintenanceStore = new RecoverDbSentryStoreImpl();
    instanceMaintenanceStore.setSessionFactory(sessionFactory);
    return instanceMaintenanceStore;
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
  public VolumeJobStoreDb volumeJobStoreDb() throws Exception {
    VolumeJobStoreImpl volumeJobStoreImpl = new VolumeJobStoreImpl();
    volumeJobStoreImpl.setSessionFactory(sessionFactory);
    return volumeJobStoreImpl;
  }


  @Bean
  public OperationStore operationStore() throws Exception {
    OperationStoreImpl operationStore = new OperationStoreImpl();
    operationStore.setSessionFactory(sessionFactory);
    operationStore.setSaveOperationDays(saveOperationDays);
    return operationStore;
  }


  @Bean
  public DriverClientStore driverClientStore() throws Exception {
    DriverClientStoreImpl driverClientStore = new DriverClientStoreImpl();
    driverClientStore.setSessionFactory(sessionFactory);
    return driverClientStore;



  }

  /********** Manager. *****************************/
  @Bean
  public SegmentUnitsDistributionManager segmentUnitsDistributionManager() throws Exception {
    SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager =
        new SegmentUnitsDistributionManagerImpl(
            storageConfiguration.getSegmentSizeByte(), twoLevelVolumeStore(), storageStore(),
            storagePoolStore(), rebalanceRuleStore(), domainStore());
    segmentUnitsDistributionManager.setSegmentWrappCount(segmentWrappCount);
    segmentUnitsDistributionManager.setVolumeInformationManger(volumeInformationManger());
    segmentUnitsDistributionManager.setAppContext(appContext());
    segmentUnitsDistributionManager.setInstanceStore(instanceStore());
    return segmentUnitsDistributionManager;
  }


  @Bean
  public PySecurityManager securityManager() {
    PySecurityManager securityManager = new PySecurityManager(apiSet);
    securityManager.setBuiltInRoles(builtInRoles);
    securityManager.setRoleStore(roleStore());
    securityManager.setAccountStore(inMemAccountStore());
    securityManager.setApiStore(apiStore());
    securityManager.setResourceStore(resourceStore());
    return securityManager;
  }

  @Bean
  public CreateVolumeManager createVolumeManager() throws Exception {

    CreateVolumeManager createVolumeManager = new CreateVolumeManager();
    createVolumeManager.setInfoCenterConfiguration(infoCenterConfiguration);
    createVolumeManager.setPasselCount(passelCount);
    createVolumeManager.setDataNodeClientFactory(dataNodeClientFactory());
    createVolumeManager.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory());
    createVolumeManager.setCreateVolumeTimeOutSecond(createVolumeTimeOut);
    createVolumeManager.setThriftTimeOut(thriftClientTimeout);
    createVolumeManager
        .setCreateSegmentUnitThreadPoolCoreSize(this.createSegmentUnitThreadPoolCoreSize);
    createVolumeManager
        .setCreateSegmentUnitThreadPoolMaxSize(this.createSegmentUnitThreadPoolMaxSize);

    createVolumeManager.setInstanceStore(instanceStore());
    createVolumeManager.setAccountStore(inMemAccountStore());
    createVolumeManager.setReserveInformation(reserveInformation());
    createVolumeManager.setVolumeInformationManger(volumeInformationManger());
    createVolumeManager.setVolumeStore(twoLevelVolumeStore());

    createVolumeManager.init();
    return createVolumeManager;
  }

  @Bean
  public HaInstanceManger haInstanceManger() throws Exception {
    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore());
    return haInstanceManger;
  }



  @Bean
  public VolumeInformationManger volumeInformationManger() throws Exception {
    VolumeInformationManger volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setInfoCenterClientFactory(informationCenterClientFactory());
    volumeInformationManger.setVolumeStore(twoLevelVolumeStore());
    volumeInformationManger.setDriverStore(driverStore());
    volumeInformationManger.setHaInstanceManger(haInstanceManger());
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    volumeInformationManger.setAppContext(appContext());
    volumeInformationManger.setAllSegmentNumberToSave(allSegmentNumberToSave);
    volumeInformationManger
        .setPercentNumberSegmentNumberMasterSave(percentNumberSegmentNumberMasterSave);

    return volumeInformationManger;
  }



  @Bean
  public ReportVolumeManager reportVolumeManager() throws Exception {
    ReportVolumeManager reportVolumeManager = new ReportVolumeManager();
    reportVolumeManager.setVolumeStore(twoLevelVolumeStore());
    reportVolumeManager.setStoragePoolStore(storagePoolStore());
    reportVolumeManager.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    reportVolumeManager.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger());
    reportVolumeManager.setLockForSaveVolumeInfo(lockForSaveVolumeInfo());
    reportVolumeManager.setVolumeRecycleStore(volumeRecycleStore());

    return reportVolumeManager;
  }



  @Bean
  public VolumeRecycleManager volumeRecycleManager() throws Exception {
    VolumeRecycleManager volumeRecycleManager = new VolumeRecycleManager();
    volumeRecycleManager.setVolumeStore(twoLevelVolumeStore());
    volumeRecycleManager.setDriverStore(driverStore());
    volumeRecycleManager.setVolumeRuleRelationshipStore(volumeRuleRelationshipStore());
    volumeRecycleManager.setExceptionForOperation(exceptionForOperation());
    volumeRecycleManager.setLockForSaveVolumeInfo(lockForSaveVolumeInfo());
    volumeRecycleManager.setVolumeRecycleStore(volumeRecycleStore());

    return volumeRecycleManager;
  }



  @Bean
  public InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger() throws Exception {
    InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger =
        new InstanceIncludeVolumeInfoManger();
    instanceIncludeVolumeInfoManger
        .setInstanceVolumesInformationStore(instanceVolumesInformationStore());
    instanceIncludeVolumeInfoManger.setVolumeStore(twoLevelVolumeStore());

    return instanceIncludeVolumeInfoManger;
  }



  @Bean
  public InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger() throws Exception {
    InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger =
        new InstanceVolumeInEquilibriumManger();
    instanceVolumeInEquilibriumManger
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    instanceVolumeInEquilibriumManger.setAllSegmentNumberToSave(allSegmentNumberToSave);
    instanceVolumeInEquilibriumManger
        .setPercentNumberSegmentNumberMasterSave(percentNumberSegmentNumberMasterSave);
    instanceVolumeInEquilibriumManger.setSegmentSize(storageConfiguration.getSegmentSizeByte());
    return instanceVolumeInEquilibriumManger;
  }



  @Bean
  public DriverClientManger driverClientManger() throws Exception {
    DriverClientManger driverClientManger = new DriverClientManger(driverClientStore(),
        driverClientInfoKeepTime, driverClientInfoKeepNumber);
    return driverClientManger;
  }

  /***************** thread.  *****************/
  @Bean
  public DriverStoreSweeperFactory driverStoreSweepWorkerFactory() {
    DriverStoreSweeperFactory driverStoreSweepWorkerFactory = new DriverStoreSweeperFactory();
    driverStoreSweepWorkerFactory.setDriverStore(driverStore());
    driverStoreSweepWorkerFactory.setDriverReportTimeout(driverReportTimeout);
    driverStoreSweepWorkerFactory.setDomainStore(domainStore());
    driverStoreSweepWorkerFactory.setAppContext(appContext());
    driverStoreSweepWorkerFactory.setScsiDriverStore(scsiDriverStore());
    driverStoreSweepWorkerFactory.setVolumeStore(twoLevelVolumeStore());
    return driverStoreSweepWorkerFactory;
  }


  @Bean
  public VolumeSweeperFactory volumeSweeperFactory() throws Exception {
    VolumeSweeperFactory volumeSweeperFactory = new VolumeSweeperFactory();
    volumeSweeperFactory.setVolumeStore(twoLevelVolumeStore());
    volumeSweeperFactory.setVolumeStatusTransitionStore(volumeStatusTransitionStore());
    volumeSweeperFactory.setTimeout(thriftClientTimeout);
    volumeSweeperFactory.setInstanceStore(instanceStore());
    volumeSweeperFactory.setDataNodeClientFactory(dataNodeClientFactory());
    volumeSweeperFactory.setDeadVolumeToRemoveTime(deadVolumeToRemove);
    volumeSweeperFactory.setVolumeStatusTransitionStore(volumeStatusTransitionStore());
    volumeSweeperFactory.setAppContext(appContext());
    volumeSweeperFactory.setVolumeJobStoreDb(volumeJobStoreDb());
    volumeSweeperFactory.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    volumeSweeperFactory.setVolumeRecycleStore(volumeRecycleStore());
    volumeSweeperFactory.setVolumeSweeperRate(volumeSweeperRate);
    volumeSweeperFactory.setOperationStore(operationStore());
    volumeSweeperFactory.setSecurityManager(securityManager());
    return volumeSweeperFactory;
  }



  @Bean
  public VolumeActionSweeperFactory volumeActionSweeperFactory() throws Exception {
    VolumeActionSweeperFactory volumeActionSweeperFactory = new VolumeActionSweeperFactory();
    volumeActionSweeperFactory.setVolumeStore(twoLevelVolumeStore());
    volumeActionSweeperFactory.setTimeout(thriftClientTimeout);
    volumeActionSweeperFactory.setInstanceStore(instanceStore());
    volumeActionSweeperFactory.setDataNodeClientFactory(dataNodeClientFactory());
    volumeActionSweeperFactory.setAppContext(appContext());
    volumeActionSweeperFactory.setStoragePoolStore(storagePoolStore());
    volumeActionSweeperFactory.setLockForSaveVolumeInfo(lockForSaveVolumeInfo());
    volumeActionSweeperFactory
        .setSegmentUnitsDistributionManager(segmentUnitsDistributionManager());
    volumeActionSweeperFactory.setVolumeInformationManger(volumeInformationManger());
    volumeActionSweeperFactory.setOperationStore(operationStore());
    volumeActionSweeperFactory.setSecurityManager(securityManager());
    return volumeActionSweeperFactory;
  }



  @Bean
  public ReportVolumeInfoSweeperFactory reportVolumeInfoSweeperFactory() throws Exception {
    ReportVolumeInfoSweeperFactory reportVolumeInfoSweeperFactory =
        new ReportVolumeInfoSweeperFactory();
    reportVolumeInfoSweeperFactory.setVolumeStore(twoLevelVolumeStore());
    reportVolumeInfoSweeperFactory.setAppContext(appContext());
    reportVolumeInfoSweeperFactory.setInfoCenterClientFactory(informationCenterClientFactory());
    reportVolumeInfoSweeperFactory.setVolumeInformationManger(volumeInformationManger());
    reportVolumeInfoSweeperFactory
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    reportVolumeInfoSweeperFactory.setReportVolumeManager(reportVolumeManager());
    return reportVolumeInfoSweeperFactory;
  }



  @Bean
  public TimeoutSweeperFactory timeoutSweeperFactory() {
    TimeoutSweeperFactory timeoutSweeperFactory = new TimeoutSweeperFactory();
    timeoutSweeperFactory.setVolumeStatusStore(volumeStatusTransitionStore());
    timeoutSweeperFactory.setVolumeStore(twoLevelVolumeStore());
    timeoutSweeperFactory.setSegUnitTimeoutStore(segmentUnitTimeoutStore());
    timeoutSweeperFactory.setAppContext(appContext());
    timeoutSweeperFactory.setNextActionTimeIntervalMs((long) nextActionTimeIntervalMs);
    timeoutSweeperFactory.setDomainStore(domainStore());
    timeoutSweeperFactory.setStoragePoolStore(storagePoolStore());
    timeoutSweeperFactory.setCapacityRecordStore(capacityRecordStore());
    timeoutSweeperFactory.setStorageStore(storageStore());
    timeoutSweeperFactory.setTakeSampleInterValSecond(takeSampleInterValSecond);
    timeoutSweeperFactory.setStoreCapacityRecordCount(storeCapacityRecordCount);
    timeoutSweeperFactory.setRoundTimeInterval(roundTimeInterval);
    timeoutSweeperFactory.setServerStatusCheck(serverStatusCheck());
    return timeoutSweeperFactory;
  }



  @Bean
  public HaInstanceCheckSweeperFactory haInstanceCheckSweeperFactory() throws Exception {
    HaInstanceCheckSweeperFactory haInstanceCheckSweeperFactory =
        new HaInstanceCheckSweeperFactory();
    haInstanceCheckSweeperFactory.setVolumeInformationManger(volumeInformationManger());
    haInstanceCheckSweeperFactory.setInstanceTimeOutCheck(instanceTimeOutCheck);
    haInstanceCheckSweeperFactory
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger());
    haInstanceCheckSweeperFactory.setAppContext(appContext());
    haInstanceCheckSweeperFactory
        .setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger());
    return haInstanceCheckSweeperFactory;
  }



  @Bean
  public HaInstanceEquilibriumSweeperFactory haInstanceMoveVolumeSweeperFactory() throws Exception {
    HaInstanceEquilibriumSweeperFactory haInstanceMoveVolumeSweeperFactory =
        new HaInstanceEquilibriumSweeperFactory();
    haInstanceMoveVolumeSweeperFactory
        .setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger());
    haInstanceMoveVolumeSweeperFactory
        .setEnableInstanceEquilibriumVolume(enableInstanceEquilibriumVolume);
    haInstanceMoveVolumeSweeperFactory.setAppContext(appContext());
    return haInstanceMoveVolumeSweeperFactory;
  }


  @Bean
  public ServerNodeAlertCheckerFactory serverNodeAlertCheckerFactory() throws Exception {
    ServerNodeAlertCheckerFactory serverNodeAlertCheckerFactory =
        new ServerNodeAlertCheckerFactory();
    serverNodeAlertCheckerFactory.setInstanceStore(instanceStore());
    serverNodeAlertCheckerFactory
        .setServerNodeReportTimeMap(informationCenter().getServerNodeReportTimeMap());
    serverNodeAlertCheckerFactory.setServerNodeReportOverTimeSecond(serverNodeReportOverTimeSecond);
    serverNodeAlertCheckerFactory.setServerNodeStore(serverNodeStore());
    serverNodeAlertCheckerFactory.setInstanceMaintenanceDbStore(instanceMaintenanceStore());
    serverNodeAlertCheckerFactory.setAppContext(appContext());
    return serverNodeAlertCheckerFactory;
  }



  @Bean
  public ServerStatusCheckFactory serverStatusCheckFactory() throws Exception {
    ServerStatusCheckFactory serverStatusCheckFactory = new ServerStatusCheckFactory();
    serverStatusCheckFactory.setAppContext(appContext());
    serverStatusCheckFactory.setServerStatusCheck(serverStatusCheck());
    return serverStatusCheckFactory;
  }



  @Bean
  public StorageStoreSweeperFactory instanceMetadataStoreSweeperFactory() {
    StorageStoreSweeperFactory factory = new StorageStoreSweeperFactory();
    factory.setStorageStore(storageStore());
    factory.setStoragePoolStore(storagePoolStore());
    factory.setDomainStore(domainStore());
    factory.setVolumeStore(twoLevelVolumeStore());
    factory.setSegmentSize(storageConfiguration.getSegmentSizeByte());
    factory.setTimeToRemove(instanceMetadataTimeToRemove);
    factory.setAppContext(appContext());
    factory.setInstanceMaintenanceDbStore(instanceMaintenanceStore());
    factory.setWaitCollectVolumeInfoSecond(waitCollectVolumeInfoSecond);
    return factory;
  }



  @Bean
  public VolumeDeleteDelayCheckerFactory volumeDeleteDelayCheckerFactory() throws Exception {
    VolumeDeleteDelayCheckerFactory volumeDeleteDelayCheckerFactory =
        new VolumeDeleteDelayCheckerFactory();
    volumeDeleteDelayCheckerFactory.setVolumeDelayStore(volumeDelayStore());
    volumeDeleteDelayCheckerFactory.setVolumeRecycleStore(volumeRecycleStore());
    volumeDeleteDelayCheckerFactory.setRecycleDeleteTimeSecond(volumeDeleteDelayRate);
    volumeDeleteDelayCheckerFactory.setAppContext(appContext());
    volumeDeleteDelayCheckerFactory.setVolumeRecycleManager(volumeRecycleManager());
    volumeDeleteDelayCheckerFactory.setInformationCenter(informationCenter());
    volumeDeleteDelayCheckerFactory.setRecycleKeepTimeSecond(volumeKeepRecycleTimeSecond);
    return volumeDeleteDelayCheckerFactory;
  }


  @Bean
  public DriverClientManagerSweeperFactory driverClientManagerSweeperFactory() throws Exception {
    DriverClientManagerSweeperFactory driverClientManagerSweeperFactory =
        new DriverClientManagerSweeperFactory();
    driverClientManagerSweeperFactory.setDriverClientManager(driverClientManger());
    driverClientManagerSweeperFactory.setAppContext(appContext());
    return driverClientManagerSweeperFactory;
  }


  @Bean
  public PeriodicWorkExecutorImpl driverClientManagerSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        driverClientManagerSweeperExecutionOptionsReader(),
        driverClientManagerSweeperFactory(), "Driver-client-manager");
    return sweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl driverStoreSweeperExecutor() throws UnableToStartException {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        driverSweeperExecutionOptionsReader(),
        driverStoreSweepWorkerFactory(), "Driver-Sweeper");
    return sweeperExecutor;
  }


  @Bean
  public PeriodicWorkExecutorImpl volumeSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        volumeSweeperExecutionOptionsReader(),
        volumeSweeperFactory(), "Volume-Sweeper");
    return sweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl volumeActionSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        volumeActionSweeperExecutionOptionsReader(),
        volumeActionSweeperFactory(), "Volume-Action-Sweeper");
    return sweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl timeoutSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl timeSweeperExecutor = new PeriodicWorkExecutorImpl(
        timeoutSweeperExecutionOptionsReader(), timeoutSweeperFactory(), "timeout-Sweeper");
    return timeSweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl haInstanceCheckSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl haInstanceCheckSweeperExecutor = new PeriodicWorkExecutorImpl(
        haInstanceCheckSweeperExecutionOptionsReader(), haInstanceCheckSweeperFactory(),
        "HaInstanceCheck-Sweeper");
    return haInstanceCheckSweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl haInstanceMoveVolumeSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl haInstanceMoveVolumeSweeperExecutor = new PeriodicWorkExecutorImpl(
        haInstanceMoveVolumeSweeperExecutionOptionsReader(), haInstanceMoveVolumeSweeperFactory(),
        "HaInstanceMoveVolume-Sweeper");
    return haInstanceMoveVolumeSweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl reportVolumeInfoSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl reportVolumeInfoSweeperExecutor = new PeriodicWorkExecutorImpl(
        reportVolumeInfoSweeperExecutionOptionsReader(), reportVolumeInfoSweeperFactory(),
        "reportVolumeInfo-Sweeper");
    return reportVolumeInfoSweeperExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl instanceMetadataStoreSweeperExecutor()
      throws UnableToStartException {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        instanceMetadataSweeperExecutionOptionsReader(), instanceMetadataStoreSweeperFactory(),
        "InstanceMetadata-Sweeper");
    return sweeperExecutor;
  }



  @Bean
  public GetRebalanceTaskSweeperFactory getRebalanceTaskSweeperFactory() throws Exception {
    GetRebalanceTaskSweeperFactory getRebalanceTaskSweeperFactory =
        new GetRebalanceTaskSweeperFactory(
            segmentUnitsDistributionManager(), volumeInformationManger(), twoLevelVolumeStore(),
            instanceStore(), this);
    return getRebalanceTaskSweeperFactory;
  }



  @Bean
  public PeriodicWorkExecutorImpl getRebalanceTaskSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        getRebalanceTaskSweeperExecutionOptionsReader(),
        getRebalanceTaskSweeperFactory(), "GetRebalanceTask-Sweeper");
    return sweeperExecutor;
  }

  @Bean
  public ExecutionOptionsReader getRebalanceTaskSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, null, getRebalanceTaskDelayRateMs);
  }



  @Bean
  public PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor() throws Exception {
    PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor = new PeriodicWorkExecutorImpl(
        serverNodeAlertCheckerExecutionOptionsReader(),
        serverNodeAlertCheckerFactory(), "ServerNode-Alert-Checker");
    return serverNodeAlertCheckerExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl volumeDeleteDelayCheckerExecutor() throws Exception {
    PeriodicWorkExecutorImpl volumeDeleteDelayCheckerExecutor = new PeriodicWorkExecutorImpl(
        volumeDeleteDelayCheckerExecutionOptionsReader(),
        volumeDeleteDelayCheckerFactory(), "Volume-Delete-Delay");
    return volumeDeleteDelayCheckerExecutor;
  }



  @Bean
  public CreateSegmentUnitWorkThread createSegmentUnitWorkThread() throws Exception {
    CreateSegmentUnitWorkThread createSegmentUnitWorkThread = new CreateSegmentUnitWorkThread(
        dataNodeClientFactory(),
        segmentUnitsDistributionManager(),
        storageConfiguration.getSegmentSizeByte()/* segmentSize */,
        thriftClientTimeout, instanceMaintenanceStore());
    return createSegmentUnitWorkThread;
  }


  @Bean
  public RebalanceTaskExecutor rebalanceTaskExecutor() throws Exception {
    RebalanceTaskExecutorImpl rebalanceTaskExecutor = new RebalanceTaskExecutorImpl(
        informationCenter(), appContext(), instanceStore(), dataNodeClientFactory(),
        rebalanceThreadPoolCoreSize, rebalanceThreadPoolMaxSize, 60, rebalanceTaskCount);
    return rebalanceTaskExecutor;
  }



  @Bean
  public PeriodicWorkExecutorImpl serverStatusCheckExecutor() throws Exception {
    PeriodicWorkExecutorImpl serverStatusCheckExecutor = new PeriodicWorkExecutorImpl(
        serverStatusCheckExecutionOptionsReader(),
        serverStatusCheckFactory(), "server-status-Checker");
    return serverStatusCheckExecutor;
  }



  @Bean
  public DataBaseTaskEngineFactory dataBaseTaskEngineFactory() throws Exception {
    DataBaseTaskEngineFactory dataBaseTaskEngineFactory = new DataBaseTaskEngineFactory(
        dbTaskCorePoolSize, dbTaskMaxPoolSize, dbTaskMaxConcurrentSize, taskStore(),
        appContext());
    return dataBaseTaskEngineFactory;
  }

  @Bean
  public ScsiDriverStatusUpdate scsiDriverStatusUpdate() {
    ScsiDriverStatusUpdate scsiDriverStatusUpdate = new ScsiDriverStatusUpdate();
    return scsiDriverStatusUpdate;
  }

  @Bean
  public ExecutionOptionsReader driverClientManagerSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, driverClientInfoSweeperRate, null);
  }

  @Bean
  public ExecutionOptionsReader driverSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, driverSweeperRate, null);
  }

  @Bean
  public ExecutionOptionsReader instanceMetadataSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1,
        OperationName.StoragePool.getEvnetDataGeneratingPeriod(), null);
  }

  @Bean
  public ExecutionOptionsReader volumeSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, volumeSweeperRate, null);
  }

  @Bean
  public ExecutionOptionsReader volumeActionSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, volumeSweeperRate, null);
  }

  @Bean
  public ExecutionOptionsReader timeoutSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, timeoutSweeper, null);
  }

  @Bean
  public ExecutionOptionsReader reportVolumeInfoSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, null, followReportDelay);
  }

  @Bean
  public ExecutionOptionsReader haInstanceCheckSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, 1000, null);
  }

  @Bean
  public ExecutionOptionsReader haInstanceMoveVolumeSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, instanceEquilibriumVolumeTime, null);
  }

  @Bean
  public ExecutionOptionsReader alarmSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, alarmSweeperRate, null);
  }

  @Bean
  public ExecutionOptionsReader serverStatusCheckExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, null, serverStatusCheckRate);
  }

  @Bean
  public ExecutionOptionsReader serverNodeAlertCheckerExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, serverNodeAlertCheckerRate, null);
  }

  @Bean
  public ExecutionOptionsReader volumeDeleteDelayCheckerExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, (int) TimeUnit.SECONDS.toMillis(volumeDeleteDelayRate),
        null);
  }

  /**
   * instance Factory.
   */
  @Bean
  public CoordinatorClientFactory coordinatorClientFactory() {
    CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(1);
    coordinatorClientFactory.getGenericClientFactory().setMaxNetworkFrameSize(maxNetworkFrameSize);
    return coordinatorClientFactory;
  }



  @Bean
  public GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory() {
    GenericThriftClientFactory<DataNodeService.Iface> genericThriftClientFactory =
        GenericThriftClientFactory
            .create(DataNodeService.Iface.class, 1);
    genericThriftClientFactory.setMaxNetworkFrameSize(maxNetworkFrameSize);
    return genericThriftClientFactory;
  }



  @Bean
  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    dihClientFactory.getGenericClientFactory().setMaxNetworkFrameSize(maxNetworkFrameSize);
    return dihClientFactory;
  }



  @Bean
  public InformationCenterClientFactory informationCenterClientFactory() throws Exception {
    InformationCenterClientFactory factory = new InformationCenterClientFactory(1);
    factory.setInstanceName(appName);
    factory.setInstanceStore(instanceStore());
    return factory;
  }

  @Bean
  public GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory() {
    return GenericThriftClientFactory.create(DataNodeService.AsyncIface.class, 1);
  }



  @Bean
  public DriverContainerClientFactory driverContainerClientFactory() throws Exception {
    DriverContainerClientFactory driverContainerClientFactory = new DriverContainerClientFactory(1);
    driverContainerClientFactory.setInstanceStore(instanceStore());
    return driverContainerClientFactory;
  }

  /**************** Process. *******/
  @Bean
  public VolumeJobProcess volumeJobProcess() throws Exception {
    VolumeJobProcess volumeStore = new VolumeJobProcess();
    volumeStore.setVolumeInformationManger(volumeInformationManger());
    volumeStore.setCreateVolumeManager(createVolumeManager());
    volumeStore.setVolumeJobStoredb(volumeJobStoreDb());
    volumeStore.setInformationCenter(informationCenter());
    volumeStore.setLockForSaveVolumeInfo(lockForSaveVolumeInfo());
    return volumeStore;
  }



  @Bean
  public VolumeProcessor volumeProcessor() throws Exception {
    VolumeProcessorImpl volumeProcessor = new VolumeProcessorImpl();
    volumeProcessor.setVolumeJobStore(volumeJobProcess());
    volumeProcessor.setAppContext(appContext());
    return volumeProcessor;
  }



  @Bean
  public UpdateOperationProcessor updateOperationProcessor() throws Exception {
    UpdateOperationProcessorImpl operationProcessor = new UpdateOperationProcessorImpl();
    operationProcessor.setOperationStore(operationStore());
    operationProcessor.setVolumeJobStoreDb(volumeJobStoreDb());
    operationProcessor.setInformationCenter(informationCenter());
    operationProcessor.setOperationPassTimeSecond(operationPasstimeSecond);
    operationProcessor.setRollbackPassTimeSecond(rollbackPassTimeSecond);
    operationProcessor.setInstanceStore(instanceStore());
    operationProcessor.setVolumeInformationManger(volumeInformationManger());
    operationProcessor.setAppContext(appContext());
    operationProcessor.setLockForSaveVolumeInfo(lockForSaveVolumeInfo());
    return operationProcessor;
  }

  /**
   * init info.
   */
  @Bean
  public CoordinatorConfigSingleton coordinatorConfiguration() {

    CoordinatorConfigSingleton coordinatorConfigSingleton = CoordinatorConfigSingleton
        .getInstance();
    int thriftTimeoutMs = 30 * 1000;
    coordinatorConfigSingleton.setThriftRequestTimeoutMs(thriftTimeoutMs);
    coordinatorConfigSingleton.setThriftConnectTimeoutMs(thriftTimeoutMs);
    int nettyTimeoutMs = 5 * 1000;
    coordinatorConfigSingleton.setNettyRequestTimeoutMs(nettyTimeoutMs);
    coordinatorConfigSingleton.setNettyConnectTimeoutMs(nettyTimeoutMs);
    coordinatorConfigSingleton.setWriteIoTimeoutMs(storageConfiguration.getIoTimeoutMs());
    coordinatorConfigSingleton.setReadIoTimeoutMs(storageConfiguration.getIoTimeoutMs());

    coordinatorConfigSingleton.setPageSize((int) storageConfiguration.getPageSizeByte());
    coordinatorConfigSingleton.setSegmentSize(storageConfiguration.getSegmentSizeByte());

    int maxDataSizePerRequest = 1048576;
    coordinatorConfigSingleton.setMaxWriteDataSizePerRequest(maxDataSizePerRequest);
    coordinatorConfigSingleton.setMaxReadDataSizePerRequest(maxDataSizePerRequest);
    coordinatorConfigSingleton.setPageWrappedCount(128);
    coordinatorConfigSingleton.setCommitLogsAfterNoWriteRequestMs(2000);
    coordinatorConfigSingleton.setPageCacheForRead(0);
    coordinatorConfigSingleton.setReadCacheForIo(0);
    coordinatorConfigSingleton.setConnectionPoolSize(2);
    coordinatorConfigSingleton.setResendDelayTimeUnitMs(20);
    coordinatorConfigSingleton.setConvertPosType("stripe");

    coordinatorConfigSingleton.setRwLogFlag(false);
    coordinatorConfigSingleton.setEnableProcessSsd(false);
    coordinatorConfigSingleton.setLocalDihPort(Integer.valueOf(dihEndPoint));

    coordinatorConfigSingleton.setIoDepth(128);
    coordinatorConfigSingleton.setLargePageSizeForPool(131072);
    coordinatorConfigSingleton.setMediumPageSizeForPool(8192);
    coordinatorConfigSingleton.setLittlePageSizeForPool(1024);
    coordinatorConfigSingleton.setCachePoolSize("16M");
    coordinatorConfigSingleton.setMaxChannelPendingSize(32);

    coordinatorConfigSingleton.setAppName(appName);
    coordinatorConfigSingleton.setHealthCheckerRate(healthCheckerRate);
    coordinatorConfigSingleton.setStreamIo(false);
    coordinatorConfigSingleton.setMaxNetworkFrameSize(maxNetworkFrameSize);
    coordinatorConfigSingleton.setTimePullVolumeAccessRulesIntervalMs(1000);
    coordinatorConfigSingleton.setReportDriverInfoIntervalTimeMs(1000);

    coordinatorConfigSingleton.setEnableLoggerTracer(true);
    coordinatorConfigSingleton.setDebugIoTimeoutMsThreshold(5000);
    coordinatorConfigSingleton.setTraceAllLogs(true);
    return coordinatorConfigSingleton;
  }

  private void initInfoCenterConstants() {
    InfoCenterConstants.setVolumeToBeCreatedTimeout(this.volumeTobeCreatedTimeout);
    InfoCenterConstants.setVolumeBeCreatingTimeout(this.volumeBeCreatingTimeout);
    InfoCenterConstants.setSegmentUnitReportTimeout(this.segmentUnitReportTimeout);
    InfoCenterConstants.setTimeOfdeadVolumeToRemove(deadVolumeToRemove);
    InfoCenterConstants.setRefreshPeriodTime(refreshPeriodTime);
    InfoCenterConstants.setMaxRebalanceTaskCount(maxRebalanceTaskCount);
    InfoCenterConstants.setFixVolumeTimeoutSec(fixVolumeTimeoutSec);

    RebalanceConfiguration rebalanceConfiguration = RebalanceConfiguration.getInstance();
    rebalanceConfiguration.setPressureThreshold(rebalancePressureThreshold);
    rebalanceConfiguration.setPressureThresholdAccuracy(rebalancePressureThresholdAccuracy);
    rebalanceConfiguration.setPressureAddend(rebalancePressureAddend);
    rebalanceConfiguration.setRebalanceTaskExpireTimeSeconds(rebalanceTaskExpireTimeSeconds);
    rebalanceConfiguration.setSegmentWrapSize(segmentWrappCount);
    rebalanceConfiguration.setRebalanceTriggerPeriod(rebalanceTriggerPeriod);
    rebalanceConfiguration.setMinOfSegmentCountCanDoRebalance(minOfSegmentCountCanDoRebalance);
    rebalanceConfiguration
        .setMaxRebalanceTaskCountPerVolumeOfDatanode(maxRebalanceTaskCountPerVolumeOfDatanode);
    rebalanceConfiguration.setMaxRebalanceVolumeCountPerPool(maxRebalanceVolumeCountPerPool);
  }

  public int getDbTaskCorePoolSize() {
    return dbTaskCorePoolSize;
  }

  public int getDbTaskMaxPoolSize() {
    return dbTaskMaxPoolSize;
  }

  public int getDbTaskMaxConcurrentSize() {
    return dbTaskMaxConcurrentSize;
  }

  public int getDbTaskWorkerRateMs() {
    return dbTaskWorkerRateMs;
  }
}
