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

import java.io.File;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.thrift.ThriftAppEngine;
import py.icshare.RecoverDbSentry;
import py.icshare.RecoverDbSentryStore;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleStore;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.engine.DataBaseTaskEngineFactory;
import py.infocenter.rebalance.old.RebalanceTaskExecutor;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.control.ServiceStartTimes;
import py.infocenter.worker.CreateSegmentUnitWorkThread;
import py.instance.InstanceStore;
import py.periodic.PeriodicWorkExecutor;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.zookeeper.ZkClientFactory;
import py.zookeeper.ZkElectionLeader;


public class InformationCenterAppEngine extends ThriftAppEngine {

  private static final Logger logger = LoggerFactory.getLogger(InformationCenterAppEngine.class);
  private final long retryPeriodOfInstallDefaultLicense = 5000;
  private final long retryTimesOfInstallDefaultLicense = 3;
  private PeriodicWorkExecutorImpl driverStoreSweeperExecutor;
  private PeriodicWorkExecutorImpl volumeSweeperExecutor;
  private PeriodicWorkExecutorImpl instanceMetadataStoreSweeperExecutor;
  private PeriodicWorkExecutorImpl timeoutSweeperExecutor;
  private PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor;
  private PeriodicWorkExecutorImpl reportVolumeInfoSweeperExecutor;
  private PeriodicWorkExecutorImpl volumeActionSweeperExecutor;
  private PeriodicWorkExecutorImpl haInstanceCheckSweeperExecutor;
  private PeriodicWorkExecutorImpl haInstanceMoveVolumeSweeperExecutor;
  private PeriodicWorkExecutorImpl getRebalanceSweeperExecutor;
  private PeriodicWorkExecutorImpl volumeDeleteDelayCheckerExecutor;
  private PeriodicWorkExecutorImpl driverClientManagerSweeperExecutor;
  private PeriodicWorkExecutorImpl serverStatusCheckExecutor;
  private InformationCenterImpl informationCenterImpl;
  private ZkElectionLeader zkElectionLeader;
  private InformationCenterAppConfig informationCenterAppConfig;
  private Set<MigrationRule> defaultMigrationRuleSet;
  private MigrationRuleStore migrationRuleStore;
  private RecoverDbSentryStore recoverDbSentryStore;
  private PySecurityManager securityManager;
  private RebalanceTaskExecutor rebalanceTaskExecutor;
  private PeriodicWorkExecutor dataBaseTaskExecutor;
  private DataBaseTaskEngineFactory dataBaseTaskFactory;

  private boolean startRebalance;
  private CreateSegmentUnitWorkThread createSegmentUnitWorkThread;
  private InfoCenterAppContext infoCenterAppContext;
  private String lockDirectory;
  private String zookeeperConnectionString;
  private int zookeeperSessionTimeout;
  private InstanceStore instanceStore;



  public InformationCenterAppEngine(InformationCenterImpl informationCenterImpl) {
    super(informationCenterImpl);
    informationCenterImpl.setInformationCenterAppEngine(this);
    this.informationCenterImpl = informationCenterImpl;
  }



  public void start() throws Exception {
    /*
     * if the switch of the election leader is opened, we should check if we can cast ticket.
     */
    // choose the Zookeeper
    logger.warn("begin to init zk service, the value:{}",
            informationCenterAppConfig.getZookeeperElectionSwitch());
    if (informationCenterAppConfig.getZookeeperElectionSwitch()) {
      /* init zk ***/
      zkElectionLeader = new ZkElectionLeader(
              new ZkClientFactory(zookeeperConnectionString, zookeeperSessionTimeout),
              lockDirectory, infoCenterAppContext);
      boolean hasTryStartZookeeper = false;
      while (true) {
        try {
          zkElectionLeader.startElection();
          break;
        } catch (Exception e) {
          if (hasTryStartZookeeper) {
            logger.error(
                    "Catch an exception when start election,maybe there is something wrong about "
                            + "zookeeper service");
            throw e;
          } else {
            logger.warn(
                    "Catch an exception when start election,maybe zookeeper service is not start,"
                            + "try start zookeeper");
            try {
              Process process = Runtime.getRuntime()
                      .exec(informationCenterAppConfig.getZooKeeperLauncher());
              process.waitFor();
              hasTryStartZookeeper = true;
              logger.warn("succeed to start zookeeper service!");
            } catch (Exception e1) {
              logger.error("failed to start zookeeper service,{}", e1);
              throw e1;
            }
          }
        }
      }
    }

    /*  set the max number threads of information center, if the data node
     **   number become bigger, we should enlarge
     **   the max number of threads
     ***/
    int numProcessors = Runtime.getRuntime().availableProcessors();
    super.setMinNumThreads(numProcessors * 4);
    super.setMaxNumThreads(numProcessors * 12);
    super.setMaxNetworkFrameSize(informationCenterAppConfig.getMaxNetworkFrameSize());
    super.start();

    driverStoreSweeperExecutor.start();
    volumeSweeperExecutor.start();
    volumeActionSweeperExecutor.start();
    timeoutSweeperExecutor.start();
    haInstanceCheckSweeperExecutor.start();
    haInstanceMoveVolumeSweeperExecutor.start();
    volumeDeleteDelayCheckerExecutor.start();
    driverClientManagerSweeperExecutor.start();
    serverStatusCheckExecutor.start();
    instanceMetadataStoreSweeperExecutor.start();
    serverNodeAlertCheckerExecutor.start();

    saveDefaultMigrationRule();

    initRecoverDbSentryIfServerIsFirstTimeStart();

    initSecurityManager();
    createSegmentUnitWorkThread.start();

    if (startRebalance) {
      rebalanceTaskExecutor.start();
    }

    if (this.dataBaseTaskExecutor == null) {
      this.dataBaseTaskExecutor = new PeriodicWorkExecutorImpl(
          new ExecutionOptionsReader(1, 1,
              null, informationCenterAppConfig.getDbTaskWorkerRateMs()),
          this.dataBaseTaskFactory, "database-task-worker");
    }

    this.dataBaseTaskExecutor.start();
    logger.warn("############# dataBaseTaskExecutor start ##############");

    reportVolumeInfoSweeperExecutor.start();

    getRebalanceSweeperExecutor.start();
    logger.warn("all the thread start ok !");
  }

  public void initRecoverDbSentryIfServerIsFirstTimeStart() throws Exception {
    ServiceStartTimes serviceStartTimes = new ServiceStartTimes();
    serviceStartTimes.setExternalFile(new File(System.getProperty("user.dir")
        + "/ServiceStartTimes"));
    serviceStartTimes.load();
    logger.warn("current start time:{}", serviceStartTimes.getStartTimes());
    if (serviceStartTimes.getStartTimes() == 0) {
      saveRecoverDbSentry();
    } else {
      // do nothing
      logger.warn("This is not the first start, "
          + "recover db sentry info had been saved in the past.");
    }

    serviceStartTimes.setStartTimes(serviceStartTimes.getStartTimes() + 1);
    serviceStartTimes.save();
  }

  public void setZkElectionLeader(ZkElectionLeader zkElectionLeader) {
    this.zkElectionLeader = zkElectionLeader;
  }

  public PeriodicWorkExecutorImpl getVolumeSweeperExecutor() {
    return volumeSweeperExecutor;
  }

  public void setVolumeSweeperExecutor(PeriodicWorkExecutorImpl volumeSweeperExecutor) {
    this.volumeSweeperExecutor = volumeSweeperExecutor;
  }

  public void setTimeoutSweeperExecutor(PeriodicWorkExecutorImpl timeoutSweeperExecutor) {
    this.timeoutSweeperExecutor = timeoutSweeperExecutor;
  }

  public PeriodicWorkExecutorImpl getInstanceMetadataStoreSweeperExecutor() {
    return instanceMetadataStoreSweeperExecutor;
  }

  public void setInstanceMetadataStoreSweeperExecutor(
      PeriodicWorkExecutorImpl instanceMetadataStoreSweeperExecutor) {
    this.instanceMetadataStoreSweeperExecutor = instanceMetadataStoreSweeperExecutor;
  }

  public PeriodicWorkExecutorImpl getDriverStoreSweeperExecutor() {
    return driverStoreSweeperExecutor;
  }

  public void setDriverStoreSweeperExecutor(PeriodicWorkExecutorImpl driverStoreSweeperExecutor) {
    this.driverStoreSweeperExecutor = driverStoreSweeperExecutor;
  }

  public PeriodicWorkExecutorImpl getServerNodeAlertCheckerExecutor() {
    return serverNodeAlertCheckerExecutor;
  }

  public void setServerNodeAlertCheckerExecutor(
      PeriodicWorkExecutorImpl serverNodeAlertCheckerExecutor) {
    this.serverNodeAlertCheckerExecutor = serverNodeAlertCheckerExecutor;
  }

  public void setDefaultMigrationRuleSet(Set<MigrationRule> defaultMigrationRuleSet) {
    this.defaultMigrationRuleSet = defaultMigrationRuleSet;
  }

  public void setMigrationRuleStore(MigrationRuleStore migrationRuleStore) {
    this.migrationRuleStore = migrationRuleStore;
  }

  public void setReportVolumeInfoSweeperExecutor(
      PeriodicWorkExecutorImpl reportVolumeInfoSweeperExecutor) {
    this.reportVolumeInfoSweeperExecutor = reportVolumeInfoSweeperExecutor;
  }

  public PeriodicWorkExecutorImpl getHaInstanceCheckSweeperExecutor() {
    return haInstanceCheckSweeperExecutor;
  }

  public void setHaInstanceCheckSweeperExecutor(
      PeriodicWorkExecutorImpl haInstanceCheckSweeperExecutor) {
    this.haInstanceCheckSweeperExecutor = haInstanceCheckSweeperExecutor;
  }

  public void setHaInstanceMoveVolumeSweeperExecutor(
      PeriodicWorkExecutorImpl haInstanceMoveVolumeSweeperExecutor) {
    this.haInstanceMoveVolumeSweeperExecutor = haInstanceMoveVolumeSweeperExecutor;
  }

  public void setSecurityManager(PySecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  public void setGetRebalanceSweeperExecutor(
      PeriodicWorkExecutorImpl getRebalanceSweeperExecutor) {
    this.getRebalanceSweeperExecutor = getRebalanceSweeperExecutor;
  }

  public void setDataBaseTaskFactory(DataBaseTaskEngineFactory dataBaseTaskFactory) {
    this.dataBaseTaskFactory = dataBaseTaskFactory;
    dataBaseTaskFactory.setInformationCenterImpl(informationCenterImpl);
  }

  public void setVolumeDeleteDelayCheckerExecutor(
      PeriodicWorkExecutorImpl volumeDeleteDelayCheckerExecutor) {
    this.volumeDeleteDelayCheckerExecutor = volumeDeleteDelayCheckerExecutor;
  }

  public void setDriverClientManagerSweeperExecutor(
      PeriodicWorkExecutorImpl driverClientManagerSweeperExecutor) {
    this.driverClientManagerSweeperExecutor = driverClientManagerSweeperExecutor;
  }

  public void setServerStatusCheckExecutor(PeriodicWorkExecutorImpl serverStatusCheckExecutor) {
    this.serverStatusCheckExecutor = serverStatusCheckExecutor;
  }



  public void stop() {
    if (zkElectionLeader != null) {
      zkElectionLeader.close();
    }

    if (driverStoreSweeperExecutor != null) {
      driverStoreSweeperExecutor.stop();
    }

    if (volumeSweeperExecutor != null) {
      volumeSweeperExecutor.stop();
    }

    if (volumeActionSweeperExecutor != null) {
      volumeActionSweeperExecutor.stop();
    }

    if (timeoutSweeperExecutor != null) {
      timeoutSweeperExecutor.stop();
    }

    if (instanceMetadataStoreSweeperExecutor != null) {
      instanceMetadataStoreSweeperExecutor.stop();
    }

    if (serverNodeAlertCheckerExecutor != null) {
      serverNodeAlertCheckerExecutor.stop();
    }

    if (createSegmentUnitWorkThread != null) {
      createSegmentUnitWorkThread.stop();
    }

    if (reportVolumeInfoSweeperExecutor != null) {
      reportVolumeInfoSweeperExecutor.stop();
    }

    if (haInstanceCheckSweeperExecutor != null) {
      haInstanceCheckSweeperExecutor.stop();
    }

    if (haInstanceMoveVolumeSweeperExecutor != null) {
      haInstanceMoveVolumeSweeperExecutor.stop();
    }

    if (volumeDeleteDelayCheckerExecutor != null) {
      volumeDeleteDelayCheckerExecutor.stop();
    }

    if (driverClientManagerSweeperExecutor != null) {
      driverClientManagerSweeperExecutor.stop();
    }

    if (serverStatusCheckExecutor != null) {
      serverStatusCheckExecutor.stop();
    }

    if (dataBaseTaskExecutor != null) {
      dataBaseTaskExecutor.stop();

      if (dataBaseTaskFactory != null) {
        dataBaseTaskFactory.stop();
      }
    }

    try {
      super.stop();
    } catch (Exception e) {
      logger.warn("caught an exception when closing thrift", e);
    }
  }


  public void saveDefaultMigrationRule() {
    if (defaultMigrationRuleSet == null || defaultMigrationRuleSet.size() == 0) {
      logger.warn("no default migrationRule need save.");
      return;
    }
    for (MigrationRule migrationRule : defaultMigrationRuleSet) {
      migrationRuleStore.save(migrationRule.toMigrationRuleInformation());
    }
    logger.warn("save defaultMigrationRuleSet to db successfully, defaultMigrationRuleSet is: {}",
        defaultMigrationRuleSet);
  }

  public void saveRecoverDbSentry() {
    List<RecoverDbSentry> recoverDbSentries = recoverDbSentryStore.list();
    if (recoverDbSentries == null || recoverDbSentries.isEmpty()) {
      recoverDbSentryStore.saveOrUpdate(new RecoverDbSentry(1));
      logger.warn("save recover db sentry success.");
    } else {
      logger.warn("there is a record in recover db, do not need insert a record");
    }
  }

  public void setStartRebalance(boolean startRebalance) {
    this.startRebalance = startRebalance;
  }

  public RebalanceTaskExecutor getRebalanceTaskExecutor() {
    return rebalanceTaskExecutor;
  }

  public void setRebalanceTaskExecutor(RebalanceTaskExecutor rebalanceTaskExecutor) {
    this.rebalanceTaskExecutor = rebalanceTaskExecutor;
  }

  public void setCreateSegmentUnitWorkThread(
      CreateSegmentUnitWorkThread createSegmentUnitWorkThread) {
    this.createSegmentUnitWorkThread = createSegmentUnitWorkThread;
  }

  private void initSecurityManager() throws Exception {
    securityManager.initApiInDb();
    securityManager.createSuperAdminRole();
    securityManager.createBuiltInRoles();
    securityManager.createSuperAdminAccount();
  }

  public PeriodicWorkExecutorImpl getVolumeActionSweeperExecutor() {
    return volumeActionSweeperExecutor;
  }

  public void setVolumeActionSweeperExecutor(PeriodicWorkExecutorImpl volumeActionSweeperExecutor) {
    this.volumeActionSweeperExecutor = volumeActionSweeperExecutor;
  }

  public void setLockDirectory(String lockDirectory) {
    this.lockDirectory = lockDirectory;
  }

  public void setZookeeperConnectionString(String zookeeperConnectionString) {
    this.zookeeperConnectionString = zookeeperConnectionString;
  }

  public void setZookeeperSessionTimeout(int zookeeperSessionTimeout) {
    this.zookeeperSessionTimeout = zookeeperSessionTimeout;
  }

  public void setInfoCenterAppContext(InfoCenterAppContext infoCenterAppContext) {
    this.infoCenterAppContext = infoCenterAppContext;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public InformationCenterAppConfig getInformationCenterAppConfig() {
    return informationCenterAppConfig;
  }

  public void setInformationCenterAppConfig(InformationCenterAppConfig informationCenterAppConfig) {
    this.informationCenterAppConfig = informationCenterAppConfig;
  }

  public RecoverDbSentryStore getRecoverDbSentryStore() {
    return recoverDbSentryStore;
  }

  public void setRecoverDbSentryStore(RecoverDbSentryStore recoverDbSentryStore) {
    this.recoverDbSentryStore = recoverDbSentryStore;
  }
}
