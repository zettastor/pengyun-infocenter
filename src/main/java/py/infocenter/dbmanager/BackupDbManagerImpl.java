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

package py.infocenter.dbmanager;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import edu.emory.mathcs.backport.java.util.Collections;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.icshare.AccessRuleInformation;
import py.icshare.AccountMetadata;
import py.icshare.CapacityRecord;
import py.icshare.CapacityRecordStore;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.RecoverDbSentry;
import py.icshare.RecoverDbSentryStore;
import py.icshare.Volume2AccessRuleRelationship;
import py.icshare.VolumeAccessRule;
import py.icshare.VolumeRecycleInformation;
import py.icshare.VolumeRuleRelationshipInformation;
import py.icshare.authorization.AccountStore;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.ResourceStore;
import py.icshare.authorization.Role;
import py.icshare.authorization.RoleStore;
import py.icshare.iscsiaccessrule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiaccessrule.IscsiAccessRule;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.MigrationRuleStore;
import py.icshare.qos.RebalanceRuleInformation;
import py.icshare.qos.RebalanceRuleStore;
import py.infocenter.store.AccessRuleStore;
import py.infocenter.store.IscsiAccessRuleStore;
import py.infocenter.store.IscsiRuleRelationshipStore;
import py.infocenter.store.VolumeDelayStore;
import py.infocenter.store.VolumeRecycleStore;
import py.infocenter.store.VolumeRuleRelationshipStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationStore;
import py.thrift.datanode.service.BackupDatabaseInfoRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.AccountMetadataBackupThrift;
import py.thrift.share.ApiToAuthorizeThrift;
import py.thrift.share.CapacityRecordThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.GetDbInfoRequestThrift;
import py.thrift.share.GetDbInfoResponseThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.IscsiRuleRelationshipThrift;
import py.thrift.share.MigrationRuleThrift;
import py.thrift.share.RebalanceRulethrift;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;
import py.thrift.share.ResourceThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeRecycleInformationThrift;
import py.thrift.share.VolumeRuleRelationshipThrift;


public class BackupDbManagerImpl implements BackupDbManager {

  private static final Logger logger = LoggerFactory.getLogger(BackupDbManagerImpl.class);
  private final long roundTimeInterval;
  private final int maxBackupCount;
  private AtomicLong roundSequenceId = new AtomicLong(0);
  private AtomicLong startTime = null;
  private Map<Group, EndPoint> roundRecordMap = new HashMap<>();
  private AtomicBoolean firstRound = new AtomicBoolean(true);
  // stores
  private VolumeRuleRelationshipStore volumeRuleRelationshipStore;
  private AccessRuleStore accessRuleStore;
  private IscsiRuleRelationshipStore iscsiRuleRelationshipStore;
  private IscsiAccessRuleStore iscsiAccessRuleStore;
  private DomainStore domainStore;
  private StoragePoolStore storagePoolStore;
  private CapacityRecordStore capacityRecordStore;
  private AccountStore accountStore;
  private ApiStore apiStore;
  private RoleStore roleStore;
  private ResourceStore resourceStore;
  private InstanceStore instanceStore;
  private IoLimitationStore ioLimitationStore;
  private MigrationRuleStore migrationRuleStore;
  private RebalanceRuleStore rebalanceRuleStore;
  private RecoverDbSentryStore recoverDbSentryStore;
  private VolumeDelayStore volumeDelayStore;
  private VolumeRecycleStore volumeRecycleStore;
  private VolumeJobStoreDb volumeJobStoreDb;

  private AtomicLong round = new AtomicLong(0);
  private Multimap<Long, EndPoint> roundRecord = HashMultimap.create();
  private Multimap<Long, EndPoint> lastRoundRecord = HashMultimap.create();
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory =
      GenericThriftClientFactory
          .create(DataNodeService.Iface.class, 1);


  public BackupDbManagerImpl(long roundTimeInterval, int maxBackupCount,
      VolumeRuleRelationshipStore volumeRuleRelationshipStore, AccessRuleStore accessRuleStore,
      DomainStore domainStore, StoragePoolStore storagePoolStore,
      CapacityRecordStore capacityRecordStore,
      AccountStore accountStore,
      ApiStore apiStore, RoleStore roleStore, ResourceStore resourceStore,
      InstanceStore instanceStore,
      IscsiRuleRelationshipStore iscsiRuleRelationshipStore,
      IscsiAccessRuleStore iscsiAccessRuleStore,
      IoLimitationStore ioLimitationStore, MigrationRuleStore migrationRuleStore,
      RebalanceRuleStore rebalanceRuleStore,
      VolumeDelayStore volumeDelayStore, VolumeRecycleStore volumeRecycleStore,
      VolumeJobStoreDb volumeJobStoreDb, RecoverDbSentryStore recoverDbSentryStore) {
    this.roundSequenceId = new AtomicLong(0);
    this.startTime = null;
    this.roundTimeInterval = roundTimeInterval;
    this.maxBackupCount = maxBackupCount;
    this.roundRecordMap = new ConcurrentHashMap<>();
    this.firstRound = new AtomicBoolean(true);
    this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
    this.accessRuleStore = accessRuleStore;
    this.iscsiRuleRelationshipStore = iscsiRuleRelationshipStore;
    this.iscsiAccessRuleStore = iscsiAccessRuleStore;
    this.domainStore = domainStore;
    this.storagePoolStore = storagePoolStore;
    this.capacityRecordStore = capacityRecordStore;
    this.accountStore = accountStore;
    this.apiStore = apiStore;
    this.roleStore = roleStore;
    this.resourceStore = resourceStore;
    this.instanceStore = instanceStore;
    this.ioLimitationStore = ioLimitationStore;
    this.migrationRuleStore = migrationRuleStore;
    this.rebalanceRuleStore = rebalanceRuleStore;

    this.volumeDelayStore = volumeDelayStore;
    this.volumeRecycleStore = volumeRecycleStore;
    this.volumeJobStoreDb = volumeJobStoreDb;
    this.recoverDbSentryStore = recoverDbSentryStore;
  }

  @Override
  public synchronized ReportDbResponseThrift process(ReportDbRequestThrift reportRequest) {
    ReportDbResponseThrift response = new ReportDbResponseThrift(getRoundSequenceId());

    if (startTime == null) {
      startTime = new AtomicLong(System.currentTimeMillis());
    }
    EndPoint endPoint = EndPoint.fromString(reportRequest.getEndpoint());
    Validate.notNull(endPoint, "endPoint info can not be null, report request: " + reportRequest);
    Validate.isTrue(reportRequest.isSetSequenceId(), "report request must set sequenceId");

    Group group = RequestResponseHelper.buildGroupFrom(reportRequest.getGroup());
    if (group == null) {
      logger.warn(
          "group is null means this datanode is a new one:{}, do not process for now, print its "
              + "request:{}",
          endPoint, reportRequest);
      return response;
    }
    // determine this round is over or not
    if (isRoundEndNow()) {
      firstRound.set(false);
      roundRefresh(true);
    }

    // record all the sequenceId in this round
    roundRecord.put(reportRequest.getSequenceId(), endPoint);
    if (firstRound.get()) {
      if (reportRequest.getSequenceId() > roundSequenceId.get()) {
        roundSequenceId.set(reportRequest.getSequenceId());
      }

      response.setSequenceId(getRoundSequenceId());
      return response;
    }

    /*
     * after first round, should check database is re-setup or new setup
     */
    if (needRecoverDb()) {
      logger.warn("need recover database: {}", lastRoundRecord);
      recoverDatabase();
    }

    if (reportRequest.getSequenceId() >= roundSequenceId.get()) {
      logger.info("do not accept bigger sequence id:{}, current sequence id:{}",
          reportRequest.getSequenceId(),
          roundSequenceId.get());
    }

    response.setSequenceId(getRoundSequenceId());
    if (this.roundRecordMap.containsKey(group)
        || this.roundRecordMap.size() >= this.maxBackupCount) {
      logger.debug(
          "already save to datanode:{} at group:{} or has save count of:{} not less than max "
              + "backup count:{}",
          this.roundRecordMap.get(group), group, this.roundRecordMap.size(), this.maxBackupCount);
      return response;
    } else {
      // generate all database info here, if failed, DO NOT save into record map and logger warn
      try {
        loadTablesFromDb(response);
        logger.info("load from DB for datanode:{} at group:{}, and response:{}", endPoint, group,
            response);
        this.roundRecordMap.put(group, endPoint);
      } catch (Exception e) {
        logger.error("can not load tables from database, will not put into record map", e);
      }
    }
    return response;
  }

  @Override
  public synchronized void backupDatabase() {
    if (isRoundEndNow()) {
      firstRound.set(false);
    }

    if (firstRound.get()) {
      return;
    }
    logger.warn("Backup database to datanode.");
    roundRefresh(false);

    if (needRecoverDb()) {
      recoverDatabase();
    }
    Set<Integer> groups = new HashSet<>();

    BackupDatabaseInfoRequest backupDatabaseInfoRequest = new BackupDatabaseInfoRequest();
    backupDatabaseInfoRequest.setRequestId(RequestIdBuilder.get());
    ReportDbResponseThrift databaseInfo = new ReportDbResponseThrift();
    databaseInfo.setSequenceId(getRoundSequenceId());
    loadTablesFromDb(databaseInfo);
    backupDatabaseInfoRequest.setDatabaseInfo(databaseInfo);

    Set<Instance> datanodes = instanceStore
        .getAll(PyService.DATANODE.getServiceName(), InstanceStatus.HEALTHY);
    for (Instance datanode : datanodes) {
      if (groups.add(datanode.getGroup().getGroupId()) && groups.size() <= maxBackupCount) {
        try {
          EndPoint endPoint = datanode.getEndPoint();
          DataNodeService.Iface dataNodeClient = this.dataNodeClientFactory
              .generateSyncClient(endPoint, 2000, 2000);
          dataNodeClient.backupDatabaseInfo(backupDatabaseInfoRequest);
          roundRecord.put(getRoundSequenceId(), endPoint);
        } catch (Exception e) {
          logger.warn("caught an exception: ", e);
          groups.remove(datanode.getGroup().getGroupId());
        }
      }
    }
    roundRefresh(false);
  }

  @Override
  public synchronized boolean recoverDatabase() {
    ReportDbRequestThrift newestDbInfo = null;

    List<Long> sequenceIdList = new ArrayList<>(lastRoundRecord.keySet());
    Collections.sort(sequenceIdList, Collections.reverseOrder());

    GetDbInfoRequestThrift getDbInfoRequest = new GetDbInfoRequestThrift();
    getDbInfoRequest.setRequestId(RequestIdBuilder.get());

    for (Long sequenceId : sequenceIdList) {
      Collection<EndPoint> datanodes = lastRoundRecord.get(sequenceId);
      for (EndPoint datanode : datanodes) {
        try {
          DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(datanode, 2000, 2000);
          GetDbInfoResponseThrift getDbInfoResponse = dataNodeClient.getDbInfo(getDbInfoRequest);
          newestDbInfo = getDbInfoResponse.getDbInfo();
          if (newestDbInfo != null) {
            break;
          }
        } catch (Exception e) {
          logger.warn("get datanode:{} DB info failed, try another", datanode, e);
        }
      }
      if (newestDbInfo != null) {
        break;
      }
    }
    logger.warn("going to recover DB: {}", newestDbInfo);
    // save newest info to database tables

    boolean result = saveTablesToDb(newestDbInfo);
    recoverDbSentryStore.saveOrUpdate(new RecoverDbSentry(1));

    return result;
  }

  /**
   * xx.
   *
   * @param clearLastRoundRecord if active backup database happened, the round time is shorter than
   *                             normal round end, so keep at least two round records has more
   *                             probability to get latest database info
   */
  private void roundRefresh(boolean clearLastRoundRecord) {
    logger.debug("this round:{} is over, and save info:{}", this.roundSequenceId.get(),
        this.roundRecordMap);
    this.startTime.set(System.currentTimeMillis());
    if (this.roundRecordMap.size() < this.maxBackupCount) {
      logger.warn("this round:{} save count:{} is less than maxBackupCount:{}", roundSequenceId,
          this.roundRecordMap.size(), this.maxBackupCount);
    }
    if (clearLastRoundRecord) {
      lastRoundRecord.clear();
    }
    lastRoundRecord.putAll(roundRecord);
    roundRecord.clear();
    this.roundRecordMap.clear();
    this.roundSequenceId.incrementAndGet();
    this.round.incrementAndGet();
  }

  private boolean isRoundEndNow() {
    return System.currentTimeMillis() > (getStartTime() + roundTimeInterval);
  }

  public long getRoundTimeInterval() {
    return roundTimeInterval;
  }

  public Long getStartTime() {
    return startTime.get();
  }

  public Long getRoundSequenceId() {
    return roundSequenceId.get();
  }

  /**
   * just for unit test.
   */
  public int getRecordCount() {
    return this.roundRecordMap.size();
  }

  /**
   * for unit test too.
   */
  public Map<Group, EndPoint> getRoundRecordMap() {
    return this.roundRecordMap;
  }

  /**
   * for test only.
   */
  public int getLastRecordCount() {
    return lastRoundRecord.size();
  }

  @Override
  public boolean needRecoverDb() {

    try {
      if (volumeRuleRelationshipStore.list().isEmpty()
          && accessRuleStore.list().isEmpty()
          && domainStore.listAllDomains().isEmpty()
          && iscsiRuleRelationshipStore.list().isEmpty()
          && iscsiAccessRuleStore.list().isEmpty()
          && storagePoolStore.listAllStoragePools().isEmpty()
          && recoverDbSentryStore.list().isEmpty()
          &&          /* when ControlCenter reboot, if the accountStore is empty,it will create a
           super
       * admin, so
       *  when RecoverDB, it can not Recover all DB
       * **/
          ioLimitationStore.list().isEmpty()
          && resourceStore.listResources().isEmpty()) {
        return true;
      }
    } catch (Exception e) {
      logger.warn("can not load from database", e);
    }
    return false;
  }

  @Override
  public void loadTablesFromDb(ReportDbResponseThrift response) {
    Validate.notNull(response, "report response can not be null");
    // domain
    List<Domain> domainList = null;
    try {
      domainList = domainStore.listAllDomains();
    } catch (Exception e) {
      logger.warn("can not get domain info from database", e);
    }
    if (domainList != null && !domainList.isEmpty()) {
      List<DomainThrift> domainThriftList = new ArrayList<>();
      for (Domain domain : domainList) {
        DomainThrift domainThrift = RequestResponseHelper.buildDomainThriftFrom(domain);
        domainThriftList.add(domainThrift);
      }
      response.setDomainThriftList(domainThriftList);
    }
    // storagePool
    List<StoragePool> storagePoolList = null;
    try {
      storagePoolList = storagePoolStore.listAllStoragePools();
    } catch (Exception e) {
      logger.warn("can not get storage pool info from database", e);
    }

    if (storagePoolList != null && !storagePoolList.isEmpty()) {
      List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();
      for (StoragePool storagePool : storagePoolList) {
        StoragePoolThrift storagePoolThrift = RequestResponseHelper
            .buildThriftStoragePoolFrom(storagePool, null);
        storagePoolThriftList.add(storagePoolThrift);
      }
      response.setStoragePoolThriftList(storagePoolThriftList);
    }

    // volumeRuleRelationship
    List<VolumeRuleRelationshipInformation> volumeRuleList = volumeRuleRelationshipStore.list();
    if (volumeRuleList != null && !volumeRuleList.isEmpty()) {
      List<VolumeRuleRelationshipThrift> volume2RuleThriftList = new ArrayList<>();
      for (VolumeRuleRelationshipInformation volume2RuleInfo : volumeRuleList) {
        Volume2AccessRuleRelationship volume2Rule = volume2RuleInfo
            .toVolume2AccessRuleRelationship();
        VolumeRuleRelationshipThrift volume2RuleThrift = RequestResponseHelper
            .buildThriftVolumeRuleRelationship(volume2Rule);
        volume2RuleThriftList.add(volume2RuleThrift);
      }
      response.setVolume2RuleThriftList(volume2RuleThriftList);
    }
    // accessRule
    List<AccessRuleInformation> accessRuleList = accessRuleStore.list();
    if (accessRuleList != null && !accessRuleList.isEmpty()) {
      List<VolumeAccessRuleThrift> accessRuleThriftList = new ArrayList<>();
      for (AccessRuleInformation accessRuleInfo : accessRuleList) {
        VolumeAccessRule accessRule = accessRuleInfo.toVolumeAccessRule();
        VolumeAccessRuleThrift accessRuleThrift = RequestResponseHelper
            .buildVolumeAccessRuleThriftFrom(accessRule);
        accessRuleThriftList.add(accessRuleThrift);
      }
      response.setAccessRuleThriftList(accessRuleThriftList);
    }

    // iscsi accessRule
    List<IscsiAccessRuleInformation> iscsiAccessRuleList = iscsiAccessRuleStore.list();
    if (iscsiAccessRuleList != null && !iscsiAccessRuleList.isEmpty()) {
      List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = new ArrayList<>();
      for (IscsiAccessRuleInformation iscsiAccessRuleInfo : iscsiAccessRuleList) {
        IscsiAccessRule iscsiAccessRule = iscsiAccessRuleInfo.toIscsiAccessRule();
        IscsiAccessRuleThrift accessRuleThrift = RequestResponseHelper
            .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
        iscsiAccessRuleThriftList.add(accessRuleThrift);
      }
      response.setIscsiAccessRuleThriftList(iscsiAccessRuleThriftList);
    }

    // iscsiRuleRelationship
    List<IscsiRuleRelationshipInformation> iscsiRuleList = iscsiRuleRelationshipStore.list();
    if (iscsiRuleList != null && !iscsiRuleList.isEmpty()) {
      List<IscsiRuleRelationshipThrift> iscsi2RuleThriftList = new ArrayList<>();
      for (IscsiRuleRelationshipInformation iscsi2RuleInfo : iscsiRuleList) {
        Iscsi2AccessRuleRelationship iscsi2Rule = iscsi2RuleInfo.toIscsi2AccessRuleRelationship();
        IscsiRuleRelationshipThrift iscsi2RuleThrift = RequestResponseHelper
            .buildThriftIscsiRuleRelationship(iscsi2Rule);
        iscsi2RuleThriftList.add(iscsi2RuleThrift);
      }
      response.setIscsi2RuleThriftList(iscsi2RuleThriftList);
    }

    // capacity record
    CapacityRecord capacityRecord = null;
    try {
      capacityRecord = capacityRecordStore.getCapacityRecord();
    } catch (Exception e1) {
      logger.warn("caught an exception", e1);
    }
    if (capacityRecord != null && !capacityRecord.getRecordMap().isEmpty()) {
      List<CapacityRecordThrift> capacityRecordThriftList = new ArrayList<>();
      CapacityRecordThrift capacityRecordThrift = RequestResponseHelper
          .buildThriftCapacityRecordFrom(capacityRecord);
      capacityRecordThriftList.add(capacityRecordThrift);

      response.setCapacityRecordThriftList(capacityRecordThriftList);
    }

    // role
    List<RoleThrift> roleThriftList = new ArrayList<>();
    List<Role> roleList = roleStore.listRoles();
    if (null != roleList && !roleList.isEmpty()) {
      for (Role role : roleList) {
        roleThriftList.add(RequestResponseHelper.buildRoleThrift(role));
      }
      response.setRoleThriftList(roleThriftList);
    }

    // account
    List<AccountMetadataBackupThrift> accountBackupList = new ArrayList<>();
    Collection<AccountMetadata> accountMetadataList = accountStore.listAccounts();
    if (null != accountMetadataList && !accountMetadataList.isEmpty()) {
      for (AccountMetadata account : accountMetadataList) {
        accountBackupList.add(RequestResponseHelper.buildAccountMetadataBackupThrift(account));
      }
      response.setAccountMetadataBackupThriftList(accountBackupList);
    }

    // ioLimitation
    List<IoLimitation> ioLimitationList = ioLimitationStore.list();
    if (ioLimitationList != null && !ioLimitationList.isEmpty()) {
      List<IoLimitationThrift> ioLimitationThriftList = new ArrayList<>();
      for (IoLimitation ioLimitation : ioLimitationList) {
        IoLimitationThrift ioLimitationThrift = RequestResponseHelper
            .buildThriftIoLimitationFrom(ioLimitation);
        ioLimitationThriftList.add(ioLimitationThrift);
        logger.debug("loadtable ioLimitationInfo {} ioLimitationThrift {}", ioLimitation,
            ioLimitationThrift);
      }
      response.setIoLimitationThriftList(ioLimitationThriftList);
    }

    // migrationSpeedRule
    List<MigrationRuleInformation> migrationSpeedList = migrationRuleStore.list();
    if (migrationSpeedList != null && !migrationSpeedList.isEmpty()) {
      List<MigrationRuleThrift> migrationSpeedThriftList = new ArrayList<>();
      for (MigrationRuleInformation migrationSpeedInfo : migrationSpeedList) {
        MigrationRule migrationSpeed = migrationSpeedInfo.toMigrationRule();
        MigrationRuleThrift migrationSpeedThrift = RequestResponseHelper
            .buildMigrationRuleThriftFrom(migrationSpeed);
        migrationSpeedThriftList.add(migrationSpeedThrift);
      }
      response.setMigrationSpeedThriftList(migrationSpeedThriftList);
    }

    //rebalanceRuleStore
    List<RebalanceRuleInformation> rebalanceRuleList = rebalanceRuleStore.list();
    if (rebalanceRuleList != null && !rebalanceRuleList.isEmpty()) {
      List<RebalanceRulethrift> rebalanceRuleThriftList = new ArrayList<>();
      for (RebalanceRuleInformation rebalanceRule : rebalanceRuleList) {
        RebalanceRulethrift rebalanceRuleThrift =
            RequestResponseHelper
                .convertRebalanceRule2RebalanceRuleThrift(rebalanceRule.toRebalanceRule());
        if (rebalanceRuleThrift == null) {
          continue;
        }
        rebalanceRuleThriftList.add(rebalanceRuleThrift);
      }
      response.setRebalanceRuleThriftList(rebalanceRuleThriftList);
    }

    //api
    List<ApiToAuthorize> apiList = null;
    try {
      apiList = apiStore.listApis();
    } catch (Exception e) {
      logger.error("can't get api info from database", e);
    }
    if (apiList != null && !apiList.isEmpty()) {
      List<ApiToAuthorizeThrift> apiThriftList = new ArrayList<>();
      for (ApiToAuthorize api : apiList) {
        apiThriftList.add(RequestResponseHelper.buildApiToAuthorizeThrift(api));
      }
      response.setApiThriftList(apiThriftList);
    }

    //resource
    List<PyResource> resourceList = null;
    try {
      resourceList = resourceStore.listResources();
    } catch (Exception e) {
      logger.error("can't get reource info from database", e);
    }
    if (resourceList != null && !resourceList.isEmpty()) {
      List<ResourceThrift> resourceThriftList = new ArrayList<>();
      for (PyResource resource : resourceList) {
        resourceThriftList.add(RequestResponseHelper.buildResourceThrift(resource));
      }
      response.setResourceThriftList(resourceThriftList);
    }

    // volume_recycle  VolumeRecycleInformationThrift
    List<VolumeRecycleInformation> volumeRecycleInformationList = null;
    try {
      volumeRecycleInformationList = volumeRecycleStore.listVolumesRecycleInfo();
    } catch (Exception e) {
      logger.error("can't get VolumeRecycleInformation from database", e);
    }
    if (volumeRecycleInformationList != null && !volumeRecycleInformationList.isEmpty()) {
      List<VolumeRecycleInformationThrift> volumeRecycleInformationThrifts = new ArrayList<>();

      for (VolumeRecycleInformation volumeRecycleInformation : volumeRecycleInformationList) {
        volumeRecycleInformationThrifts.add(RequestResponseHelper
            .buildThriftVolumeRecycleInformationFrom(volumeRecycleInformation));
      }
      response.setVolumeRecycleInformationThriftList(volumeRecycleInformationThrifts);
    }

    return;
  }

  @Override
  public boolean saveTablesToDb(ReportDbRequestThrift newestDbInfo) {
    if (newestDbInfo == null) {
      return false;
    }
    // domain
    List<DomainThrift> domainThriftList = newestDbInfo.getDomainThriftList();
    if (domainThriftList != null && !domainThriftList.isEmpty()) {
      for (DomainThrift domainThrift : domainThriftList) {
        try {
          Domain domain = RequestResponseHelper.buildDomainFrom(domainThrift);
          domainStore.saveDomain(domain);
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveDomain:{}, find some error:", domainThrift, e);
        }
      }
    }
    // storagePool
    List<StoragePoolThrift> storagePoolThriftList = newestDbInfo.getStoragePoolThriftList();
    if (storagePoolThriftList != null && !storagePoolThriftList.isEmpty()) {
      for (StoragePoolThrift storagePoolThrift : storagePoolThriftList) {
        try {
          StoragePool storagePool = RequestResponseHelper
              .buildStoragePoolFromThrift(storagePoolThrift);
          storagePoolStore.saveStoragePool(storagePool);
        } catch (Exception e) {
          logger
              .warn("saveTablesToDB in saveStoragePool:{}, find some error:", storagePoolThrift, e);
        }
      }
    }
    // volumeRuleRelationship
    List<VolumeRuleRelationshipThrift> volume2RuleThriftList = newestDbInfo
        .getVolume2RuleThriftList();
    if (volume2RuleThriftList != null && !volume2RuleThriftList.isEmpty()) {
      for (VolumeRuleRelationshipThrift volume2RuleThrift : volume2RuleThriftList) {
        try {
          Volume2AccessRuleRelationship volume2Rule = RequestResponseHelper
              .buildVolume2AccessRuleRelationshipFromThrift(volume2RuleThrift);
          volumeRuleRelationshipStore.save(volume2Rule.toVolumeRuleRelationshipInformation());
        } catch (Exception e) {
          logger.warn("saveTablesToDB in volumeRuleRelationship:{}, find some error:",
              volume2RuleThrift, e);
        }
      }
    }
    // accessRule
    List<VolumeAccessRuleThrift> accessRuleThriftList = newestDbInfo.getAccessRuleThriftList();
    if (accessRuleThriftList != null && !accessRuleThriftList.isEmpty()) {
      for (VolumeAccessRuleThrift accessRuleThrift : accessRuleThriftList) {
        try {
          VolumeAccessRule accessRule = RequestResponseHelper
              .buildVolumeAccessRuleFrom(accessRuleThrift);
          accessRuleStore.save(accessRule.toAccessRuleInformation());
        } catch (Exception e) {
          logger
              .warn("saveTablesToDB in VolumeAccessRule:{}, find some error:", accessRuleThrift, e);
        }
      }
    }

    // iscsiRuleRelationship
    List<IscsiRuleRelationshipThrift> iscsi2RuleThriftList = newestDbInfo
        .getIscsi2RuleThriftList();
    if (iscsi2RuleThriftList != null && !iscsi2RuleThriftList.isEmpty()) {
      for (IscsiRuleRelationshipThrift iscsi2RuleThrift : iscsi2RuleThriftList) {
        try {
          Iscsi2AccessRuleRelationship iscsi2Rule = RequestResponseHelper
              .buildIscsi2AccessRuleRelationshipFromThrift(iscsi2RuleThrift);
          iscsiRuleRelationshipStore.save(iscsi2Rule.toIscsiRuleRelationshipInformation());
        } catch (Exception e) {
          logger.warn("saveTablesToDB in IscsiRuleRelationship:{}, find some error:",
              iscsi2RuleThrift, e);
        }
      }
    }
    // accessRule
    List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = newestDbInfo
        .getIscsiAccessRuleThriftList();
    if (iscsiAccessRuleThriftList != null && !iscsiAccessRuleThriftList.isEmpty()) {
      for (IscsiAccessRuleThrift iscsiAccessRuleThrift : iscsiAccessRuleThriftList) {
        try {
          IscsiAccessRule iscsiAccessRule = RequestResponseHelper
              .buildIscsiAccessRuleFrom(iscsiAccessRuleThrift);
          iscsiAccessRuleStore.save(iscsiAccessRule.toIscsiAccessRuleInformation());
        } catch (Exception e) {
          logger
              .warn("saveTablesToDB in IscsiAccessRule:{}, find some error:", iscsiAccessRuleThrift,
                  e);
        }
      }
    }

    // capacity record
    List<CapacityRecordThrift> capacityRecordThriftList = newestDbInfo
        .getCapacityRecordThriftList();
    if (capacityRecordThriftList != null && !capacityRecordThriftList.isEmpty()) {
      for (CapacityRecordThrift capacityRecordThrift : capacityRecordThriftList) {
        try {
          CapacityRecord capacityRecord = RequestResponseHelper
              .buildCapacityRecordFrom(capacityRecordThrift);
          capacityRecordStore.saveCapacityRecord(capacityRecord);
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveCapacityRecord:{}, find some error:",
              capacityRecordThrift, e);
        }
      }
    }

    // api
    List<ApiToAuthorizeThrift> apiThriftList = newestDbInfo.getApiThriftList();
    if (apiThriftList != null && !apiThriftList.isEmpty()) {
      for (ApiToAuthorizeThrift apiThrift : apiThriftList) {
        try {
          ApiToAuthorize api = RequestResponseHelper.buildApiToAuthorizeFrom(apiThrift);
          apiStore.saveApi(api);
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveAPI:{}, find some error:", apiThrift, e);
        }
      }
    }

    // resource
    List<ResourceThrift> resourceThriftList = newestDbInfo.getResourceThriftList();
    if (resourceThriftList != null && !resourceThriftList.isEmpty()) {
      for (ResourceThrift resourceThrift : resourceThriftList) {
        try {
          PyResource resource = RequestResponseHelper.buildResourceFrom(resourceThrift);
          resourceStore.saveResource(resource);
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveResource:{}, find some error:", resourceThrift, e);
        }
      }
    }

    // role
    if (newestDbInfo.isSetRoleThriftList()) {
      for (RoleThrift roleThrift : newestDbInfo.getRoleThriftList()) {
        try {
          roleStore.saveRole(RequestResponseHelper.buildRoleFrom(roleThrift));
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveRole:{}, find some error:", roleThrift, e);
        }
      }
    }

    // account
    if (newestDbInfo.isSetAccountMetadataBackupThriftList()) {
      for (AccountMetadataBackupThrift accountMetadataBackupThrift :
          newestDbInfo.getAccountMetadataBackupThriftList()) {
        try {
          AccountMetadata accountBackup = RequestResponseHelper.buildAccountMetadataBackupFrom(
              accountMetadataBackupThrift);
          accountStore.saveAccount(accountBackup);
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveAccount:{}, find some error:",
              accountMetadataBackupThrift, e);
        }
      }
    }
    // ioLimitation
    List<IoLimitationThrift> ioLimitationThriftList = newestDbInfo.getIoLimitationThriftList();
    if (ioLimitationThriftList != null && !ioLimitationThriftList.isEmpty()) {
      for (IoLimitationThrift ioLimitationThrift : ioLimitationThriftList) {
        try {
          IoLimitation iolimition = RequestResponseHelper.buildIoLimitationFrom(ioLimitationThrift);
          ioLimitationStore.save(iolimition);
        } catch (Exception e) {
          logger.warn("saveTablesToDB in ioLimitation:{}, find some error:", ioLimitationThrift, e);
        }
      }
    }

    // migrationRule
    List<MigrationRuleThrift> migrationSpeedRuleThriftList = newestDbInfo
        .getMigrationRuleThriftList();
    if (migrationSpeedRuleThriftList != null && !migrationSpeedRuleThriftList.isEmpty()) {
      for (MigrationRuleThrift migrationSpeedRuleThrift : migrationSpeedRuleThriftList) {
        try {
          MigrationRule migrationSpeed = RequestResponseHelper
              .buildMigrationRuleFrom(migrationSpeedRuleThrift);
          migrationRuleStore.save(migrationSpeed.toMigrationRuleInformation());
        } catch (Exception e) {
          logger.warn("saveTablesToDB in MigrationRule:{}, find some error:",
              migrationSpeedRuleThrift, e);
        }
      }
    }

    //rebalanceRuleStore
    List<RebalanceRulethrift> rebalanceRuleThriftList = newestDbInfo.getRebalanceRuleThriftList();
    if (rebalanceRuleThriftList != null && !rebalanceRuleThriftList.isEmpty()) {
      for (RebalanceRulethrift rebalanceRuleThrift : rebalanceRuleThriftList) {
        try {
          rebalanceRuleStore.save(
              RequestResponseHelper.convertRebalanceRuleThrift2RebalanceRule(rebalanceRuleThrift)
                  .toRebalanceRuleInformation());
        } catch (Exception e) {
          logger.warn("saveTablesToDB in rebalance rule:{}, find some error:", rebalanceRuleThrift,
              e);
        }
      }
    }

    // volume_recycle  VolumeRecycleInformationThrift
    List<VolumeRecycleInformationThrift> volumeRecycleInformationThrifts = newestDbInfo
        .getVolumeRecycleInformationThriftList();
    if (volumeRecycleInformationThrifts != null && !volumeRecycleInformationThrifts.isEmpty()) {
      for (VolumeRecycleInformationThrift volumeRecycleInformationThrift :
          volumeRecycleInformationThrifts) {
        try {
          volumeRecycleStore.saveVolumeRecycleInfo(
              RequestResponseHelper.buildVolumeRecycleInformationFrom(
                  volumeRecycleInformationThrift));
        } catch (Exception e) {
          logger.warn("saveTablesToDB in saveVolumeRecycleInfo:{}, find some error:",
              volumeRecycleInformationThrift, e);
        }
      }
    }

    return true;
  }

  public VolumeRuleRelationshipStore getVolumeRuleRelationshipStore() {
    return volumeRuleRelationshipStore;
  }

  public void setVolumeRuleRelationshipStore(
      VolumeRuleRelationshipStore volumeRuleRelationshipStore) {
    this.volumeRuleRelationshipStore = volumeRuleRelationshipStore;
  }

  public IscsiRuleRelationshipStore getIscsiRuleRelationshipStore() {
    return iscsiRuleRelationshipStore;
  }

  public void setIscsiRuleRelationshipStore(IscsiRuleRelationshipStore iscsiRuleRelationshipStore) {
    this.iscsiRuleRelationshipStore = iscsiRuleRelationshipStore;
  }

  public IscsiAccessRuleStore getIscsiAccessRuleStore() {
    return iscsiAccessRuleStore;
  }

  public void setIscsiAccessRuleStore(IscsiAccessRuleStore iscsiAccessRuleStore) {
    this.iscsiAccessRuleStore = iscsiAccessRuleStore;
  }

  public AccessRuleStore getAccessRuleStore() {
    return accessRuleStore;
  }

  public void setAccessRuleStore(AccessRuleStore accessRuleStore) {
    this.accessRuleStore = accessRuleStore;
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public CapacityRecordStore getCapacityRecordStore() {
    return capacityRecordStore;
  }

  public void setCapacityRecordStore(CapacityRecordStore capacityRecordStore) {
    this.capacityRecordStore = capacityRecordStore;
  }

  public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeClientFactory() {
    return dataNodeClientFactory;
  }

  public void setDataNodeClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
    this.dataNodeClientFactory = dataNodeClientFactory;
  }

  public IoLimitationStore getIoLimitationStore() {
    return ioLimitationStore;
  }

  public void setIoLimitationStore(IoLimitationStore ioLimitationStore) {
    this.ioLimitationStore = ioLimitationStore;
  }

  public MigrationRuleStore getMigrationRuleStore() {
    return migrationRuleStore;
  }

  public void setMigrationRuleStore(MigrationRuleStore migrationRuleStore) {
    this.migrationRuleStore = migrationRuleStore;
  }

  public RebalanceRuleStore getRebalanceRuleStore() {
    return rebalanceRuleStore;
  }

  public void setRebalanceRuleStore(RebalanceRuleStore rebalanceRuleStore) {
    this.rebalanceRuleStore = rebalanceRuleStore;
  }

  public RecoverDbSentryStore getRecoverDbSentryStore() {
    return recoverDbSentryStore;
  }

  public void setRecoverDbSentryStore(RecoverDbSentryStore recoverDbSentryStore) {
    this.recoverDbSentryStore = recoverDbSentryStore;
  }

  /**
   * this.round.get() > 1 means info center has start two round time, and had recovery DB if
   * necessary
   */
  @Override
  public boolean passedRecoveryTime() {
    return this.round.get() > 1;
  }
}
