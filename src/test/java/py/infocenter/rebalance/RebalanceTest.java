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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.RequestResponseHelper.convertFromSegmentUnitTypeThrift;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;
import static py.icshare.InstanceMetadata.DatanodeType.NORMAL;
import static py.icshare.InstanceMetadata.DatanodeType.SIMPLE;
import static py.volume.VolumeInAction.NULL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.RequestResponseHelper;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.EndPoint;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.StoragePoolStoreImpl;
import py.icshare.exception.AccessDeniedException;
import py.icshare.exception.VolumeNotFoundException;
import py.icshare.qos.RebalanceRule;
import py.icshare.qos.RebalanceRuleInformation;
import py.icshare.qos.RebalanceRuleStore;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.instance.manger.HaInstanceManger;
import py.infocenter.instance.manger.HaInstanceMangerWithZk;
import py.infocenter.instance.manger.InstanceIncludeVolumeInfoManger;
import py.infocenter.instance.manger.InstanceVolumeInEquilibriumManger;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.rebalance.builder.SimulateInstanceBuilder;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.struct.RebalanceTaskManager;
import py.infocenter.rebalance.struct.SimulateInstanceInfo;
import py.infocenter.rebalance.struct.SimulatePool;
import py.infocenter.rebalance.struct.SimulateSegment;
import py.infocenter.rebalance.struct.SimulateSegmentUnit;
import py.infocenter.rebalance.struct.SimulateVolume;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.InstanceVolumesInformationStore;
import py.infocenter.store.SegmentUnitTimeoutStore;
import py.infocenter.store.SegmentUnitTimeoutStoreImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStatusTransitionStore;
import py.infocenter.store.VolumeStatusTransitionStoreImpl;
import py.infocenter.store.VolumeStore;
import py.infocenter.test.utils.StorageMemStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.informationcenter.StoragePoolStrategy;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.io.qos.RebalanceAbsoluteTime;
import py.io.qos.RebalanceRelativeTime;
import py.membership.SegmentMembership;
import py.test.TestBase;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataRequest;
import py.thrift.share.AbsoluteTimethrift;
import py.thrift.share.AccountNotFoundExceptionThrift;
import py.thrift.share.AddRebalanceRuleRequest;
import py.thrift.share.ApplyRebalanceRuleRequest;
import py.thrift.share.ArbiterMigrateThrift;
import py.thrift.share.DatanodeTypeNotSetExceptionThrift;
import py.thrift.share.DeleteRebalanceRuleRequest;
import py.thrift.share.GetAppliedRebalanceRulePoolRequest;
import py.thrift.share.GetAppliedRebalanceRulePoolResponse;
import py.thrift.share.GetRebalanceRuleRequest;
import py.thrift.share.GetRebalanceRuleResponse;
import py.thrift.share.GetUnAppliedRebalanceRulePoolRequest;
import py.thrift.share.GetUnAppliedRebalanceRulePoolResponse;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.PermissionNotGrantExceptionThrift;
import py.thrift.share.PoolAlreadyAppliedRebalanceRuleExceptionThrift;
import py.thrift.share.PrimaryMigrateThrift;
import py.thrift.share.RebalanceRulethrift;
import py.thrift.share.RebalanceTaskListThrift;
import py.thrift.share.RelativeTimethrift;
import py.thrift.share.SecondaryMigrateThrift;
import py.thrift.share.SegIdThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.UnApplyRebalanceRuleRequest;
import py.thrift.share.UpdateRebalanceRuleRequest;
import py.thrift.share.WeekDaythrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class RebalanceTest extends TestBase {

  public RebalanceConfiguration config = RebalanceConfiguration.getInstance();

  @Mock
  private StoragePoolStore storagePoolStore2;
  private InformationCenterImpl informationCenterImpl2;
  private RebalanceRuleStore rebalanceRuleStore2;
  private SegmentUnitsDistributionManagerImpl distributionManager2;
  @Mock
  private DomainStore domainStore;
  @Mock
  private RebalanceRuleStore rebalanceRuleStore;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private PySecurityManager securityManager;
  private long segmentSize = 1;
  private int maxSegmentCountPerArchive = 50000;
  private InformationCenterImpl informationCenterImpl;
  private StoragePool storagePool;
  private VolumeInformationManger volumeInformationManger;
  private VolumeStore volumeStore;
  private StorageStore storageStore;
  private StoragePoolStore storagePoolStore;
  private SegmentUnitsDistributionManagerImpl distributionManager;
  private Map<InstanceId, ObjectCounter<Long>> mapInstanceIdToDiskCounter = new HashMap<>();
  private Random random = new Random(System.currentTimeMillis());
  private Semaphore semaphore = new Semaphore(0);
  private AtomicInteger groupIdIndex = new AtomicInteger(0);
  private AtomicLong instanceIdIndex = new AtomicLong(0);
  private AtomicInteger volumeIdIndexer = new AtomicInteger(0);
  private long domainId;
  private int segmentWrappCount = 5;

  @Mock
  private InstanceVolumesInformationStore instanceVolumesInformationStore;

  private InstanceIncludeVolumeInfoManger instanceIncludeVolumeInfoManger;
  private InstanceVolumeInEquilibriumManger instanceVolumeInEquilibriumManger;
  @Mock
  private InstanceStore instanceStore;

  private VolumeStatusTransitionStore volumeStatusStore; // store all volumes need to process 
  // their status;
  private SegmentUnitTimeoutStore segmentUnitTimeoutStore;

  public RebalanceTest()
      throws AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift {

    ApplicationContext ctx = new AnnotationConfigApplicationContext(
        InformationCenterAppConfigTest.class);
    volumeStore = (VolumeStore) ctx.getBean("twoLevelVolumeStore");

    volumeInformationManger = new VolumeInformationManger();
    storageStore = new StorageMemStore();
    storagePoolStore = new StoragePoolMemStore();
    distributionManager = new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
        storageStore, storagePoolStore, rebalanceRuleStore, domainStore);
    distributionManager.setVolumeInformationManger(volumeInformationManger);
    distributionManager.setSegmentWrappCount(segmentWrappCount);
    distributionManager.setInstanceStore(instanceStore);

    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    volumeInformationManger.setVolumeStore(volumeStore);
    volumeInformationManger.setAppContext(appContext);

    informationCenterImpl = new InformationCenterImpl();
    informationCenterImpl.setSegmentWrappCount(segmentWrappCount);
    informationCenterImpl.setSegmentUnitsDistributionManager(distributionManager);
    informationCenterImpl.setVolumeStore(volumeStore);
    informationCenterImpl.setRebalanceRuleStore(rebalanceRuleStore);
    informationCenterImpl.setVolumeInformationManger(volumeInformationManger);

    try {
      informationCenterImpl.startAutoRebalance();
    } catch (TException e) {
      e.printStackTrace();
    }
    informationCenterImpl.setAppContext(appContext);
    informationCenterImpl.setStorageStore(storageStore);
    informationCenterImpl.setDomainStore(domainStore);
    informationCenterImpl.setStoragePoolStore(storagePoolStore);
    informationCenterImpl.setAppContext(appContext);
    informationCenterImpl.setSecurityManager(securityManager);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    storagePool = new StoragePool(0L, 0L, "pool", null, null);
    storagePoolStore.saveStoragePool(storagePool);

    when(rebalanceRuleStore.list()).thenReturn(new ArrayList<>());

    RebalanceConfiguration.getInstance().setPressureThreshold(0.01);
    RebalanceConfiguration.getInstance().setRebalanceTaskExpireTimeSeconds(300);

    domainId = RequestIdBuilder.get();

    //for report segment unit
    instanceIncludeVolumeInfoManger = new InstanceIncludeVolumeInfoManger();
    instanceIncludeVolumeInfoManger.setVolumeStore(volumeStore);
    instanceIncludeVolumeInfoManger
        .setInstanceVolumesInformationStore(instanceVolumesInformationStore);
    volumeInformationManger.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    instanceVolumeInEquilibriumManger = new InstanceVolumeInEquilibriumManger();
    instanceVolumeInEquilibriumManger
        .setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    informationCenterImpl.setInstanceVolumeInEquilibriumManger(instanceVolumeInEquilibriumManger);
    informationCenterImpl.setInstanceIncludeVolumeInfoManger(instanceIncludeVolumeInfoManger);

    HaInstanceManger haInstanceManger = new HaInstanceMangerWithZk(instanceStore);

    volumeInformationManger.setHaInstanceManger(haInstanceManger);
    Instance instanceMaster = new Instance(new InstanceId("3"), new Group(1), "test3",
        InstanceStatus.HEALTHY);
    haInstanceManger.setMaterInstance(instanceMaster);

    volumeStatusStore = new VolumeStatusTransitionStoreImpl();
    segmentUnitTimeoutStore = new SegmentUnitTimeoutStoreImpl(3000);

    informationCenterImpl.setVolumeStatusStore(volumeStatusStore);
    informationCenterImpl.setSegmentUnitTimeoutStore(segmentUnitTimeoutStore);

    InstanceId instanceId = new InstanceId(1);
    when(appContext.getInstanceId()).thenReturn(instanceId);

    //mock Instance
    Instance datanodeInstance = new Instance(new InstanceId("1"), "test",
        InstanceStatus.HEALTHY, new EndPoint("10.0.0.80", 8021));
    datanodeInstance.setNetSubHealth(false);

    when(instanceStore.get(any(InstanceId.class))).thenReturn(datanodeInstance);
    when(instanceStore.getByHostNameAndServiceName(any(String.class), any(String.class)))
        .thenReturn(datanodeInstance);
  }


  public void aboutRebalanceRuleInit()
      throws AccountNotFoundExceptionThrift, PermissionNotGrantExceptionThrift {
    @SuppressWarnings("resource")
    ApplicationContext ctx = new AnnotationConfigApplicationContext(
        InformationCenterAppConfigTest.class);
    rebalanceRuleStore2 = (RebalanceRuleStore) ctx.getBean("rebalanceRuleStore");
    distributionManager2 = new SegmentUnitsDistributionManagerImpl(segmentSize, volumeStore,
        storageStore, storagePoolStore2, rebalanceRuleStore2, domainStore);
    distributionManager2.setVolumeInformationManger(volumeInformationManger);

    informationCenterImpl2 = new InformationCenterImpl();
    informationCenterImpl2.setSegmentUnitsDistributionManager(distributionManager2);
    informationCenterImpl2.setRebalanceRuleStore(rebalanceRuleStore2);
    informationCenterImpl2.setStoragePoolStore(storagePoolStore2);
    informationCenterImpl2.setSecurityManager(securityManager);
    informationCenterImpl2.setAppContext(appContext);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
  }


  @Before
  public void init() {
    Timer volumeUpdateTimer = new Timer();
    volumeUpdateTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        for (VolumeMetadata volumeMetadata : volumeInformationManger.listVolumes()) {
          try {
            distributionManager.updateSendRebalanceTasks(volumeMetadata);
          } catch (DatanodeTypeNotSetExceptionThrift datanodeTypeNotSetExceptionThrift) {
            logger.warn("datanode type not set when update sending task list");
          }
        }
      }
    }, 2 * 1000, 1000);
  }

  private long generateArchiveId(long instanceId, int archiveIndex) {
    return instanceId * 1000 + archiveIndex;
  }

  private void addArchiveToExistingStorage(long instanceId, int archiveCount) {
    InstanceMetadata instance = storageStore.get(instanceId);
    List<RawArchiveMetadata> archiveList = instance.getArchives();
    int existingArchiveSize = archiveList.size();
    ObjectCounter<Long> diskCounter = mapInstanceIdToDiskCounter.get(new InstanceId(instanceId));
    for (int i = 0; i < archiveCount; i++) {
      RawArchiveMetadata archive = new RawArchiveMetadata();
      int archiveIndex = existingArchiveSize + i;
      long archiveId = generateArchiveId(instanceId, archiveIndex);
      archive.setArchiveId(archiveId);
      archive.setStoragePoolId(storagePool.getPoolId());
      archive.setLogicalFreeSpace(segmentSize * maxSegmentCountPerArchive);
      archive.setGroup(instance.getGroup());
      archive.setInstanceId(instance.getInstanceId());
      archive.setDeviceName("sd" + archiveIndex);
      archive.setStorageType(StorageType.SSD);
      archive.setSerialNumber(instanceId + "sd" + archiveIndex);
      storagePool.addArchiveInDatanode(instanceId, archiveId);
      diskCounter.set(archiveId, 0);
      archiveList.add(archive);
    }
    storageStore.save(instance);
  }

  private void addStorage(int groupCount, int instanceCountPerGroup, int archiveCount) {
    for (int i = 0; i < groupCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), instanceCountPerGroup, archiveCount);
    }
  }

  private void addInstanceToGroup(int groupId, int instanceCountPerGroup, int archiveCount) {
    long freeSpace = 0;
    Group group = new Group(groupId);
    for (int j = 0; j < instanceCountPerGroup; j++) {
      InstanceId instanceId = new InstanceId(instanceIdIndex.incrementAndGet());
      ObjectCounter<Long> diskCounter = new TreeSetObjectCounter<>();
      mapInstanceIdToDiskCounter.put(instanceId, diskCounter);
      InstanceMetadata instanceMetadata = new InstanceMetadata(instanceId);
      instanceMetadata.setFreeSpace(freeSpace);
      instanceMetadata.setGroup(group);
      instanceMetadata.setDatanodeStatus(OK);
      instanceMetadata.setDatanodeType(NORMAL);
      List<RawArchiveMetadata> archives = new ArrayList<>();
      instanceMetadata.setArchives(archives);
      instanceMetadata.setDomainId(domainId);
      distributionManager.updateSimpleDatanodeInfo(instanceMetadata);
      storageStore.save(instanceMetadata);

      addArchiveToExistingStorage(instanceId.getId(), archiveCount);
    }
  }

  private Set<Long> removeInstanceFromGroup(int groupId, int removeInstanceCount) {
    Set<Long> removedInsId = new HashSet<>();
    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      if (instanceMetadata.getGroup().getGroupId() == groupId) {
        if (removeInstanceCount > 0) {
          long insId = instanceMetadata.getInstanceId().getId();
          storageStore.delete(insId);
          removedInsId.add(insId);
          removeInstanceCount--;
        } else {
          break;
        }
      }
    }
    return removedInsId;
  }

  private void chgInstanceToSimpleDatanode(long instanceId) {
    InstanceMetadata instanceMetadata = storageStore.get(instanceId);
    instanceMetadata.setDatanodeType(SIMPLE);
    distributionManager.updateSimpleDatanodeInfo(instanceMetadata);
  }

  private void setDatanodeArchiveWeight(long instanceId, int weight) {
    InstanceMetadata instanceMetadata = storageStore.get(instanceId);
    List<RawArchiveMetadata> rawArchiveMetadataList = instanceMetadata.getArchives();
    for (RawArchiveMetadata rawArchiveMetadata : rawArchiveMetadataList) {
      rawArchiveMetadata.setWeight(weight);
    }
  }

  private Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> createavolume(
      long size, VolumeType volumeType)
      throws TException {
    ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> result =
        distributionManager
            .reserveVolume(size, volumeType, false, informationCenterImpl.getSegmentWrappCount(),
                storagePool.getPoolId());
    logger.debug("reserve result {}", result);
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeIdIndexer.incrementAndGet(),
        volumeIdIndexer.get(),
        size, segmentSize, volumeType, 0L, storagePool.getPoolId());
    volumeMetadata.setSegmentWrappCount(informationCenterImpl.getSegmentWrappCount());
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        indexMapEntry : result
        .entrySet()) {
      int segIndex = indexMapEntry.getKey();
      final SegId segId = new SegId(volumeIdIndexer.get(), segIndex);
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> map = indexMapEntry
          .getValue();
      List<InstanceIdAndEndPointThrift> arbiterUnits = map.get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceIdAndEndPointThrift> segUnits = map.get(SegmentUnitTypeThrift.Normal);

      // build membership
      SegmentMembership membership;
      InstanceId primary = new InstanceId(segUnits.get(0).getInstanceId());
      List<InstanceId> secondaries = new ArrayList<>();
      for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
        secondaries.add(new InstanceId(segUnits.get(i).getInstanceId()));
      }
      List<InstanceId> arbiters = new ArrayList<>();
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiters.add(new InstanceId(arbiterUnits.get(i).getInstanceId()));
      }
      if (arbiters.isEmpty()) {
        membership = new SegmentMembership(primary, secondaries);
      } else {
        membership = new SegmentMembership(primary, secondaries, arbiters);
      }

      // build segment meta data
      SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

      // build segment units
      for (InstanceId arbiter : membership.getArbiters()) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter);
        InstanceId instanceId = new InstanceId(arbiter);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
        segmentUnitMetadata.setVolumeSource(volumeMetadata.getVolumeSource());
        segmentUnitMetadata.setLastReported(System.currentTimeMillis() - 10);
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
      normalUnits.add(primary);
      for (InstanceId normalUnit : normalUnits) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
            volumeType, SegmentUnitType.Normal);
        InstanceId instanceId = new InstanceId(normalUnit);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
        segmentUnitMetadata.setArchiveId(archiveId);
        segmentUnitMetadata.setVolumeSource(volumeMetadata.getVolumeSource());
        segmentUnitMetadata.setLastReported(System.currentTimeMillis() - 10);
        mapInstanceIdToDiskCounter.get(instanceId).increment(archiveId);
        segment.putSegmentUnitMetadata(instanceId, segmentUnitMetadata);
      }

      volumeMetadata.addSegmentMetadata(segment, membership);
    }
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setInAction(NULL);
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeMetadata.setName("test");
    volumeInformationManger.saveVolume(volumeMetadata);

    return result;
  }

  private ReportSegmentUnitsMetadataRequest createSegmentUnit(long size, VolumeType volumeType)
      throws TException, JsonProcessingException {

    //request
    ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest();
    List<SegmentUnitMetadataThrift> segUnitsMetadata = new ArrayList<>();

    ObjectCounter<InstanceId> diskIndexer = new TreeSetObjectCounter<>();
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> result =
        distributionManager
            .reserveVolume(size, volumeType, false, informationCenterImpl.getSegmentWrappCount(),
                storagePool.getPoolId());
    logger.debug("reserve result {}", result);
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeIdIndexer.incrementAndGet(),
        volumeIdIndexer.get(),
        size, segmentSize, volumeType, 0L, storagePool.getPoolId());
    volumeMetadata.setSegmentWrappCount(informationCenterImpl.getSegmentWrappCount());
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);

    //save the volume
    volumeMetadata.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
    volumeStore.saveVolumeForReport(volumeMetadata);

    for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        indexMapEntry : result
        .entrySet()) {
      int segIndex = indexMapEntry.getKey();
      final SegId segId = new SegId(volumeIdIndexer.get(), segIndex);
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> map = indexMapEntry
          .getValue();
      List<InstanceIdAndEndPointThrift> arbiterUnits = map.get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceIdAndEndPointThrift> segUnits = map.get(SegmentUnitTypeThrift.Normal);

      // build membership
      SegmentMembership membership;
      InstanceId primary = new InstanceId(segUnits.get(0).getInstanceId());
      List<InstanceId> secondaries = new ArrayList<>();
      for (int i = 1; i <= volumeType.getNumSecondaries(); i++) {
        secondaries.add(new InstanceId(segUnits.get(i).getInstanceId()));
      }
      List<InstanceId> arbiters = new ArrayList<>();
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiters.add(new InstanceId(arbiterUnits.get(i).getInstanceId()));
      }
      if (arbiters.isEmpty()) {
        membership = new SegmentMembership(primary, secondaries);
      } else {
        membership = new SegmentMembership(primary, secondaries, arbiters);
      }

      // build segment meta data
      SegmentMetadata segment = new SegmentMetadata(segId, segIndex);

      // build segment units
      for (InstanceId arbiter : membership.getArbiters()) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            SegmentUnitStatus.Arbiter, volumeType, SegmentUnitType.Arbiter);
        InstanceId instanceId = new InstanceId(arbiter);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        segmentUnitMetadata.setArchiveId(instance.getArchives().get(diskIndex).getArchiveId());
        segmentUnitMetadata.setVolumeSource(volumeMetadata.getVolumeSource());

        if (segmentUnitMetadata.getSegId().getIndex() == 0) {
          segmentUnitMetadata.setVolumeMetadataJson(
              volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
        }
        segmentUnitMetadata.setMembership(membership);
        SegmentUnitMetadataThrift segUnitMetadataThrift = RequestResponseHelper
            .buildThriftSegUnitMetadataFrom(segmentUnitMetadata);

        segUnitsMetadata.add(segUnitMetadataThrift);

      }

      Set<InstanceId> normalUnits = new HashSet<>(membership.getSecondaries());
      normalUnits.add(primary);
      for (InstanceId normalUnit : normalUnits) {
        SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(segId, 0, membership,
            normalUnit.equals(primary) ? SegmentUnitStatus.Primary : SegmentUnitStatus.Secondary,
            volumeType, SegmentUnitType.Normal);
        InstanceId instanceId = new InstanceId(normalUnit);
        InstanceMetadata instance = storageStore.get(instanceId.getId());
        diskIndexer.increment(instanceId);
        int diskIndex = (int) (diskIndexer.get(instanceId) % instance.getArchives().size());

        segmentUnitMetadata.setInstanceId(instanceId);
        long archiveId = instance.getArchives().get(diskIndex).getArchiveId();
        segmentUnitMetadata.setArchiveId(archiveId);
        segmentUnitMetadata.setVolumeSource(volumeMetadata.getVolumeSource());
        mapInstanceIdToDiskCounter.get(instanceId).increment(archiveId);

        if (segmentUnitMetadata.getSegId().getIndex() == 0) {
          segmentUnitMetadata.setVolumeMetadataJson(
              volumeMetadata.getVersion() + ":" + volumeMetadata.toJsonString());
        }

        segmentUnitMetadata.setMembership(membership);
        SegmentUnitMetadataThrift segUnitMetadataThrift = RequestResponseHelper
            .buildThriftSegUnitMetadataFrom(segmentUnitMetadata);

        segUnitsMetadata.add(segUnitMetadataThrift);

      }

      request.setSegUnitsMetadata(segUnitsMetadata);
      //make unit
    }

    return request;
  }

  private synchronized void migrateArbiter(ArbiterMigrateThrift task)
      throws AccessDeniedException {
    final SegId segId = RequestResponseHelper.buildSegIdFrom(task.getSegId());
    InstanceId destination = new InstanceId(task.getTargetInstanceId());
    InstanceId source = new InstanceId(task.getSrcInstanceId());

    VolumeMetadata volume = null;
    try {
      volume = volumeInformationManger
          .getVolumeNew(segId.getVolumeId().getId(), Constants.SUPERADMIN_ACCOUNT_ID);
    } catch (VolumeNotFoundException e) {
      logger.error("can not find volume :", e);
      return;
    }

    SegmentMetadata oldSegment = volume.getSegmentByIndex(segId.getIndex());
    SegmentMetadata newSegment = new SegmentMetadata(segId, segId.getIndex());

    SegmentMembership oldMembership = oldSegment.getLatestMembership();
    if (!oldMembership.isArbiter(source)) {
      logger.error("migrate source is not arbiter when migrate arbiter! , seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      //assertTrue(false);
      return;
    }
    if (oldMembership.contain(destination)) {
      logger.error(
          "migrate destination is not empty when migrate arbiter! , seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      //assertTrue(false);
      return;
    }

    Set<InstanceId> arbiterSet = new HashSet<>(oldMembership.getArbiters());
    arbiterSet.remove(source);
    arbiterSet.add(destination);

    SegmentMembership newMembership = new SegmentMembership(
        oldMembership.getSegmentVersion().incGeneration(), oldMembership.getPrimary(), null,
        oldMembership.getSecondaries(), arbiterSet, oldMembership.getInactiveSecondaries(),
        oldMembership.getJoiningSecondaries(), oldMembership.getSecondaryCandidate(),
        oldMembership.getPrimaryCandidate());

    for (SegmentUnitMetadata segmentUnitMetadata : oldSegment.getSegmentUnits()) {
      //SegmentUnitMetadata newSegmentUnitMetadata = new SegmentUnitMetadata(segmentUnitMetadata);
      segmentUnitMetadata.setMembership(newMembership);

      if (segmentUnitMetadata.getInstanceId().equals(source)) {
        segmentUnitMetadata.setInstanceId(destination);
      }
      newSegment.putSegmentUnitMetadata(segmentUnitMetadata.getInstanceId(), segmentUnitMetadata);
    }
    volume.addSegmentMetadata(newSegment, newMembership);

    if (!volume.isStable()) {
      logger.error("volume not stable after migrate arbiter, seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      return;
    }

    volumeInformationManger.saveVolume(volume);
  }

  private synchronized void migrateSecondary(SecondaryMigrateThrift task) throws Exception {
    final SegId segId = RequestResponseHelper.buildSegIdFrom(task.getSegId());
    InstanceId destination = new InstanceId(task.getTargetInstanceId());
    InstanceId source = new InstanceId(task.getSrcInstanceId());

    VolumeMetadata volume = volumeInformationManger
        .getVolumeNew(segId.getVolumeId().getId(), Constants.SUPERADMIN_ACCOUNT_ID);

    SegmentMetadata oldSegment = volume.getSegmentByIndex(segId.getIndex());
    SegmentMetadata newSegment = new SegmentMetadata(segId, segId.getIndex());

    SegmentMembership oldMembership = oldSegment.getLatestMembership();
    if (!oldMembership.isSecondary(source)) {
      logger.error(
          "migrate source is not secondary when migrate secondary! , seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      //assertTrue(false);
      throw new Exception();
    }
    if (oldMembership.contain(destination)) {
      logger.error(
          "migrate destination is not empty when migrate secondary! , seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      throw new Exception();
    }

    SegmentMembership newMembership = oldMembership.addSecondaryCandidate(destination)
        .secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(destination, source);

    for (SegmentUnitMetadata segmentUnitMetadata : oldSegment.getSegmentUnits()) {
      //SegmentUnitMetadata newSegmentUnitMetadata = new SegmentUnitMetadata(segmentUnitMetadata);
      segmentUnitMetadata.setMembership(newMembership);

      if (segmentUnitMetadata.getInstanceId().equals(source)) {
        segmentUnitMetadata.setInstanceId(destination);

        ObjectCounter<Long> sourceArchiveCounter = mapInstanceIdToDiskCounter.get(source);
        sourceArchiveCounter.decrement(segmentUnitMetadata.getArchiveId());

        ObjectCounter<Long> destArchiveCounter = mapInstanceIdToDiskCounter.get(destination);
        long archiveId = destArchiveCounter.min();
        segmentUnitMetadata.setArchiveId(archiveId);
        destArchiveCounter.increment(archiveId);
      }
      newSegment.putSegmentUnitMetadata(segmentUnitMetadata.getInstanceId(), segmentUnitMetadata);
    }
    volume.addSegmentMetadata(newSegment, newMembership);

    if (!volume.isStable()) {
      logger.error("volume not stable after migrate secondary, seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      return;
    }

    volumeInformationManger.saveVolume(volume);
  }

  private synchronized void migratePrimary(PrimaryMigrateThrift task) throws Exception {
    InstanceId destinationPrimary = new InstanceId(task.getTargetInstanceId());
    VolumeMetadata volume = volumeInformationManger
        .getVolumeNew(task.getSegId().getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID);

    SegmentMetadata oldSegment = volume.getSegmentByIndex(task.getSegId().getSegmentIndex());
    SegmentMetadata newSegment = RequestResponseHelper.buildSegmentMetadataFrom(
        RequestResponseHelper.buildThriftSegmentMetadataFrom(oldSegment, false));

    SegmentMembership oldMembership = newSegment.getLatestMembership();
    if (!oldMembership.isPrimary(new InstanceId(task.getSrcInstanceId()))) {
      logger.error("migrate source is not primary when migrate primary! , seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      throw new Exception();
    }
    if (!oldMembership.isSecondary(destinationPrimary)) {
      logger.error(
          "migrate destination is not secondary when migrate primary! , seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      throw new Exception();
    }

    SegmentMembership newMembership = oldMembership
        .secondaryBecomePrimaryCandidate(destinationPrimary)
        .primaryCandidateBecomePrimary(destinationPrimary);

    for (SegmentUnitMetadata segmentUnitMetadata : newSegment.getSegmentUnits()) {
      segmentUnitMetadata.setMembership(newMembership);
      if (segmentUnitMetadata.getInstanceId().equals(destinationPrimary)) {
        segmentUnitMetadata.setStatus(SegmentUnitStatus.Primary);
      } else if (segmentUnitMetadata.getInstanceId().getId() == (task.getSrcInstanceId())) {
        segmentUnitMetadata.setStatus(SegmentUnitStatus.Secondary);
      }
    }
    volume.addSegmentMetadata(newSegment, newMembership);

    if (!volume.isStable()) {
      logger.error("volume not stable after migrate primary, seg index:{}, {} ---> {}",
          task.getSegId(), task.getSrcInstanceId(), task.getTargetInstanceId());
      return;
    }

    volumeInformationManger.saveVolume(volume);
  }

  /**
   * poll task and virtual migrate.
   *
   * @param instanceMetadata instance info
   * @param maxExecTime      instance info
   * @param failRatio        instance info
   * @return has polled tasks?
   */
  private List<PrimaryMigrateThrift> pollAndExecTask(InstanceMetadata instanceMetadata,
      int maxExecTime, double failRatio) throws Exception {
    List<PrimaryMigrateThrift> currentSteps = new LinkedList<>();

    RebalanceTaskListThrift rebalanceTasks = informationCenterImpl
        .getRebalanceTasks(instanceMetadata.getInstanceId());
    if ((!rebalanceTasks.isSetPrimaryMigrateList() || rebalanceTasks.getPrimaryMigrateList()
        .isEmpty())
        && (!rebalanceTasks.isSetSecondaryMigrateList() || rebalanceTasks.getSecondaryMigrateList()
        .isEmpty())
        && (!rebalanceTasks.isSetArbiterMigrateList() || rebalanceTasks.getArbiterMigrateList()
        .isEmpty())) {
      return currentSteps;
    }

    List<VolumeMetadata> volumes = volumeStore.listVolumes();
    for (VolumeMetadata volume : volumes) {
      double rebalanceRatio = distributionManager.getRebalanceProgressInfo(volume.getVolumeId())
          .getRebalanceRatio();
      logger.warn("volume:{} rebalance ratio: {}", volume.getVolumeId(), rebalanceRatio);
      assertTrue(rebalanceRatio <= 1.0);
      assertTrue(rebalanceRatio >= 0);
    }

    if (rebalanceTasks.isSetPrimaryMigrateList()) {
      List<PrimaryMigrateThrift> taskList = new LinkedList<>(
          rebalanceTasks.getPrimaryMigrateList());
      Collections.shuffle(taskList);
      for (PrimaryMigrateThrift primaryMigrateThrift : taskList) {
        if (maxExecTime > 0) {
          try {
            Thread.sleep(random.nextInt(maxExecTime));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        if (random.nextInt(100) < (int) (failRatio * 100)) {
          //Randomly remove several tasks that can not be done correctly from taskList
          logger.warn("failed to migrate primary, seg index:{}, {} ---> {}",
              primaryMigrateThrift.getSegId(), primaryMigrateThrift.getSrcInstanceId(),
              primaryMigrateThrift.getTargetInstanceId());
          continue;
        }

        migratePrimary(primaryMigrateThrift);
        logger.debug("migrate primary, seg index:{}, {} ---> {}",
            primaryMigrateThrift.getSegId(), primaryMigrateThrift.getSrcInstanceId(),
            primaryMigrateThrift.getTargetInstanceId());
      }
      currentSteps.addAll(rebalanceTasks.getPrimaryMigrateList());
    }

    if (rebalanceTasks.isSetSecondaryMigrateList()) {
      List<SecondaryMigrateThrift> taskList = new LinkedList<>(
          rebalanceTasks.getSecondaryMigrateList());
      Collections.shuffle(taskList);
      for (SecondaryMigrateThrift secondaryMigrateThrift : taskList) {
        if (maxExecTime > 0) {
          try {
            Thread.sleep(random.nextInt(maxExecTime));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        if (random.nextInt(100) < (int) (failRatio * 100)) {
          //Randomly remove several tasks that can not be done correctly from taskList
          logger.warn("failed to migrate secondary, seg index:{}, {} ---> {}",
              secondaryMigrateThrift.getSegId(), secondaryMigrateThrift.getSrcInstanceId(),
              secondaryMigrateThrift.getTargetInstanceId());
          continue;
        }

        migrateSecondary(secondaryMigrateThrift);
        logger.debug("migrate secondary, seg index:{}, {} ---> {}",
            secondaryMigrateThrift.getSegId(), secondaryMigrateThrift.getSrcInstanceId(),
            secondaryMigrateThrift.getTargetInstanceId());

        currentSteps.add(new PrimaryMigrateThrift(secondaryMigrateThrift.getSegId(),
            secondaryMigrateThrift.getSrcInstanceId(),
            secondaryMigrateThrift.getTargetInstanceId()));
      }
    }

    if (rebalanceTasks.isSetArbiterMigrateList()) {
      List<ArbiterMigrateThrift> taskList = new LinkedList<>(
          rebalanceTasks.getArbiterMigrateList());
      Collections.shuffle(taskList);
      for (ArbiterMigrateThrift arbiterMigrateThrift : taskList) {
        if (maxExecTime > 0) {
          try {
            Thread.sleep(random.nextInt(maxExecTime));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        if (random.nextInt(100) < (int) (failRatio * 100)) {
          //Randomly remove several tasks that can not be done correctly from taskList
          logger.warn("failed to migrate secondary, seg index:{}, {} ---> {}",
              arbiterMigrateThrift.getSegId(), arbiterMigrateThrift.getSrcInstanceId(),
              arbiterMigrateThrift.getTargetInstanceId());
          continue;
        }

        migrateArbiter(arbiterMigrateThrift);
        logger.debug("migrate secondary, seg index:{}, {} ---> {}",
            arbiterMigrateThrift.getSegId(), arbiterMigrateThrift.getSrcInstanceId(),
            arbiterMigrateThrift.getTargetInstanceId());

        currentSteps.add(new PrimaryMigrateThrift(arbiterMigrateThrift.getSegId(),
            arbiterMigrateThrift.getSrcInstanceId(), arbiterMigrateThrift.getTargetInstanceId()));
      }
    }

    return currentSteps;
  }

  /**
   * calculate rebalance tasks, and return simulate volume after rebalance.
   *
   * @param instanceId     instance id
   * @param volumeMetadata volume info
   * @return simulate volume after rebalance
   */
  private SimulateVolume calcRebalanceTasks(InstanceId instanceId, VolumeMetadata volumeMetadata) {
    distributionManager.forceStart();

    final RebalanceTaskManager rebalanceTaskManager = distributionManager.getRebalanceTaskManager();

    StoragePool storagePoolTemp = new StoragePool();
    storagePoolTemp.setDomainId(storagePool.getDomainId());
    storagePoolTemp.setPoolId(storagePool.getPoolId() + 1);
    storagePoolTemp.setVolumeIds(storagePool.getVolumeIds());
    rebalanceTaskManager.updateEnvironment(volumeMetadata.getVolumeId(), storagePoolTemp);
    rebalanceTaskManager.setPoolEnvChgTimestamp(volumeMetadata.getVolumeId(), 1);

    //calculate rebalance task
    informationCenterImpl.getRebalanceTasks(instanceId);

    //waiting for thread start
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    //waiting for calculation over
    while (!distributionManager.isRebalanceTaskUsable(volumeMetadata.getVolumeId())) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    //got result of volume
    return distributionManager.getResultSimulateVolume(volumeMetadata.getVolumeId());
  }

  /**
   * verify simulate volume is balanced.
   *
   * @param resultOfSimulateVolume simulate volume
   * @throws DatanodeTypeNotSetExceptionThrift if caught a exception
   */
  private void verifyVolumeBalance(SimulateVolume resultOfSimulateVolume, boolean isWeightBalance)
      throws DatanodeTypeNotSetExceptionThrift {
    verifyVolumeBalance(resultOfSimulateVolume, isWeightBalance, true);
  }

  /**
   * verify simulate volume is balanced.
   *
   * @param resultOfSimulateVolume simulate volume
   * @throws DatanodeTypeNotSetExceptionThrift if caught a exception
   */
  private void verifyVolumeBalance(SimulateVolume resultOfSimulateVolume, boolean isWeightBalance,
      boolean verifyResult) throws DatanodeTypeNotSetExceptionThrift {
    if (resultOfSimulateVolume == null) {
      return;
    }
    writeResult2File(resultOfSimulateVolume, storageStore.list());

    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize, resultOfSimulateVolume.getVolumeType())
        .collectionInstance();

    long arbiterDiffCount = 10;
    long secondaryDiffMaxCount = 0;
    if (!isWeightBalance) {
      arbiterDiffCount = -1;
      secondaryDiffMaxCount = -1;
    }
    boolean isResultDistributeOk = verifyResult(resultOfSimulateVolume, simulateInstanceBuilder,
        arbiterDiffCount, secondaryDiffMaxCount);

  }

  @Test
  public void rebalanceTestRebalanceRule() throws TException, IOException, SQLException {
    aboutRebalanceRuleInit();

    {
      RebalanceRuleInformation information = new RebalanceRuleInformation();
      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(1L);
      poolIdList.add(0L);
      poolIdList.add(100L);
      poolIdList.add(123L);
      String jsonStr = information.pooIdList2PoolIdStr(poolIdList);
      List<Long> poolIdListRet = information.poolIdStr2PoolIdList(jsonStr);
      assert (poolIdListRet.size() == 4);
    }

    long ruleId = 10000;
    long poolId = 10000;

    List<StoragePool> allStoragePool = new ArrayList<>();
    StoragePool storagePool1 = new StoragePool(poolId + 1, 0L, "pool1",
        StoragePoolStrategy.Capacity, null);
    allStoragePool.add(storagePool1);
    storagePool1 = new StoragePool(poolId + 2, 0L, "pool2", StoragePoolStrategy.Capacity, null);
    allStoragePool.add(storagePool1);
    storagePool1 = new StoragePool(poolId + 3, 0L, "pool3", StoragePoolStrategy.Capacity, null);
    allStoragePool.add(storagePool1);
    storagePool1 = new StoragePool(poolId + 4, 0L, "pool4", StoragePoolStrategy.Capacity, null);
    allStoragePool.add(storagePool1);

    when(storagePoolStore2.getStoragePool(poolId + 1)).thenReturn(storagePool1);
    when(storagePoolStore2.listAllStoragePools()).thenReturn(allStoragePool);

    informationCenterImpl2.startAutoRebalance();

    //add a rule, and get
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 1);
      rule.setRuleName("rule1");

      AddRebalanceRuleRequest addRequest = new AddRebalanceRuleRequest();
      addRequest.setRequestId(RequestIdBuilder.get());
      addRequest.setRule(rule);

      informationCenterImpl2.addRebalanceRule(addRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 1);
      RebalanceRulethrift responseRule = getResponse.getRules().get(0);
      assert (responseRule.getRuleId() == ruleId + 1);
    }

    //add a rule, and get
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);
      rule.setRuleName("rule2");

      AddRebalanceRuleRequest addRequest = new AddRebalanceRuleRequest();
      addRequest.setRequestId(RequestIdBuilder.get());
      addRequest.setRule(rule);

      informationCenterImpl2.addRebalanceRule(addRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 2);
      RebalanceRulethrift responseRule = getResponse.getRules().get(1);
      assert (responseRule.getRuleId() == ruleId + 2);
    }

    //update rule name
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);
      rule.setRuleName("rule2+update");

      UpdateRebalanceRuleRequest updateRequest = new UpdateRebalanceRuleRequest();
      updateRequest.setRequestId(RequestIdBuilder.get());
      updateRequest.setRule(rule);

      informationCenterImpl2.updateRebalanceRule(updateRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 2);
      RebalanceRulethrift responseRule = getResponse.getRules().get(1);
      assert (responseRule.getRuleId() == ruleId + 2);
      assert (responseRule.getRuleName().equals("rule2+update"));
    }

    //update relative time
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);

      RelativeTimethrift relativeTimeThrift = new RelativeTimethrift();
      relativeTimeThrift.setWaitTime(71);
      rule.setRelativeTime(relativeTimeThrift);

      UpdateRebalanceRuleRequest updateRequest = new UpdateRebalanceRuleRequest();
      updateRequest.setRequestId(RequestIdBuilder.get());
      updateRequest.setRule(rule);

      informationCenterImpl2.updateRebalanceRule(updateRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 2);
      RebalanceRulethrift responseRule = getResponse.getRules().get(1);
      assert (responseRule.getRuleId() == ruleId + 2);
      assert (responseRule.getRelativeTime().getWaitTime() == 71);
    }

    //update absolute time
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);

      final List<AbsoluteTimethrift> absoluteTimeThriftList = new ArrayList<>();

      AbsoluteTimethrift absoluteTimeThrift = new AbsoluteTimethrift();
      absoluteTimeThrift.setId(1);
      absoluteTimeThrift.setBeginTime(100);
      absoluteTimeThrift.setEndTime(200);
      Set<WeekDaythrift> weekDayThriftSet = new HashSet<>();
      weekDayThriftSet.add(WeekDaythrift.MON);
      weekDayThriftSet.add(WeekDaythrift.FRI);
      absoluteTimeThrift.setWeekDay(weekDayThriftSet);
      absoluteTimeThriftList.add(absoluteTimeThrift);

      absoluteTimeThrift = new AbsoluteTimethrift();
      absoluteTimeThrift.setId(2);
      absoluteTimeThrift.setBeginTime(200);
      absoluteTimeThrift.setEndTime(300);
      weekDayThriftSet = new HashSet<>();
      weekDayThriftSet.add(WeekDaythrift.SAT);
      weekDayThriftSet.add(WeekDaythrift.SUN);
      absoluteTimeThrift.setWeekDay(weekDayThriftSet);
      absoluteTimeThriftList.add(absoluteTimeThrift);

      rule.setAbsoluteTimeList(absoluteTimeThriftList);

      UpdateRebalanceRuleRequest updateRequest = new UpdateRebalanceRuleRequest();
      updateRequest.setRequestId(RequestIdBuilder.get());
      updateRequest.setRule(rule);

      informationCenterImpl2.updateRebalanceRule(updateRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 2);
      RebalanceRulethrift responseRule = getResponse.getRules().get(1);
      assert (responseRule.getRuleId() == ruleId + 2);
      List<AbsoluteTimethrift> responseAbsoluteTimethriftList =
          responseRule.getAbsoluteTimeList();
      assert (responseAbsoluteTimethriftList.size() == 2);
      assert (responseAbsoluteTimethriftList.get(0).getBeginTime() == 100);
    }

    //apply success(null -> apply) | unApply
    {
      //apply
      ApplyRebalanceRuleRequest applyRequest = new ApplyRebalanceRuleRequest();
      applyRequest.setRequestId(RequestIdBuilder.get());

      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);
      applyRequest.setRule(rule);

      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(poolId + 1);
      poolIdList.add(poolId + 2);
      applyRequest.setStoragePoolIdList(poolIdList);

      informationCenterImpl2.applyRebalanceRule(applyRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 2);
      RebalanceRulethrift responseRule0 = getResponse.getRules().get(0);
      assert (responseRule0.getRuleId() == ruleId + 1);
      RebalanceRulethrift responseRule1 = getResponse.getRules().get(1);
      assert (responseRule1.getRuleId() == ruleId + 2);

      GetAppliedRebalanceRulePoolRequest appliedRequest =
          new GetAppliedRebalanceRulePoolRequest();
      appliedRequest.setRequestId(RequestIdBuilder.get());
      appliedRequest.setRuleId(ruleId + 1);
      GetAppliedRebalanceRulePoolResponse appliedResponse = informationCenterImpl2
          .getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 0);

      appliedRequest.setRuleId(ruleId + 2);
      appliedResponse = informationCenterImpl2.getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 2);

      GetUnAppliedRebalanceRulePoolRequest unappliedRequest =
          new GetUnAppliedRebalanceRulePoolRequest();
      unappliedRequest.setRequestId(RequestIdBuilder.get());
      GetUnAppliedRebalanceRulePoolResponse unappliedResponse = informationCenterImpl2
          .getUnAppliedRebalanceRulePool(unappliedRequest);
      assert (unappliedResponse.getStoragePoolList().size() == 2);

      //unApply
      UnApplyRebalanceRuleRequest unApplyRequest = new UnApplyRebalanceRuleRequest();
      unApplyRequest.setRequestId(RequestIdBuilder.get());
      unApplyRequest.setRule(rule);
      unApplyRequest.setStoragePoolIdList(poolIdList);

      informationCenterImpl2.unApplyRebalanceRule(unApplyRequest);

      appliedRequest.setRuleId(ruleId + 2);
      appliedResponse = informationCenterImpl2.getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 0);

      unappliedRequest.setRequestId(RequestIdBuilder.get());
      unappliedResponse = informationCenterImpl2.getUnAppliedRebalanceRulePool(unappliedRequest);
      assert (unappliedResponse.getStoragePoolList().size() == 4);
    }

    //apply true(update pool id)
    {
      //apply
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);

      ApplyRebalanceRuleRequest applyRequest = new ApplyRebalanceRuleRequest();
      applyRequest.setRequestId(RequestIdBuilder.get());
      applyRequest.setRule(rule);

      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(poolId + 1);
      applyRequest.setStoragePoolIdList(poolIdList);

      informationCenterImpl2.applyRebalanceRule(applyRequest);

      poolIdList.add(poolId + 3);
      poolIdList.add(poolId + 4);

      informationCenterImpl2.applyRebalanceRule(applyRequest);

      GetAppliedRebalanceRulePoolRequest appliedRequest =
          new GetAppliedRebalanceRulePoolRequest();
      appliedRequest.setRequestId(RequestIdBuilder.get());
      appliedRequest.setRuleId(ruleId + 1);
      GetAppliedRebalanceRulePoolResponse appliedResponse = informationCenterImpl2
          .getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 0);

      appliedRequest.setRuleId(ruleId + 2);
      appliedResponse = informationCenterImpl2.getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 3);

      GetUnAppliedRebalanceRulePoolRequest unappliedRequest =
          new GetUnAppliedRebalanceRulePoolRequest();
      unappliedRequest.setRequestId(RequestIdBuilder.get());
      GetUnAppliedRebalanceRulePoolResponse unappliedResponse = informationCenterImpl2
          .getUnAppliedRebalanceRulePool(unappliedRequest);
      assert (unappliedResponse.getStoragePoolList().size() == 1);
    }

    //apply failed
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 1);

      ApplyRebalanceRuleRequest applyRequest = new ApplyRebalanceRuleRequest();
      applyRequest.setRequestId(RequestIdBuilder.get());
      applyRequest.setRule(rule);

      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(poolId + 1);
      applyRequest.setStoragePoolIdList(poolIdList);

      boolean hasException = false;
      try {
        informationCenterImpl2.applyRebalanceRule(applyRequest);
      } catch (PoolAlreadyAppliedRebalanceRuleExceptionThrift e) {
        hasException = true;
      }
      assertTrue(hasException);
    }
    //unApply true
    {
      RebalanceRulethrift rule = new RebalanceRulethrift();
      rule.setRuleId(ruleId + 2);

      UnApplyRebalanceRuleRequest unApplyRequest = new UnApplyRebalanceRuleRequest();
      unApplyRequest.setRequestId(RequestIdBuilder.get());
      unApplyRequest.setRule(rule);

      List<Long> poolIdList = new ArrayList<>();
      poolIdList.add(poolId + 1);
      poolIdList.add(poolId + 2);
      poolIdList.add(poolId + 3);
      unApplyRequest.setStoragePoolIdList(poolIdList);

      informationCenterImpl2.unApplyRebalanceRule(unApplyRequest);

      GetAppliedRebalanceRulePoolRequest appliedRequest =
          new GetAppliedRebalanceRulePoolRequest();
      appliedRequest.setRequestId(RequestIdBuilder.get());
      appliedRequest.setRuleId(ruleId + 1);
      GetAppliedRebalanceRulePoolResponse appliedResponse = informationCenterImpl2
          .getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 0);

      appliedRequest.setRuleId(ruleId + 2);
      appliedResponse = informationCenterImpl2.getAppliedRebalanceRulePool(appliedRequest);
      assert (appliedResponse.getStoragePoolList().size() == 1);

      GetUnAppliedRebalanceRulePoolRequest unappliedRequest =
          new GetUnAppliedRebalanceRulePoolRequest();
      unappliedRequest.setRequestId(RequestIdBuilder.get());
      GetUnAppliedRebalanceRulePoolResponse unappliedResponse = informationCenterImpl2
          .getUnAppliedRebalanceRulePool(unappliedRequest);
      assert (unappliedResponse.getStoragePoolList().size() == 3);
    }

    //delete
    {
      List<Long> ruleIdList = new ArrayList<>();
      ruleIdList.add(ruleId + 1);
      DeleteRebalanceRuleRequest delRequest = new DeleteRebalanceRuleRequest();
      delRequest.setRequestId(RequestIdBuilder.get());
      delRequest.setRuleIdList(ruleIdList);

      informationCenterImpl2.deleteRebalanceRule(delRequest);

      GetRebalanceRuleRequest getRequest = new GetRebalanceRuleRequest();
      getRequest.setRequestId(RequestIdBuilder.get());
      GetRebalanceRuleResponse getResponse = informationCenterImpl2.getRebalanceRule(getRequest);
      assert (getResponse.getRules().size() == 1);
      RebalanceRulethrift responseRule0 = getResponse.getRules().get(0);
      assert (responseRule0.getRuleId() == ruleId + 2);
    }
  }

  @Test
  public void rebalanceTestIsPoolEnvChanged() throws TException {
    addStorage(3, 1, 2);
    createavolume(segmentSize * 1, VolumeType.SMALL);
    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    //build environment 1
    SimulatePool simulatePool = new SimulatePool(storagePool);
    StoragePool newStoragePool = new StoragePool();
    newStoragePool.setDomainId(storagePool.getDomainId());
    newStoragePool.setPoolId(storagePool.getPoolId());
    newStoragePool.setVolumeIds(storagePool.getVolumeIds());
    Multimap<Long, Long> datanode2ArchivesMap = HashMultimap
        .create(storagePool.getArchivesInDataNode());
    newStoragePool.setArchivesInDataNode(datanode2ArchivesMap);

    //no archive lost or add
    SimulatePool.PoolChangedStatus poolChangedStatus = simulatePool
        .isPoolEnvChanged(newStoragePool, volume);
    assertTrue(poolChangedStatus == SimulatePool.PoolChangedStatus.NO_CHANGE);

    //add archive
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), 1L);
    poolChangedStatus = simulatePool.isPoolEnvChanged(newStoragePool, volume);
    assertTrue(poolChangedStatus == SimulatePool.PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME);
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), 1L);

    Multimap<Long, Long> oldDatanode2ArchivesMap = storagePool.getArchivesInDataNode();
    List<Long> archives = new ArrayList<>(oldDatanode2ArchivesMap.get(instanceIdIndex.get()));

    //archive lost
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), archives.get(0));
    poolChangedStatus = simulatePool.isPoolEnvChanged(newStoragePool, volume);
    assertTrue(
        poolChangedStatus == SimulatePool.PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME);
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), archives.get(0));

    //archive lost and add
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), archives.get(0));
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), 1L);
    poolChangedStatus = simulatePool.isPoolEnvChanged(newStoragePool, volume);
    assertTrue(
        poolChangedStatus == SimulatePool.PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME);
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), archives.get(0));
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), 1L);

    //build environment 2
    addStorage(1, 1, 2);
    oldDatanode2ArchivesMap = storagePool.getArchivesInDataNode();
    archives = new ArrayList<>(oldDatanode2ArchivesMap.get(instanceIdIndex.get()));
    datanode2ArchivesMap = HashMultimap.create(storagePool.getArchivesInDataNode());
    newStoragePool.setArchivesInDataNode(datanode2ArchivesMap);

    //archive lost but archive has not distribute seg unit
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), archives.get(0));
    poolChangedStatus = simulatePool.isPoolEnvChanged(newStoragePool, volume);
    assertTrue(poolChangedStatus == SimulatePool.PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME);
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), archives.get(0));

    //archive lost but archive has not distribute seg unit, and has archive add
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), archives.get(0));
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), 1L);
    poolChangedStatus = simulatePool.isPoolEnvChanged(newStoragePool, volume);
    assertTrue(poolChangedStatus == SimulatePool.PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME);
    newStoragePool.addArchiveInDatanode(instanceIdIndex.get(), archives.get(0));
    newStoragePool.removeArchiveFromDatanode(instanceIdIndex.get(), 1L);
  }


  @Test
  public void rebalanceTestCanRebalance() throws TException, ParseException, IOException {
    long poolId = 1000;
    VolumeMetadata volume = mock(VolumeMetadata.class);
    when(volume.getStoragePoolId()).thenReturn(poolId);

    //rebalance switch off
    {
      informationCenterImpl.pauseAutoRebalance();
      boolean doRebalance = distributionManager.canRebalance(null);
      assertTrue(!doRebalance);
    }

    //rebalance switch on and rule is null
    {
      informationCenterImpl.startAutoRebalance();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(null);
      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(doRebalance);
    }

    //rule Ok - week null time null, but wait time is 60s
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);
      when(volume.getStableTime()).thenReturn(System.currentTimeMillis());
      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(!doRebalance);
    }

    //rule Ok = week null time null, stable time is Ok
    {
      when(volume.getStableTime())
          .thenReturn(System.currentTimeMillis() - RebalanceRule.MIN_WAIT_TIME * 1000);
      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(doRebalance);
    }

    /*
     * absolute time
     */
    long nowTimestamp = System.currentTimeMillis();
    Date nowTime = new Date(nowTimestamp);
    Calendar nowCalendar = Calendar.getInstance();
    nowCalendar.setTime(nowTime);
    int nowDayOfWeek = nowCalendar.get(Calendar.DAY_OF_WEEK) - 1;
    if (nowDayOfWeek < 0) {
      nowDayOfWeek = 0;
    }
    RebalanceAbsoluteTime.WeekDay nowWeekDay = RebalanceAbsoluteTime.WeekDay
        .findByValue(nowDayOfWeek);

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    long zeroTimestamp = calendar.getTimeInMillis();
    ;

    long relativeTimeS = nowTimestamp / 1000 - zeroTimestamp / 1000;

    //rule Ok-week OK time OK
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      final List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();

      RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
      Set<RebalanceAbsoluteTime.WeekDay> weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS - 100);
      absoluteTime.setEndTime(relativeTimeS + 100);
      absoluteTimeList.add(absoluteTime);

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(doRebalance);
    }

    //rule Ok-week OK time OK
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();

      final List<RebalanceAbsoluteTime> absoluteTimeList = (new RebalanceRuleInformation())
          .jsonStr2AbsoluteTime("[{\"endTime\":86100,\"id\":0}]");

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(doRebalance);
    }

    //rule failed-week
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      final List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();

      RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
      Set<RebalanceAbsoluteTime.WeekDay> weekDays = new HashSet<>();
      weekDays.add(RebalanceAbsoluteTime.WeekDay.findByValue(nowWeekDay.getValue() + 1));
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS - 100);
      absoluteTime.setEndTime(relativeTimeS + 100);
      absoluteTimeList.add(absoluteTime);

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(!doRebalance);
    }

    //rule failed-time begin < end
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      final List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();

      RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
      Set<RebalanceAbsoluteTime.WeekDay> weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS + 100);
      absoluteTime.setEndTime(relativeTimeS + 500);
      absoluteTimeList.add(absoluteTime);

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(!doRebalance);
    }

    //rule failed-time begin > end
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      final List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();

      RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
      Set<RebalanceAbsoluteTime.WeekDay> weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS + 100);
      absoluteTime.setEndTime(relativeTimeS - 100 - 100);
      absoluteTimeList.add(absoluteTime);

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(!doRebalance);
    }

    //multi-rule Ok
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      final List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();

      RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
      Set<RebalanceAbsoluteTime.WeekDay> weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS + 100);
      absoluteTime.setEndTime(relativeTimeS - 100);
      absoluteTimeList.add(absoluteTime);

      absoluteTime = new RebalanceAbsoluteTime();
      weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS - 100);
      absoluteTime.setEndTime(relativeTimeS + 100);
      absoluteTimeList.add(absoluteTime);

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(doRebalance);
    }

    //multi-rule failed
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      final List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();

      RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
      Set<RebalanceAbsoluteTime.WeekDay> weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS + 100);
      absoluteTime.setEndTime(relativeTimeS - 100);
      absoluteTimeList.add(absoluteTime);

      weekDays = new HashSet<>();
      weekDays.add(nowWeekDay);
      absoluteTime = new RebalanceAbsoluteTime();
      absoluteTime.setWeekDaySet(weekDays);
      absoluteTime.setBeginTime(relativeTimeS + 100);
      absoluteTime.setEndTime(relativeTimeS + 200);
      absoluteTimeList.add(absoluteTime);

      rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(!doRebalance);
    }

    /*
     * relative time
     */
    //OK
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();

      RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
      relativeTime.setWaitTime(5);
      rebalanceRule.setRelativeTime(relativeTime);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);
      when(volume.getStableTime()).thenReturn(System.currentTimeMillis());

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(!doRebalance);
    }

    //failed
    {
      final RebalanceRule rebalanceRule = new RebalanceRule();
      RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
      relativeTime.setWaitTime(3);
      rebalanceRule.setRelativeTime(relativeTime);

      RebalanceRuleInformation rebalanceRuleInformation = rebalanceRule
          .toRebalanceRuleInformation();
      when(rebalanceRuleStore.getRuleOfPool(poolId)).thenReturn(rebalanceRuleInformation);
      when(volume.getStableTime())
          .thenReturn(System.currentTimeMillis() - RebalanceRule.MIN_WAIT_TIME * 1000);

      boolean doRebalance = distributionManager.canRebalance(volume);
      assertTrue(doRebalance);
    }
  }

  @Test
  public void rebalanceTestRebalanceTriggerCheckerTimer() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 10;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    distributionManager.forceStart();
    final RebalanceTaskManager rebalanceTaskManager = distributionManager.getRebalanceTaskManager();

    assertTrue(rebalanceTaskManager.getRebalanceStatus(volume.getVolumeId())
        == RebalanceTaskManager.VolumeRebalanceStatus.CHECKING);
    try {
      distributionManager.selectRebalanceTasks(null);
    } catch (NoNeedToRebalance e) {
      logger.error("", e);
    }
    assertTrue(rebalanceTaskManager.getRebalanceStatus(volume.getVolumeId())
        == RebalanceTaskManager.VolumeRebalanceStatus.CHECKING);

    rebalanceTaskManager.setRebalanceStatus(volume.getVolumeId(),
        RebalanceTaskManager.VolumeRebalanceStatus.BALANCED);
    assertTrue(rebalanceTaskManager.getRebalanceStatus(volume.getVolumeId())
        == RebalanceTaskManager.VolumeRebalanceStatus.BALANCED);
    try {
      distributionManager.selectRebalanceTasks(null);
    } catch (NoNeedToRebalance e) {
      logger.error("", e);
    }
    assertTrue(rebalanceTaskManager.getRebalanceStatus(volume.getVolumeId())
        == RebalanceTaskManager.VolumeRebalanceStatus.CHECKING);
  }

  @Test
  public void rebalanceTestPsaNoWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 10;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPsaSameWeight() throws Exception {
    informationCenterImpl.setSegmentWrappCount(12);
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 2;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {

      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPsaExtremeDiffWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 1000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;

    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    logger.warn("instance count is {}", instanceMetadataList.size());

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
      logger.warn("set instance:{} archive weight:{}", instanceMetadata.getInstanceId().getId(),
          weight);
    }
    setDatanodeArchiveWeight(
        instanceMetadataList.get(instanceMetadataList.size() - 1).getInstanceId().getId(), 1234);

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  @Test
  public void rebalanceTestPsaRandomWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  @Test
  public void rebalanceTestPssNoWeight() throws Exception {
    informationCenterImpl.setSegmentWrappCount(1);
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    addInstanceCount = 3;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    logger.warn("segment count:{}, instance count:{}", segmentCount, instanceMetadataList.size());

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPssSameWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 1000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 10;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPssExtremeDiffWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 3968;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 8;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    setDatanodeArchiveWeight(instanceIdIndex.get(), 1230);

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  @Test
  public void rebalanceTestPssRandomWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPssaaNoWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPssaaSameWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPssaaExtremeDiffWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }
    setDatanodeArchiveWeight(instanceIdIndex.get(), 1230);

    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPssaaRandomWeight() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  /**
   * different group have different datanode and has simple datanode.
   */
  @Test
  public void rebalanceTestPssaaRandomWeightDifferentNodeOfGroupAddNotSimple()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 2, 2);

    chgInstanceToSimpleDatanode(instanceIdIndex.get());
    chgInstanceToSimpleDatanode(instanceIdIndex.get() - 3);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 1000;

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }
    //chgInstanceToSimpleDatanode(instanceIdIndex.get());

    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    logger.warn("segment count:{}, instance count:{}", segmentCount, instanceMetadataList.size());

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  /**
   * different group have different datanode and has simple datanode.
   */
  @Test
  public void rebalanceTestPssaaRandomWeightDifferentNodeOfGroupAddSimple() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 2, 2);

    chgInstanceToSimpleDatanode(instanceIdIndex.get());
    chgInstanceToSimpleDatanode(instanceIdIndex.get() - 3);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 45;

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    addInstanceCount = 3;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }
    chgInstanceToSimpleDatanode(instanceIdIndex.get());

    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    logger.warn("segment count:{}, instance count:{}", segmentCount, instanceMetadataList.size());

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPsa3GroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 2, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);
    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);
    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPsa8GroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(8, 2, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 50, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));
      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  /**
   * rebalanceTestPsa3GroupDifferentnodeofgroupNodedownReservesegmentAddnodeInonegroup.
   */
  @Test
  public void rebalanceTestPsa3() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(1, 2, 2);
    addStorage(1, 5, 2);
    addStorage(1, 3, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    //verifyResult(volume.getVolumeType(), segIndex2Instances,3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPsa8GroupDifferentNodeOfGroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(1, 2, 2);
    addStorage(1, 5, 2);
    addStorage(1, 3, 2);
    addStorage(1, 6, 2);
    addStorage(1, 3, 2);
    addStorage(1, 4, 2);
    addStorage(1, 2, 2);
    addStorage(1, 3, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    //verifyResult(volume.getVolumeType(), segIndex2Instances,3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPss3GroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 2, 2);
    int segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);
    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();
        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());
        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPss8GroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(8, 2, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 50, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {

      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    verifyVolumeBalance(resultOfSimulateVolume, true);
  }

  @Test
  public void rebalanceTestPss3GroupDifferentNodeOfGroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(1, 2, 2);
    addStorage(1, 3, 2);
    addStorage(1, 5, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    //verifyResult(volume.getVolumeType(), segIndex2Instances,3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      /*
       * reserve segment unit S and A
       */
      //reserved other segment unit
      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {

      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true, false);
  }

  @Test
  public void rebalanceTestPss8GroupDifferentNodeOfGroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(1, 2, 2);
    addStorage(1, 3, 2);
    addStorage(1, 5, 2);
    addStorage(1, 4, 2);
    addStorage(1, 2, 2);
    addStorage(1, 6, 2);
    addStorage(1, 2, 2);
    addStorage(1, 3, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    //verifyResult(volume.getVolumeType(), segIndex2Instances,3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true, false);
  }

  @Test
  public void rebalanceTestPssaa5GroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 2, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true, false);
  }

  @Test
  public void rebalanceTestPssaa8GroupNodeDownReserveSegmentAddNodeInOneGroup()
      throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(8, 2, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 50, 10, 3);

    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, true, false);
  }

  /**
   * rebalanceTestPssaa5GroupDifferentnodeofgroupNodedownReservesegmentAddnodeInonegroup.
   */
  @Test
  public void rebalanceTestPssa() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(1, 2, 2);
    addStorage(1, 3, 2);
    addStorage(1, 5, 2);
    addStorage(1, 3, 2);
    addStorage(1, 3, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    //verifyResult(volume.getVolumeType(), segIndex2Instances,3, 10, 10, 3);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      /*
       * change P
       */
      //if removed segment unit is primary, make other S to Primary
      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  /**
   * rebalanceTestPssaa8GroupDifferentnodeofgroupNodedownReservesegmentAddnodeInonegroupv.
   */
  @Test
  public void rebalanceTestPssaa() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(1, 2, 2);
    addStorage(1, 3, 2);
    addStorage(1, 5, 2);
    addStorage(1, 3, 2);
    addStorage(1, 3, 2);
    addStorage(1, 6, 2);
    addStorage(1, 4, 2);
    addStorage(1, 3, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    segmentCount = 5000;
    logger.warn("segment count is {}", segmentCount);

    /*
     * create volume
     */
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);

    /*
     * remove one instance
     */
    Set<Long> removeInsIdSet = removeInstanceFromGroup(groupIdIndex.get(), 1);
    long removeInsId = removeInsIdSet.iterator().next();
    InstanceId removeInsIdObj = new InstanceId(removeInsId);

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      final SegId segId = segmentMetadata.getSegId();
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      if (!segmentUnitMetadataMap.containsKey(removeInsIdObj)) {
        continue;
      }

      SegmentMembership oldMembership = segmentMetadata.getLatestMembership();

      SegmentUnitMetadata removedSegUnit = segmentUnitMetadataMap.get(removeInsIdObj);

      if (oldMembership.isPrimary(removeInsIdObj)) {
        InstanceId secondaryId = oldMembership.getSecondaries().iterator().next();

        PrimaryMigrateThrift task = new PrimaryMigrateThrift();
        task.setSegId(new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()));
        task.setSrcInstanceId(removeInsIdObj.getId());
        task.setTargetInstanceId(secondaryId.getId());

        migratePrimary(task);
      }

      Set<Long> excludedInstanceIds = new HashSet<>();
      excludedInstanceIds.add(oldMembership.getPrimary().getId());
      for (InstanceId secondary : oldMembership.getSecondaries()) {
        excludedInstanceIds.add(secondary.getId());
      }
      for (InstanceId arbiter : oldMembership.getArbiters()) {
        excludedInstanceIds.add(arbiter.getId());
      }
      excludedInstanceIds.remove(removeInsId);

      Set<Long> storagePoolIdList = new HashSet<>();
      for (StoragePool pool : storagePoolStore.listAllStoragePools()) {
        storagePoolIdList.add(pool.getPoolId());
      }
      Domain domain = new Domain();
      domain.setDomainId(domainId);
      domain.setStoragePools(storagePoolIdList);
      when(domainStore.getDomain(any(Long.class))).thenReturn(domain);

      SegmentUnitTypeThrift reserveType = SegmentUnitTypeThrift.Normal;
      if (oldMembership.getArbiters().contains(removeInsIdObj)) {
        reserveType = SegmentUnitTypeThrift.Arbiter;
      }

      ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
          excludedInstanceIds,
          1, volume.getVolumeId(), segId.getIndex(), convertFromSegmentUnitTypeThrift(reserveType));

      ReserveSegUnitResult reserveSegUnitResult = distributionManager
          .reserveSegUnits(reserveSegUnitsInfo);

      List<InstanceMetadataThrift> insList = reserveSegUnitResult.getInstances();
      long reserveDestId = insList.get(0).getInstanceId();
      if (reserveType == SegmentUnitTypeThrift.Normal) {
        SecondaryMigrateThrift secondaryMigrateThrift = new SecondaryMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        secondaryMigrateThrift.setInitMembership(
            RequestResponseHelper.buildThriftMembershipFrom(removedSegUnit.getSegId(),
                volume.getSegmentByIndex(segId.getIndex()).getLatestMembership()));

        migrateSecondary(secondaryMigrateThrift);
      } else {
        ArbiterMigrateThrift arbiterMigrateThrift = new ArbiterMigrateThrift(
            new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex()),
            removeInsId, reserveDestId, removedSegUnit.getVolumeType().getVolumeTypeThrift(),
            null, volume.getStoragePoolId(),
            removedSegUnit.getSegmentUnitType().getSegmentUnitTypeThrift(),
            volume.getSegmentWrappCount(),
            removedSegUnit.isEnableLaunchMultiDrivers(),
            removedSegUnit.getVolumeSource().getVolumeSourceThrift());

        migrateArbiter(arbiterMigrateThrift);
      }
    }

    writeResult2File(volume, storageStore.list());

    /*
     * add one instance
     */
    addInstanceToGroup(groupIdIndex.get(), 1, 2);

    /*
     * rebalance
     */
    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      //chgInstanceToSimpleDatanode(instanceMetadata.getInstanceId().getId());
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    //calculate rebalance tasks
    SimulateVolume resultOfSimulateVolume = calcRebalanceTasks(
        instanceMetadataList.get(0).getInstanceId(), volume);

    //verify simulate volume is balanced
    verifyVolumeBalance(resultOfSimulateVolume, false);
  }

  @Test
  @Ignore
  public void rebalanceTestPsaSameWeightMultiThread() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(3, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 1050;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 1;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    distributionManager.forceStart();

    final RebalanceTaskManager rebalanceTaskManager = distributionManager.getRebalanceTaskManager();
    for (VolumeMetadata volumeTemp : volumeList) {
      rebalanceTaskManager.setPoolEnvChgTimestamp(volumeTemp.getVolumeId(), 1);
    }

    Timer[] timer = new Timer[instanceMetadataList.size()];
    for (int i = 0; i < instanceMetadataList.size(); i++) {
      InstanceMetadata instanceMetadata = instanceMetadataList.get(i);
      PoolTask poolTask = new PoolTask(instanceMetadata, 200, 0.1);
      timer[i] = new Timer();
      timer[i].schedule(poolTask, 2 * 1000, 300);
    }

    semaphore.acquire(instanceMetadataList.size());

    double rebalanceRatio = distributionManager.getRebalanceProgressInfo(volume.getVolumeId())
        .getRebalanceRatio();
    logger.warn("volume:{} rebalance ratio: {}", volume.getVolumeId(), rebalanceRatio);
    assertTrue(rebalanceRatio == 1.0);

    writeResult2File(
        volumeInformationManger.getVolumeNew(volume.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
        storageStore.list());

    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize, volume.getVolumeType())
        .collectionInstance();
    boolean isResultDistributeOk = verifyResult(
        volumeInformationManger.getVolume(volume.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
        simulateInstanceBuilder, 10, 0);

    assertTrue(isResultDistributeOk);
  }

  @Test
  @Ignore
  public void rebalanceTestPssaaSameWeightMultiThread() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 1050;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 2;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    distributionManager.forceStart();
    final RebalanceTaskManager rebalanceTaskManager = distributionManager.getRebalanceTaskManager();
    for (VolumeMetadata volumeTemp : volumeList) {
      rebalanceTaskManager.setPoolEnvChgTimestamp(volumeTemp.getVolumeId(), 1);
    }

    Timer[] timer = new Timer[instanceMetadataList.size()];
    for (int i = 0; i < instanceMetadataList.size(); i++) {
      InstanceMetadata instanceMetadata = instanceMetadataList.get(i);
      PoolTask poolTask = new PoolTask(instanceMetadata, 200, 0.1);
      timer[i] = new Timer();
      timer[i].schedule(poolTask, 2 * 1000, 300);
    }

    semaphore.acquire(instanceMetadataList.size());

    double rebalanceRatio = distributionManager.getRebalanceProgressInfo(volume.getVolumeId())
        .getRebalanceRatio();
    logger.warn("volume:{} rebalance ratio: {}", volume.getVolumeId(), rebalanceRatio);
    assertTrue((long) rebalanceRatio == 1);

    writeResult2File(
        volumeInformationManger.getVolume(volume.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
        storageStore.list());

    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize, volume.getVolumeType())
        .collectionInstance();
    boolean isResultDistributeOk = verifyResult(
        volumeInformationManger.getVolume(volume.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
        simulateInstanceBuilder, 10, 0);

    assert (isResultDistributeOk);
  }

  @Test
  @Ignore
  public void rebalanceTestPsaPssPssaaSameWeightMultiThread3Volume() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    config.setMaxRebalanceVolumeCountPerPool(5);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 3000;
    logger.warn("segment count is {}", segmentCount);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segIndex2Instances2 =
        createavolume(segmentSize * segmentCount, VolumeType.SMALL);
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segIndex2Instances3 =
        createavolume(segmentSize * segmentCount, VolumeType.REGULAR);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 10;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), 23);
    }

    distributionManager.forceStart();
    final RebalanceTaskManager rebalanceTaskManager = distributionManager.getRebalanceTaskManager();
    for (VolumeMetadata volumeTemp : volumeList) {
      rebalanceTaskManager.setPoolEnvChgTimestamp(volumeTemp.getVolumeId(), 1);
    }

    Timer[] timer = new Timer[instanceMetadataList.size()];
    for (int i = 0; i < instanceMetadataList.size(); i++) {
      InstanceMetadata instanceMetadata = instanceMetadataList.get(i);
      PoolTask poolTask = new PoolTask(instanceMetadata, 200, 0.1);
      timer[i] = new Timer();
      timer[i].schedule(poolTask, 2 * 1000, 300);
    }

    semaphore.acquire(instanceMetadataList.size());

    for (VolumeMetadata volumeMetadata : volumeList) {
      double rebalanceRatio = distributionManager.getRebalanceProgressInfo(volume.getVolumeId())
          .getRebalanceRatio();
      logger.warn("volume:{} rebalance ratio: {}", volume.getVolumeId(), rebalanceRatio);
      assertTrue((long) rebalanceRatio == 1);

      writeResult2File(volumeInformationManger
              .getVolume(volumeMetadata.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
          storageStore.list());

      SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
          storageStore,
          segmentSize, volumeMetadata.getVolumeType()).collectionInstance();
      boolean isResultDistributeOk = verifyResult(
          volumeInformationManger.getVolume(volumeMetadata.getVolumeId(),
              Constants.SUPERADMIN_ACCOUNT_ID), simulateInstanceBuilder, 10, 0);

      assert (isResultDistributeOk);
    }
  }

  /**
   * environment changed when migrating.
   *
   * @throws Exception exception
   */
  @Test
  @Ignore
  public void rebalanceTestPssaaRandomWeightMultiThreadEnvironmentChanged() throws Exception {
    setLogLevel(Level.WARN);
    config.setMaxRebalanceTaskCountPerVolumeOfDatanode(100);
    addStorage(5, 1, 2);

    int segmentCount = random.nextInt(5000);
    if (segmentCount == 0) {
      segmentCount = 1;
    }
    //segmentCount = 1000;
    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        createavolume(segmentSize * segmentCount, VolumeType.LARGE);

    List<VolumeMetadata> volumeList = volumeInformationManger.listVolumes();
    VolumeMetadata volume = volumeList.get(0);

    writeResult2File(volume.getVolumeType(), storageStore.list(), segIndex2Instances);
    verifyResult(volume.getVolumeType(), segIndex2Instances, 3, 10, 10, 3);

    int addInstanceCount = random.nextInt(10) + 1;
    //addInstanceCount = 2;
    for (int i = 0; i < addInstanceCount; i++) {
      addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
    }

    List<InstanceMetadata> instanceMetadataList = storageStore.list();
    logger.warn("segment count:{}, instance count:{}", segmentCount, instanceMetadataList.size());

    for (InstanceMetadata instanceMetadata : instanceMetadataList) {
      int weight = random.nextInt(100);
      if (weight == 0) {
        weight = 1;
      }
      setDatanodeArchiveWeight(instanceMetadata.getInstanceId().getId(), weight);
    }

    distributionManager.forceStart();
    final RebalanceTaskManager rebalanceTaskManager = distributionManager.getRebalanceTaskManager();
    for (VolumeMetadata volumeTemp : volumeList) {
      rebalanceTaskManager.setPoolEnvChgTimestamp(volumeTemp.getVolumeId(), 1);
    }

    Timer[] timer = new Timer[instanceMetadataList.size()];
    PoolTask[] poolLastTasks = new PoolTask[instanceMetadataList.size()];
    for (int i = 0; i < instanceMetadataList.size(); i++) {
      InstanceMetadata instanceMetadata = instanceMetadataList.get(i);
      poolLastTasks[i] = new PoolTask(instanceMetadata, 300, 0.1);
      timer[i] = new Timer();
      timer[i].schedule(poolLastTasks[i], 2 * 1000, 300);
    }

    //add instance when migrating that simulate environment changed
    int addInstanceCountSecond = random.nextInt(3) + 1;
    Timer[] timerSecond = new Timer[addInstanceCountSecond];
    while (true) {
      boolean chgEnvironment = false;
      for (PoolTask poolTask : poolLastTasks) {
        int runTask = segmentCount / poolLastTasks.length;
        runTask = Math.min(runTask, 10);
        if (poolTask.getRunTimes() > runTask) {
          chgEnvironment = true;
          break;
        }
      }
      if (chgEnvironment) {
        //change environment
        logger.warn("add {} instance when migrating", addInstanceCountSecond);
        for (int i = 0; i < addInstanceCountSecond; i++) {
          addInstanceToGroup(groupIdIndex.incrementAndGet(), 1, 2);
          int weight = random.nextInt(100);
          if (weight == 0) {
            weight = 1;
          }
          setDatanodeArchiveWeight(instanceIdIndex.get(), weight);

          for (VolumeMetadata volumeTemp : volumeList) {
            rebalanceTaskManager.setPoolEnvChgTimestamp(volumeTemp.getVolumeId(), 1);
          }

          //add timer
          InstanceMetadata instanceMetadata = storageStore.get(instanceIdIndex.get());
          timerSecond[i] = new Timer();
          timerSecond[i].schedule(new PoolTask(instanceMetadata, 300, 0.1), 2 * 1000, 300);
        }
        break;
      }
      Thread.sleep(500);
    }

    semaphore.acquire(instanceMetadataList.size());

    double rebalanceRatio = distributionManager.getRebalanceProgressInfo(volume.getVolumeId())
        .getRebalanceRatio();
    logger.warn("volume:{} rebalance ratio: {}", volume.getVolumeId(), rebalanceRatio);
    assertTrue((long) rebalanceRatio == 1);

    writeResult2File(
        volumeInformationManger.getVolume(volume.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
        storageStore.list());

    SimulateInstanceBuilder simulateInstanceBuilder = new SimulateInstanceBuilder(storagePool,
        storageStore, segmentSize, volume.getVolumeType())
        .collectionInstance();
    boolean isResultDistributeOk = verifyResult(
        volumeInformationManger.getVolume(volume.getVolumeId(), Constants.SUPERADMIN_ACCOUNT_ID),
        simulateInstanceBuilder, -1, -1);

    assert (isResultDistributeOk);
  }

  public void parseResultFromFile() throws IOException {
    File readFile = new File("/tmp/ReserveVolumeTest_PS.log");
    BufferedReader reader = new BufferedReader(new FileReader(readFile));

    long datanodeCount = 0;

    /*
     * parse.
     */
    ObjectCounter<Long> primaryId = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryId = new TreeSetObjectCounter<>();
    ObjectCounter<Long> arbiterId = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primary2SecondaryMap = new HashMap<>();
    LinkedList<Set<Long>> balanceSecondaryNode = new LinkedList<>();
    String lineBuf;
    while ((lineBuf = reader.readLine()) != null) {
      String[] comb = lineBuf.split("\t");
      datanodeCount = comb.length - 2;
      Set<Long> snode = new HashSet<>();
      long downNode = 3;
      boolean needBalance = false;

      ObjectCounter<Long> secondaryOfPrimaryCounterTemp = new TreeSetObjectCounter<>();
      long primaryIdTemp = 0;
      for (int index = 2; index < comb.length; index++) {
        if (comb[index].equals("P")) {
          primaryIdTemp = (long) (index - 2);
          primaryId.increment(primaryIdTemp);
          if (downNode == index - 2) {
            needBalance = true;
          }
        } else if (comb[index].equals("S")) {
          secondaryId.increment((long) (index - 2));
          snode.add((long) (index - 2));
          secondaryOfPrimaryCounterTemp.increment((long) (index - 2));
        } else if (comb[index].equals("A")) {
          arbiterId.increment((long) (index - 2));
        }
      }

      if (needBalance) {
        balanceSecondaryNode.add(snode);
      }

      ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryMap
          .computeIfAbsent(primaryIdTemp, value -> new TreeSetObjectCounter<>());

      for (Long secondaryIdTemp : secondaryOfPrimaryCounterTemp.getAll()) {
        secondaryOfPrimaryCounter.increment(secondaryIdTemp);
      }

    }

    /*
     * write node combination
     */
    File writeFile = new File("/tmp/ReserveVolumeTest_PS_result.log");
    OutputStream outputStream = null;
    if (!writeFile.exists()) {
      try {
        writeFile.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      outputStream = new FileOutputStream(writeFile);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    String writeBuf = "";

    /*
     * primary
     */
    int pmax = 0;
    int pmin = 0xffffff;
    writeBuf += "P:\t\t";
    for (long i = 0; i < datanodeCount; i++) {
      writeBuf += primaryId.get(i) + "\t";
      pmax = Math.max(pmax, (int) primaryId.get(i));
      if (primaryId.get(i) != 0) {
        pmin = Math.min(pmin, (int) primaryId.get(i));
      }
    }
    if (pmin == 0xffffff) {
      pmin = 0;
    }
    writeBuf += "\r\n";
    writeBuf += "P分配差额：\t" + pmax + "\t" + pmin + "\t" + (pmax - pmin) + "\r\n\n";

    /*
     * Secondary
     */
    pmax = 0;
    pmin = 0xffffff;
    writeBuf += "S:\t\t";
    for (long i = 0; i < datanodeCount; i++) {
      writeBuf += secondaryId.get(i) + "\t";
      pmax = Math.max(pmax, (int) secondaryId.get(i));
      if (secondaryId.get(i) != 0) {
        pmin = Math.min(pmin, (int) secondaryId.get(i));
      }
    }
    if (pmin == 0xffffff) {
      pmin = 0;
    }
    writeBuf += "\r\n";
    writeBuf += "S分配差额：\t" + pmax + "\t" + pmin + "\t" + (pmax - pmin) + "\r\n\n";

    /*
     * arbiter
     */
    pmax = 0;
    pmin = 0xffffff;
    writeBuf += "A:\t\t";
    for (long i = 0; i < datanodeCount; i++) {
      writeBuf += arbiterId.get(i) + "\t";
      pmax = Math.max(pmax, (int) arbiterId.get(i));
      if (arbiterId.get(i) != 0) {
        pmin = Math.min(pmin, (int) arbiterId.get(i));
      }
    }
    if (pmin == 0xffffff) {
      pmin = 0;
    }
    writeBuf += "\r\n";
    writeBuf += "A分配差额：\t" + pmax + "\t" + pmin + "\t" + (pmax - pmin) + "\r\n\n";

    /*
     * rebalance
     */
    ObjectCounter<Long> pbalancecounter = new TreeSetObjectCounter<>();
    for (Set<Long> snode : balanceSecondaryNode) {
      for (Long i : snode) {
        pbalancecounter.increment(i);
      }
    }

    for (long primaryIdTemp : primary2SecondaryMap.keySet()) {
      ObjectCounter<Long> secondaryOfPrimaryCounter = primary2SecondaryMap.get(primaryIdTemp);
      pmax = 0;
      pmin = 0xffffff;
      writeBuf += "P(" + primaryIdTemp + ")再平衡:\t";

      for (long i = 0; i < datanodeCount; i++) {
        writeBuf += secondaryOfPrimaryCounter.get(i) + "\t";
        pmax = Math.max(pmax, (int) secondaryOfPrimaryCounter.get(i));
        if (secondaryOfPrimaryCounter.get(i) != 0) {
          pmin = Math.min(pmin, (int) secondaryOfPrimaryCounter.get(i));
        }
      }
      if (pmin == 0xffffff) {
        pmin = 0;
      }
      writeBuf += "\r\n";
      writeBuf += "P再平衡差额：\t" + pmax + "\t" + pmin + "\t" + (pmax - pmin) + "\r\n\n";
    }

    //write
    try {
      outputStream.write(writeBuf.getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }

    //close
    try {
      outputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeResult2File(VolumeType volumeType, List<InstanceMetadata> allInstanceList,
      Map<Integer, Map<SegmentUnitTypeThrift,
          List<InstanceIdAndEndPointThrift>>> segIndex2Instances) {
    File file = new File("/tmp/ReserveVolumeTest_PS.log");
    OutputStream outputStream = null;
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      outputStream = new FileOutputStream(file);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    for (int segIndex : segIndex2Instances.keySet()) {
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> instanceListFromRemote =
          segIndex2Instances
              .get(segIndex);
      List<InstanceIdAndEndPointThrift> arbiterInstanceList = instanceListFromRemote
          .get(SegmentUnitTypeThrift.Arbiter);

      Set<Long> arbiterIdSet = new HashSet<>();
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiterIdSet.add(arbiterInstanceList.get(i).getInstanceId());
      }

      List<InstanceIdAndEndPointThrift> normalInstanceList = instanceListFromRemote
          .get(SegmentUnitTypeThrift.Normal);

      long primaryDatanodeId = normalInstanceList.get(0).getInstanceId();
      Set<Long> secondaryIdSet = new HashSet<>();
      for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
        secondaryIdSet.add(normalInstanceList.get(i).getInstanceId());
      }

      String writeBuf = segIndex + "\t";

      for (long i = 0; i < allInstanceList.size(); i++) {
        long id = allInstanceList.get((int) i).getInstanceId().getId();
        if (primaryDatanodeId == id) {
          writeBuf += "\tP";
        } else if (secondaryIdSet.contains(id)) {
          writeBuf += "\tS";
        } else if (arbiterIdSet.contains(id)) {
          writeBuf += "\tA";
        } else {
          writeBuf += "\tO";
        }
      }

      writeBuf += "\r\n";

      try {
        outputStream.write(writeBuf.getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    //close
    try {
      outputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      parseResultFromFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeResult2File(VolumeMetadata volume, List<InstanceMetadata> allInstanceList) {
    File file = new File("/tmp/ReserveVolumeTest_PS.log");
    OutputStream outputStream = null;
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      outputStream = new FileOutputStream(file);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    for (int segIndex : volume.getSegmentTable().keySet()) {
      SegmentMetadata segmentMetadata = volume.getSegmentTable().get(segIndex);
      Map<InstanceId, SegmentUnitMetadata> segmentUnitMap = segmentMetadata
          .getSegmentUnitMetadataTable();

      Set<Long> arbiterIdSet = new HashSet<>();
      Set<Long> secondaryIdSet = new HashSet<>();
      long primaryDatanodeId = -1;
      for (InstanceId insId : segmentUnitMap.keySet()) {
        SegmentUnitMetadata segmentUnitMetadata = segmentUnitMap.get(insId);
        if (segmentUnitMetadata.getMembership().isArbiter(insId)) {
          arbiterIdSet.add(insId.getId());
        } else if (segmentUnitMetadata.getMembership().isPrimary(insId)) {
          primaryDatanodeId = insId.getId();
        } else if (segmentUnitMetadata.getMembership().isSecondary(insId)) {
          secondaryIdSet.add(insId.getId());
        }
      }

      String writeBuf = segIndex + "\t";

      for (long i = 0; i < allInstanceList.size(); i++) {
        long id = allInstanceList.get((int) i).getInstanceId().getId();
        if (primaryDatanodeId == id) {
          writeBuf += "\tP";
        } else if (secondaryIdSet.contains(id)) {
          writeBuf += "\tS";
        } else if (arbiterIdSet.contains(id)) {
          writeBuf += "\tA";
        } else {
          writeBuf += "\tO";
        }
      }

      writeBuf += "\r\n";

      try {
        outputStream.write(writeBuf.getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    //close
    try {
      outputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      parseResultFromFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void writeResult2File(SimulateVolume simulateVolume,
      List<InstanceMetadata> allInstanceList) {
    if (simulateVolume == null) {
      return;
    }

    File file = new File("/tmp/ReserveVolumeTest_PS.log");
    OutputStream outputStream = null;
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      outputStream = new FileOutputStream(file);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    for (int segIndex : simulateVolume.getSegIndex2SimulateSegmentMap().keySet()) {
      SimulateSegment segmentMetadata = simulateVolume.getSegIndex2SimulateSegmentMap()
          .get(segIndex);

      Set<Long> arbiterIdSet = new HashSet<>();
      Set<Long> secondaryIdSet = new HashSet<>();
      long primaryDatanodeId = segmentMetadata.getPrimaryId().getId();
      for (InstanceId instanceId : segmentMetadata.getSecondaryIdSet()) {
        secondaryIdSet.add(instanceId.getId());
      }
      for (InstanceId instanceId : segmentMetadata.getArbiterIdSet()) {
        arbiterIdSet.add(instanceId.getId());
      }

      String writeBuf = segIndex + "\t";

      for (long i = 0; i < allInstanceList.size(); i++) {
        long id = allInstanceList.get((int) i).getInstanceId().getId();
        if (primaryDatanodeId == id) {
          writeBuf += "\tP";
        } else if (secondaryIdSet.contains(id)) {
          writeBuf += "\tS";
        } else if (arbiterIdSet.contains(id)) {
          writeBuf += "\tA";
        } else {
          writeBuf += "\tO";
        }
      }

      writeBuf += "\r\n";

      try {
        outputStream.write(writeBuf.getBytes());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    //close
    try {
      outputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      parseResultFromFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void verifyResult(VolumeType volumeType,
      Map<Integer, Map<SegmentUnitTypeThrift,
          List<InstanceIdAndEndPointThrift>>> segIndex2Instances,
      long pdiffCount, long sdiffCount, long afiffCount, long pdownDiffCount) {
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();
    for (int segIndex : segIndex2Instances.keySet()) {

      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> instanceListFromRemote =
          segIndex2Instances
              .get(segIndex);
      List<InstanceIdAndEndPointThrift> arbiterInstanceList = instanceListFromRemote
          .get(SegmentUnitTypeThrift.Arbiter);

      //count necessary arbiter
      for (int i = 0; i < volumeType.getNumArbiters(); i++) {
        arbiterIdCounter.increment(arbiterInstanceList.get(i).getInstanceId());
      }

      List<InstanceIdAndEndPointThrift> normalInstanceList = instanceListFromRemote
          .get(SegmentUnitTypeThrift.Normal);

      //count necessary primary
      long primaryId = normalInstanceList.get(0).getInstanceId();
      primaryIdCounter.increment(primaryId);

      ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap.get(primaryId);
      if (primarySecondaryCounter == null) {
        primarySecondaryCounter = new TreeSetObjectCounter<>();
        primaryId2SecondaryIdCounterMap.put(primaryId, primarySecondaryCounter);
      }

      //count necessary secondary
      for (int i = 1; i < volumeType.getNumSecondaries() + 1; i++) {
        long secondaryId = normalInstanceList.get(i).getInstanceId();
        secondaryIdCounter.increment(secondaryId);

        primarySecondaryCounter.increment(secondaryId);
      }
    }

    long maxCount;
    long minCount;
    /*
     * verify average distribution
     */
    //verify necessary primary max and min count
    maxCount = primaryIdCounter.maxValue();
    minCount = primaryIdCounter.minValue();
    if (maxCount - minCount > pdiffCount) {
      logger.error("primary average distribute failed ! maxCount:{}, minCount:{}", maxCount,
          minCount);
    }
    assert (maxCount - minCount <= pdiffCount);

    //verify necessary secondary max and min count
    maxCount = secondaryIdCounter.maxValue();
    minCount = secondaryIdCounter.minValue();
    if (maxCount - minCount > sdiffCount) {
      logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}", maxCount,
          minCount);
    }
    assert (maxCount - minCount <= sdiffCount);

    //verify necessary arbiter max and min count
    if (arbiterIdCounter.size() > 0) {
      maxCount = arbiterIdCounter.maxValue();
      minCount = arbiterIdCounter.minValue();
      if (maxCount - minCount > afiffCount) {
        logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}", maxCount,
            minCount);
      }
      assert (maxCount - minCount <= afiffCount);
    }

    /*
     * verify rebalance when P down
     */
    for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()) {
      ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
      maxCount = secondaryCounterTemp.maxValue();
      minCount = secondaryCounterTemp.minValue();

      if (maxCount - minCount > pdownDiffCount) {
        logger.error("rebalance failed when P down! maxCount:{}, minCount:{}, entry:{}", maxCount,
            minCount, entry);
      }
      assert (maxCount - minCount <= pdownDiffCount);
    }
  }

  public boolean verifyResult(VolumeMetadata volume, long pdiffCount, long sdiffCount,
      long afiffCount, long pdownDiffCount) {
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

    for (int segIndex : volume.getSegmentTable().keySet()) {
      SegmentMetadata segmentMetadata = volume.getSegmentTable().get(segIndex);

      Map<InstanceId, SegmentUnitMetadata> segmentUnitMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      Set<Long> secondaryOfPrimarySet = new HashSet<>();
      long primaryId = -1;
      for (InstanceId insId : segmentUnitMap.keySet()) {
        SegmentUnitMetadata segmentUnitMetadata = segmentUnitMap.get(insId);
        if (segmentUnitMetadata.getMembership().isArbiter(insId)) {
          arbiterIdCounter.increment(insId.getId());
        } else if (segmentUnitMetadata.getMembership().isPrimary(insId)) {
          primaryId = insId.getId();
          primaryIdCounter.increment(insId.getId());
        } else if (segmentUnitMetadata.getMembership().isSecondary(insId)) {
          secondaryIdCounter.increment(insId.getId());
          secondaryOfPrimarySet.add(insId.getId());
        }
      }

      ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap
          .computeIfAbsent(primaryId, value -> new TreeSetObjectCounter<>());
      //count necessary secondary
      for (long secondaryId : secondaryOfPrimarySet) {
        primarySecondaryCounter.increment(secondaryId);
      }
    }

    long maxCount;
    long minCount;
    /*
     * verify average distribution
     */
    //verify necessary primary max and min count
    maxCount = primaryIdCounter.maxValue();
    minCount = primaryIdCounter.minValue();
    if (maxCount - minCount > pdiffCount) {
      logger.error("primary average distribute failed ! maxCount:{}, minCount:{}", maxCount,
          minCount);
    }
    if (maxCount - minCount > pdiffCount) {
      return false;
    }

    //verify necessary secondary max and min count
    maxCount = secondaryIdCounter.maxValue();
    minCount = secondaryIdCounter.minValue();
    if (maxCount - minCount > sdiffCount) {
      logger.error("secondary average distribute failed ! maxCount:{}, minCount:{}", maxCount,
          minCount);
    }
    if (maxCount - minCount > sdiffCount) {
      return false;
    }

    //verify necessary arbiter max and min count
    if (arbiterIdCounter.size() > 0) {
      maxCount = arbiterIdCounter.maxValue();
      minCount = arbiterIdCounter.minValue();
      if (maxCount - minCount > afiffCount) {
        logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}", maxCount,
            minCount);
      }
      if (maxCount - minCount > afiffCount) {
        return false;
      }
    }

    /*
     * verify rebalance when P down
     */
    for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()) {
      ObjectCounter<Long> secondaryCounterTemp = entry.getValue();
      maxCount = secondaryCounterTemp.maxValue();
      minCount = secondaryCounterTemp.minValue();

      if (maxCount - minCount > pdownDiffCount) {
        logger.error("rebalance failed when P down! maxCount:{}, minCount:{}, entry:{}", maxCount,
            minCount, entry);
      }
      if (maxCount - minCount > pdownDiffCount) {
        return false;
      }
    }
    return true;
  }

  public boolean verifyResult(VolumeMetadata volume,
      SimulateInstanceBuilder simulateInstanceBuilder, long afiffCount,
      long maxNoBalanceSecondary) {
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

    for (int segIndex : volume.getSegmentTable().keySet()) {
      SegmentMetadata segmentMetadata = volume.getSegmentTable().get(segIndex);

      Map<InstanceId, SegmentUnitMetadata> segmentUnitMap = segmentMetadata
          .getSegmentUnitMetadataTable();
      Set<Long> secondaryOfPrimarySet = new HashSet<>();
      long primaryId = -1;
      Set<Long> segmentUnitInstance = new HashSet<>();
      for (InstanceId insId : segmentUnitMap.keySet()) {
        SegmentUnitMetadata segmentUnitMetadata = segmentUnitMap.get(insId);

        if (segmentUnitInstance.contains(insId.getId())) {
          logger.error("segment used same group: volume type:{} P:{} S:{} A:{}",
              volume.getVolumeType(), segmentUnitMetadata.getMembership().getPrimary(),
              segmentUnitMetadata.getMembership().getSecondaries(),
              segmentUnitMetadata.getMembership().getArbiters());
          return false;
        }
        segmentUnitInstance.add(insId.getId());

        if (segmentUnitMetadata.getMembership().isArbiter(insId)) {
          arbiterIdCounter.increment(insId.getId());
        } else if (segmentUnitMetadata.getMembership().isPrimary(insId)) {
          primaryId = insId.getId();
          primaryIdCounter.increment(insId.getId());
        } else if (segmentUnitMetadata.getMembership().isSecondary(insId)) {
          secondaryIdCounter.increment(insId.getId());
          secondaryOfPrimarySet.add(insId.getId());
        }
      }

      ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap
          .computeIfAbsent(primaryId, value -> new TreeSetObjectCounter<>());
      //count necessary secondary
      for (long secondaryId : secondaryOfPrimarySet) {
        primarySecondaryCounter.increment(secondaryId);
      }
    }

    Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap = simulateInstanceBuilder
        .getInstanceId2SimulateInstanceMap();

    Map<Long, SimulateInstanceInfo> canUsedInstanceId2SimulateInstanceMap = simulateInstanceBuilder
        .removeCanNotUsedInstance(volume.getVolumeType());

    int totalWeight = 0;
    for (SimulateInstanceInfo instance : canUsedInstanceId2SimulateInstanceMap.values()) {
      totalWeight += instance.getWeight();
    }

    long secondaryCountPerSegment = volume.getVolumeType().getNumSecondaries();

    boolean ret = true;
    long diffCountMax = 3;
    long diffRatioMax = 3;
    /*
     * verify average distribution
     */
    //verify necessary primary max and min count
    for (SimulateInstanceInfo instance : canUsedInstanceId2SimulateInstanceMap.values()) {
      long factCount = primaryIdCounter.get(instance.getInstanceId().getId());
      long expiredCount = (long) (volume.getSegmentCount() * (double) instance.getWeight()
          / (double) totalWeight);

      double expiredRatio = (double) instance.getWeight() / (double) totalWeight * 100;
      double factRatio = (double) factCount / (double) volume.getSegmentCount() * 100;
      if (Math.abs(expiredRatio - factRatio) > diffRatioMax
          && Math.abs(expiredCount - factCount) > diffCountMax) {
        logger.error(
            "primary:{} average distribute failed ! expiredRatio*100:{}, factRatio*100:{}, "
                + "expiredCount:{}, factCount:{}",
            instance.getInstanceId(), expiredRatio, factRatio, expiredCount, factCount);
        ret = false;
      }
    }
    //verify necessary secondary max and min count
    for (SimulateInstanceInfo instance : canUsedInstanceId2SimulateInstanceMap.values()) {
      long factCount = secondaryIdCounter.get(instance.getInstanceId().getId());
      long expiredCount = (long) (
          volume.getSegmentCount() * secondaryCountPerSegment * (double) instance.getWeight()
              / (double) totalWeight);

      double expiredRatio = (double) instance.getWeight() / (double) totalWeight * 100;
      double factRatio =
          (double) factCount / (double) (volume.getSegmentCount() * secondaryCountPerSegment) * 100;
      if (Math.abs(expiredRatio - factRatio) > diffRatioMax
          && Math.abs(expiredCount - factCount) > diffCountMax) {
        logger.error(
            "secondary:{} average distribute failed ! expiredRatio*100:{}, factRatio*100:{}, "
                + "expiredCount:{}, factCount:{}",
            instance.getInstanceId(), expiredRatio, factRatio, expiredCount, factCount);
        //return false;
      }
    }

    //verify necessary arbiter max and min count
    if (arbiterIdCounter.size() > 0) {
      long maxCount = arbiterIdCounter.maxValue();
      long minCount = arbiterIdCounter.minValue();
      if (afiffCount >= 0) {
        if (maxCount - minCount > afiffCount) {
          logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}",
              maxCount, minCount);
        }
        if (maxCount - minCount > afiffCount) {
          ret = false;
        }
      }
    }

    /*
     * verify rebalance when P down
     */
    Set<Long> noBalanceSecondarySet = new HashSet<>();
    for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()) {
      long primaryCount = primaryIdCounter.get(entry.getKey());
      ObjectCounter<Long> secondaryCounterTemp = entry.getValue();

      int secondaryTotalWeight = 0;
      for (long instanceId : secondaryCounterTemp.getAll()) {
        SimulateInstanceInfo instance = instanceId2SimulateInstanceMap.get(instanceId);
        secondaryTotalWeight += instance.getWeight();
      }

      for (long instanceId : secondaryCounterTemp.getAll()) {
        SimulateInstanceInfo instance = instanceId2SimulateInstanceMap.get(instanceId);
        long factCount = secondaryCounterTemp.get(instance.getInstanceId().getId());
        long expiredCount = (long) (
            primaryCount * secondaryCountPerSegment * (double) instance.getWeight()
                / (double) secondaryTotalWeight);

        double expiredRatio = (double) instance.getWeight() / (double) secondaryTotalWeight * 100;
        double factRatio =
            (double) factCount / (double) (primaryCount * secondaryCountPerSegment) * 100;
        if (Math.abs(expiredRatio - factRatio) > diffRatioMax
            && Math.abs(expiredCount - factCount) > diffCountMax) {
          logger.error(
              "secondary:{} of primary:{} average distribute failed ! expiredRatio*100:{}, "
                  + "factRatio*100:{}, expiredCount:{}, factCount:{}",
              instanceId, entry.getKey(), expiredRatio, factRatio, expiredCount, factCount);
          noBalanceSecondarySet.add(instanceId);
          ret = false;
        }
      }
    }
    if (maxNoBalanceSecondary == -1) {
      maxNoBalanceSecondary = canUsedInstanceId2SimulateInstanceMap.size() / 2;
    }
    if (!ret && noBalanceSecondarySet.size() <= maxNoBalanceSecondary) {
      ret = true;
    }
    return ret;
  }

  public boolean verifyResult(SimulateVolume simulateVolume,
      SimulateInstanceBuilder simulateInstanceBuilder, long afiffCount,
      long maxNoBalanceSecondary) {
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();

    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

    for (int segIndex : simulateVolume.getSegIndex2SimulateSegmentMap().keySet()) {
      SimulateSegment segmentMetadata = simulateVolume.getSegIndex2SimulateSegmentMap()
          .get(segIndex);

      Map<InstanceId, SimulateSegmentUnit> segmentUnitMap = segmentMetadata
          .getInstanceId2SimulateSegUnitMap();

      Set<Long> secondaryOfPrimarySet = new HashSet<>();
      long primaryId = -1;
      Set<Long> segmentUnitInstance = new HashSet<>();
      for (InstanceId insId : segmentUnitMap.keySet()) {
        SimulateSegmentUnit segmentUnitMetadata = segmentUnitMap.get(insId);

        if (segmentUnitInstance.contains(insId.getId())) {
          logger.error("segment used same group: volume type:{} P:{} S:{} A:{}",
              simulateVolume.getVolumeType(), segmentMetadata.getPrimaryId(),
              segmentMetadata.getSecondaryIdSet(), segmentMetadata.getArbiterIdSet());
          return false;
        }
        segmentUnitInstance.add(insId.getId());

        if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Arbiter) {
          arbiterIdCounter.increment(insId.getId());
        } else if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Primary) {
          primaryId = insId.getId();
          primaryIdCounter.increment(insId.getId());
        } else if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Secondary) {
          secondaryIdCounter.increment(insId.getId());
          secondaryOfPrimarySet.add(insId.getId());
        }
      }

      ObjectCounter<Long> primarySecondaryCounter = primaryId2SecondaryIdCounterMap
          .computeIfAbsent(primaryId, value -> new TreeSetObjectCounter<>());
      //count necessary secondary
      for (long secondaryId : secondaryOfPrimarySet) {
        primarySecondaryCounter.increment(secondaryId);
      }
    }

    Map<Long, SimulateInstanceInfo> instanceId2SimulateInstanceMap = simulateInstanceBuilder
        .getInstanceId2SimulateInstanceMap();
    Map<Long, SimulateInstanceInfo> canUsedInstanceId2SimulateInstanceMap = simulateInstanceBuilder
        .removeCanNotUsedInstance(simulateVolume.getVolumeType());

    int totalWeight = 0;
    for (SimulateInstanceInfo instance : canUsedInstanceId2SimulateInstanceMap.values()) {
      totalWeight += instance.getWeight();
    }

    long secondaryCountPerSegment = simulateVolume.getVolumeType().getNumSecondaries();

    boolean ret = true;
    long diffCountMax = 3;
    long diffRatioMax = 3;
    /*
     * verify average distribution
     */
    //verify necessary primary max and min count
    for (SimulateInstanceInfo instance : canUsedInstanceId2SimulateInstanceMap.values()) {
      long factCount = primaryIdCounter.get(instance.getInstanceId().getId());
      long expiredCount = (long) (simulateVolume.getSegmentCount() * (double) instance.getWeight()
          / (double) totalWeight);

      double expiredRatio = (double) instance.getWeight() / (double) totalWeight * 100;
      double factRatio = (double) factCount / (double) simulateVolume.getSegmentCount() * 100;
      if (Math.abs(expiredRatio - factRatio) > diffRatioMax
          && Math.abs(expiredCount - factCount) > diffCountMax) {
        logger.error(
            "primary:{} average distribute failed ! expiredRatio*100:{}, factRatio*100:{}, "
                + "expiredCount:{}, factCount:{}",
            instance.getInstanceId(), expiredRatio, factRatio, expiredCount, factCount);
        ret = false;
      }
    }
    //verify necessary secondary max and min count
    for (SimulateInstanceInfo instance : canUsedInstanceId2SimulateInstanceMap.values()) {
      long factCount = secondaryIdCounter.get(instance.getInstanceId().getId());
      long expiredCount = (long) (
          simulateVolume.getSegmentCount() * secondaryCountPerSegment * (double) instance
              .getWeight() / (double) totalWeight);

      double expiredRatio = (double) instance.getWeight() / (double) totalWeight * 100;
      double factRatio = (double) factCount / (double) (simulateVolume.getSegmentCount()
          * secondaryCountPerSegment) * 100;
      if (Math.abs(expiredRatio - factRatio) > diffRatioMax
          && Math.abs(expiredCount - factCount) > diffCountMax) {
        logger.error(
            "secondary:{} average distribute failed ! expiredRatio*100:{}, factRatio*100:{}, "
                + "expiredCount:{}, factCount:{}",
            instance.getInstanceId(), expiredRatio, factRatio, expiredCount, factCount);
        //return false;
      }
    }

    //verify necessary arbiter max and min count
    if (arbiterIdCounter.size() > 0) {
      long maxCount = arbiterIdCounter.maxValue();
      long minCount = arbiterIdCounter.minValue();
      if (afiffCount >= 0) {
        if (maxCount - minCount > afiffCount) {
          logger.error("arbiter average distribute failed ! maxCount:{}, minCount:{}",
              maxCount, minCount);
        }

        if (maxCount - minCount > afiffCount) {
          ret = false;
        }
      }
    }

    /*
     * verify rebalance when P down
     */
    Set<Long> noBalanceSecondarySet = new HashSet<>();
    for (Map.Entry<Long, ObjectCounter<Long>> entry : primaryId2SecondaryIdCounterMap.entrySet()) {
      long primaryCount = primaryIdCounter.get(entry.getKey());
      ObjectCounter<Long> secondaryCounterTemp = entry.getValue();

      int secondaryTotalWeight = 0;
      for (long instanceId : secondaryCounterTemp.getAll()) {
        SimulateInstanceInfo instance = instanceId2SimulateInstanceMap.get(instanceId);
        secondaryTotalWeight += instance.getWeight();
      }

      for (long instanceId : secondaryCounterTemp.getAll()) {
        SimulateInstanceInfo instance = instanceId2SimulateInstanceMap.get(instanceId);
        long factCount = secondaryCounterTemp.get(instance.getInstanceId().getId());
        long expiredCount = (long) (
            primaryCount * secondaryCountPerSegment * (double) instance.getWeight()
                / (double) secondaryTotalWeight);

        double expiredRatio = (double) instance.getWeight() / (double) secondaryTotalWeight * 100;
        double factRatio =
            (double) factCount / (double) (primaryCount * secondaryCountPerSegment) * 100;
        if (Math.abs(expiredRatio - factRatio) > diffRatioMax
            && Math.abs(expiredCount - factCount) > diffCountMax) {
          logger.error(
              "secondary:{} of primary:{} average distribute failed ! expiredRatio*100:{}, "
                  + "factRatio*100:{}, expiredCount:{}, factCount:{}",
              instanceId, entry.getKey(), expiredRatio, factRatio, expiredCount, factCount);
          noBalanceSecondarySet.add(instanceId);
          ret = false;
        }
      }
    }

    if (maxNoBalanceSecondary == -1) {
      maxNoBalanceSecondary = canUsedInstanceId2SimulateInstanceMap.size() / 2;
    }

    if (!ret && noBalanceSecondarySet.size() <= maxNoBalanceSecondary) {
      ret = true;
    }

    return ret;
  }

  private class StoragePoolMemStore extends StoragePoolStoreImpl {

    @Override
    public void saveStoragePoolToDb(StoragePool storagePool) {
      // not saving
    }

    @Override
    public StoragePool getStoragePoolFromDb(Long storagePoolId) throws SQLException, IOException {
      return null;
    }

    @Override
    public void reloadAllStoragePoolsFromDb() throws SQLException, IOException {
    }

    @Override
    public void deleteStoragePoolFromDb(Long storagePoolId) {
    }
  }

  class PoolTask extends TimerTask {

    InstanceMetadata instanceMetadata;
    int maxExecTime;
    double failRatio;
    long runTimes = 0;

    public PoolTask() {
    }

    public PoolTask(InstanceMetadata instanceMetadata, int maxExecTime, double failRatio) {
      this.instanceMetadata = instanceMetadata;
      this.maxExecTime = maxExecTime;
      this.failRatio = failRatio;
    }

    public long getRunTimes() {
      return runTimes;
    }

    @Override
    public void run() {
      runTimes++;
      try {
        pollAndExecTask(instanceMetadata, maxExecTime, failRatio);

        if (!distributionManager.hasAnyRebalanceTasks()) {
          semaphore.release();
          cancel();
        }
      } catch (Exception e) {
        logger.error("caught a exception, ", e);
        semaphore.release();
        assertTrue(false);
      }
    }
  }

}
