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

package py.infocenter.worker;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.common.PyService;
import py.common.Utils;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceInformation;
import py.icshare.InstanceMetadata;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolLevel;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.monitor.common.CounterName;
import py.monitor.common.OperationName;
import py.monitor.common.UserDefineName;
import py.periodic.Worker;
import py.querylog.eventdatautil.EventDataWorker;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * check instance metadata periodic, if it is timeout, then i will remove the instance metadata.
 *
 */
public class StorageStoreSweeper implements Worker {

  private static final Logger logger = LoggerFactory.getLogger(StorageStoreSweeper.class);
  private final long generateAlertValue = 0L;
  private final long recoveryAlertValue = 1L;
  private StorageStore storageStore;
  private int timeToRemove;
  private AppContext appContext;
  private StoragePoolStore storagePoolStore;
  private DomainStore domainStore;
  private VolumeStore volumeStore;
  private long segmentSize;
  private InstanceMaintenanceDbStore instanceMaintenanceDbStore;
  private int waitCollectVolumeInfoSecond = 30;
  private long startServerTimestampMs;

  @Override
  public void doWork() throws Exception {
    // check the service is working right, maybe become SUSPEND
    if (appContext.getStatus() == InstanceStatus.SUSPEND) {
      logger.info("I am SUSPEND now, clear all datanodes' metadata:{}", storageStore);
      // need clear the cache data
      storageStore.clearMemoryData();
      logger.info("reset storage sweeper start work timestamp");
      startServerTimestampMs = 0;
      return;
    }

    if (startServerTimestampMs == 0) {
      startServerTimestampMs = System.currentTimeMillis();
      logger.warn("mark storage sweeper start work timestamp:{}", startServerTimestampMs);
    }

    List<InstanceMaintenanceInformation> instanceMaintenanceDbStoreList = instanceMaintenanceDbStore
        .listAll();

    for (InstanceMaintenanceInformation info : instanceMaintenanceDbStoreList) {
      if (info != null) {
        if (System.currentTimeMillis() > info.getEndTime()) {
          instanceMaintenanceDbStore.delete(info);
        }
      }
    }

    Multimap<Long, Integer> domainId2SimpleGroupIdMap = HashMultimap.create();
    Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
    Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
    long sysTime = System.currentTimeMillis();
    for (InstanceMetadata instance : storageStore.list()) {
      if (sysTime - instance.getLastUpdated() > timeToRemove) {
        logger.warn("mark the not report instance metadata:{}", instance.getEndpoint());
        instance.setDatanodeStatus(InstanceMetadata.DatanodeStatus.UNKNOWN);

        //change archives status to UNKNOWN
        for (RawArchiveMetadata rawArchiveMetadata : instance.getArchives()) {
          rawArchiveMetadata.setStatus(ArchiveStatus.UNKNOWN);
        }

        storageStore.save(instance);
      }

      if (instance.getDatanodeStatus().equals(InstanceMetadata.DatanodeStatus.OK)
          || instanceMaintenanceDbStore.getById(instance.getInstanceId().getId()) != null) {
        instanceId2InstanceMetadata.put(instance.getInstanceId().getId(), instance);
        for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
          //when archives changed to not good, pool level will be changed
          if (archiveMetadata.getStatus() == ArchiveStatus.GOOD) {
            archiveId2Archive.put(archiveMetadata.getArchiveId(), archiveMetadata);
          }
        }
      }

      if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE
          && instance.getDomainId() != null) {
        domainId2SimpleGroupIdMap.put(instance.getDomainId(), instance.getGroup().getGroupId());
      }
    } // for loop datanode

    // wait several seconds(waitCollectVolumeInfoSecond) from infocenter become OK status, then 
    // start to calc pool info and output event data
    if ((System.currentTimeMillis() - startServerTimestampMs) < (this.waitCollectVolumeInfoSecond
        * 1000)) {
      logger.warn("info center just start server at:{}, wait {} second",
          Utils.millsecondToString(startServerTimestampMs), this.waitCollectVolumeInfoSecond);
      return;
    }

    HashMap<Long, Integer> poolId2MaxSegmentUnitPerSegmentMap = new HashMap<>();

    HashMap<Long, Integer> poolId2MaxNormalSegmentUnitPerSegmentMap = new HashMap<>();

    ObjectCounter<Long> storagePoolVolumeCounter = new TreeSetObjectCounter<>();
    for (VolumeMetadata volume : volumeStore.listVolumes()) {
      if (volume.getVolumeStatus() != VolumeStatus.Deleting
          && volume.getVolumeStatus() != VolumeStatus.Deleted
          && volume.getVolumeStatus() != VolumeStatus.Dead) {
        storagePoolVolumeCounter.increment(volume.getStoragePoolId());
      }

      if (volume.getVolumeStatus() != VolumeStatus.Dead) {
        // all segment unit count
        int maxSegmentUnitPerSegment = volume.getVolumeType().getNumMembers();
        if (poolId2MaxSegmentUnitPerSegmentMap.containsKey(volume.getStoragePoolId())) {
          maxSegmentUnitPerSegment = Math
              .max(poolId2MaxSegmentUnitPerSegmentMap.get(volume.getStoragePoolId()),
                  maxSegmentUnitPerSegment);
        }
        poolId2MaxSegmentUnitPerSegmentMap.put(volume.getStoragePoolId(), maxSegmentUnitPerSegment);

        // normal segment unit count
        int maxNormalSegmentUnitPerSegment =
            volume.getVolumeType().getNumMembers() - volume.getVolumeType().getNumArbiters();
        if (poolId2MaxNormalSegmentUnitPerSegmentMap.containsKey(volume.getStoragePoolId())) {
          maxNormalSegmentUnitPerSegment = Math
              .max(poolId2MaxNormalSegmentUnitPerSegmentMap.get(volume.getStoragePoolId()),
                  maxNormalSegmentUnitPerSegment);
        }
        poolId2MaxNormalSegmentUnitPerSegmentMap
            .put(volume.getStoragePoolId(), maxNormalSegmentUnitPerSegment);
      }
    }

    // calculate storage pool total space and free space, at the same time, count the group
    // number in the storage pool, write these information to event data
    for (StoragePool storagePool : storagePoolStore.listAllStoragePools()) {
      final long poolId = storagePool.getPoolId();
      long logicalSpace = 0;
      long logicalFreeSpace = 0;
      final long logicalPssFreeSpace = StoragePoolSpaceCalculator.calculateFreeSpace(storagePool,
          instanceId2InstanceMetadata, archiveId2Archive,
          VolumeType.REGULAR.getNumSecondaries() + 1, segmentSize);
      final long logicalPsaFreeSpace = StoragePoolSpaceCalculator.calculateFreeSpace(storagePool,
          instanceId2InstanceMetadata, archiveId2Archive, VolumeType.SMALL.getNumSecondaries() + 1,
          segmentSize);
      Set<Integer> groupSet = new HashSet<>();
      Set<Integer> simpleGroupSet = new HashSet<>();
      Set<Integer> normalGroupSet = new HashSet<>();
      boolean archiveLost = false;
      boolean rebuildFailed = false;

      for (Long archiveId : storagePool.getArchivesInDataNode().values()) {
        RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
        if (archiveMetadata != null) {
          logicalSpace += archiveMetadata.getLogicalSpace();
          logicalFreeSpace += archiveMetadata.getLogicalFreeSpace();

          InstanceMetadata instance = instanceId2InstanceMetadata
              .get(archiveMetadata.getInstanceId().getId());
          groupSet.add(instance.getGroup().getGroupId());

          if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.SIMPLE) {
            simpleGroupSet.add(instance.getGroup().getGroupId());
          } else if (instance.getDatanodeType() == InstanceMetadata.DatanodeType.NORMAL) {
            normalGroupSet.add(instance.getGroup().getGroupId());
          } else {
            logger.warn("instance id:{} endpoint:{} DatanodeType not set!",
                instance.getInstanceId().getId(), instance.getEndpoint());
          }

          if (!archiveMetadata.drainAllMigrateFailedSegIds().isEmpty()) {
            rebuildFailed = true;
          }
        } else {
          // cannot find one archive in the storage pool
          archiveLost = true;
        }
      } // for loop archive in one storage pool

      storagePool.setTotalSpace(logicalSpace);
      storagePool.setFreeSpace(logicalFreeSpace);
      storagePool.setLogicalPssFreeSpace(logicalPssFreeSpace);
      storagePool.setLogicalPsaFreeSpace(logicalPsaFreeSpace);

      // if storage pool has no group(means has not add any archive yet), do not generate query log
      if (groupSet.isEmpty() || storagePool.isDeleting()) {
        continue;
      }

      Map<String, String> userDefineParams = new HashMap<>();
      userDefineParams.put(UserDefineName.StoragePoolID.name(), String.valueOf(poolId));
      userDefineParams.put(UserDefineName.StoragePoolName.name(), storagePool.getName());
      final EventDataWorker eventDataWorker = new EventDataWorker(PyService.INFOCENTER,
          userDefineParams);

      long freeSpaceRatio = logicalSpace == 0 ? 0 : (logicalFreeSpace * 100) / logicalSpace;
      long availablePssSegmentCount = logicalPssFreeSpace / segmentSize;
      long availablePsaSegmentCount = logicalPsaFreeSpace / segmentSize;

      Map<String, Long> counters = new HashMap<>();
      counters.put(CounterName.STORAGEPOOL_FREE_SPACE_RATIO.name(), freeSpaceRatio);
      counters.put(CounterName.STORAGEPOOL_AVAILABLE_PSS_SEGMENT_COUNT.name(),
          availablePssSegmentCount);
      counters.put(CounterName.STORAGEPOOL_AVAILABLE_PSA_SEGMENT_COUNT.name(),
          availablePsaSegmentCount);
      counters.put(CounterName.STORAGEPOOL_GROUP_AMOUNT.name(), Long.valueOf(groupSet.size()));
      counters
          .put(CounterName.STORAGEPOOL_VOLUME_AMOUNT.name(), storagePoolVolumeCounter.get(poolId));

      //get the max number of arbiter segment unit count per volume segment in current pool,
      //when has no volume, default volume is SMALL
      int maxAllSegmentUnitPerSegment = VolumeType.SMALL.getNumMembers();
      if (poolId2MaxSegmentUnitPerSegmentMap.containsKey(storagePool.getPoolId())) {
        maxAllSegmentUnitPerSegment = poolId2MaxSegmentUnitPerSegmentMap
            .get(storagePool.getPoolId());
      }
      //get the max number of normal segment unit count per volume segment in current pool,
      //when has no volume, default volume is SMALL
      int maxNormalSegmentUnitPerSegment =
          VolumeType.SMALL.getNumMembers() - VolumeType.SMALL.getNumArbiters();
      if (poolId2MaxNormalSegmentUnitPerSegmentMap.containsKey(storagePool.getPoolId())) {
        maxNormalSegmentUnitPerSegment = poolId2MaxNormalSegmentUnitPerSegmentMap
            .get(storagePool.getPoolId());
      }

      //group counter is or not enough to create segment unit
      simpleGroupSet.addAll(domainId2SimpleGroupIdMap.get(storagePool.getDomainId()));
      boolean isGroupEnough = isGroupEnoughToCreateSegment(simpleGroupSet, normalGroupSet,
          maxAllSegmentUnitPerSegment, maxNormalSegmentUnitPerSegment);

      logger.info("pool:{} rebuildFail:{}, archiveLost:{}, isGroupEnough:{}",
          storagePool.getName(), rebuildFailed, archiveLost, isGroupEnough);

      storagePool.setStoragePoolLevel(StoragePoolLevel.HIGH.name());

      if (archiveLost) {
        storagePool.setStoragePoolLevel(StoragePoolLevel.MIDDLE.name());
      }

      if (!isGroupEnough || rebuildFailed) {
        storagePool.setStoragePoolLevel(StoragePoolLevel.LOW.name());
      }

      // event data for alert
      if (archiveLost) {
        counters.put(CounterName.STORAGEPOOL_LOST_DISK.name(), generateAlertValue);
      } else {
        counters.put(CounterName.STORAGEPOOL_LOST_DISK.name(), recoveryAlertValue);
      }

      if (rebuildFailed) {
        counters.put(CounterName.STORAGEPOOL_REBUILD_FAIL.name(), generateAlertValue);
      } else {
        counters.put(CounterName.STORAGEPOOL_REBUILD_FAIL.name(), recoveryAlertValue);
      }

      // check volumes' status in pool
      boolean allVolumeStable = true;
      Set<Long> volumeIds = storagePool.getVolumeIds();
      for (Long volumeId : volumeIds) {
        VolumeMetadata volumeMetadata = volumeStore.getVolume(volumeId);
        if (volumeMetadata == null) {
          continue;
        }

        if (volumeMetadata.getVolumeStatus() != VolumeStatus.Stable) {
          allVolumeStable = false;
          break;
        }
      }

      // the all volume in current pool is ok, the pool can rebuild
      if (allVolumeStable) {
        counters.put(CounterName.STORAGE_POOL_CANNOT_REBUILD.name(), recoveryAlertValue);
        logger.warn("find the pool :{} can rebuild info", storagePool.getName());
      }

      eventDataWorker.work(OperationName.StoragePool.name(), counters);


    }
    // for loop storage pools

    // calculate domain total space and free space
    for (Domain domain : domainStore.listAllDomains()) {
      long logicalSpace = 0;
      long freeSpace = 0;
      for (Long instanceId : domain.getDataNodes()) {
        InstanceMetadata instanceMetadata = instanceId2InstanceMetadata.get(instanceId);
        if (instanceMetadata != null && instanceMetadata.getDatanodeStatus()
            .equals(InstanceMetadata.DatanodeStatus.OK)) {
          logicalSpace += instanceMetadata.getLogicalCapacity();
          freeSpace += instanceMetadata.getFreeSpace();
        }
      }
      domain.setLogicalSpace(logicalSpace);
      domain.setFreeSpace(freeSpace);
    }
  }

  /**
   * group counter is or not enough to create segment unit.
   *
   * @param simpleGroupSetInPool           simple datanode group set
   * @param normalGroupSetInPool           normal datanode group set
   * @param maxSegmentUnitPerSegment       segment unit per segment must need
   * @param maxNormalSegmentUnitPerSegment normal segment unit per segment must need
   * @return true:when
   */
  private boolean isGroupEnoughToCreateSegment(Set<Integer> simpleGroupSetInPool,
      Set<Integer> normalGroupSetInPool,
      int maxSegmentUnitPerSegment, int maxNormalSegmentUnitPerSegment) {
    logger.info(
        "simpleGroupSetInPool:{}, normalGroupSetInPool:{}, maxSegmentUnitPerSegment:{}, "
            + "maxNormalSegmentUnitPerSegment:{}",
        simpleGroupSetInPool, normalGroupSetInPool, maxSegmentUnitPerSegment,
        maxNormalSegmentUnitPerSegment);
    Set<Integer> allGroupSet = new HashSet<>();
    allGroupSet.addAll(simpleGroupSetInPool);
    allGroupSet.addAll(normalGroupSetInPool);

    if (allGroupSet.size() < maxSegmentUnitPerSegment) {
      logger.warn("group is not enough, max segment unit:{}, but current group size:{}",
          maxSegmentUnitPerSegment, allGroupSet.size());
      return false;
    } else if (normalGroupSetInPool.size() < maxNormalSegmentUnitPerSegment) {
      logger.warn("group is not enough, normal segment unit:{}, but current normal group size:{}",
          maxNormalSegmentUnitPerSegment, normalGroupSetInPool.size());
      return false;
    }

    return true;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public StorageStore getStorageStore() {
    return storageStore;
  }

  public void setInstanceMetadataStore(StorageStore storageStore) {
    this.storageStore = storageStore;
  }

  public void setTimeToRemove(int timeToRemove) {
    this.timeToRemove = timeToRemove;
  }

  public StoragePoolStore getStoragePoolStore() {
    return storagePoolStore;
  }

  public void setStoragePoolStore(StoragePoolStore storagePoolStore) {
    this.storagePoolStore = storagePoolStore;
  }

  public DomainStore getDomainStore() {
    return domainStore;
  }

  public void setDomainStore(DomainStore domainStore) {
    this.domainStore = domainStore;
  }

  public VolumeStore getVolumeStore() {
    return volumeStore;
  }

  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }

  public long getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(long segmentSize) {
    this.segmentSize = segmentSize;
  }

  public InstanceMaintenanceDbStore getInstanceMaintenanceDbStore() {
    return instanceMaintenanceDbStore;
  }

  public void setInstanceMaintenanceDbStore(InstanceMaintenanceDbStore instanceMaintenanceDbStore) {
    this.instanceMaintenanceDbStore = instanceMaintenanceDbStore;
  }

  public void setWaitCollectVolumeInfoSecond(int waitCollectVolumeInfoSecond) {
    this.waitCollectVolumeInfoSecond = waitCollectVolumeInfoSecond;
  }

  /**
   * just for test.
   */
  public void setPoolIdMapEventDataWorker(Map<Long, EventDataWorker> poolIdMapEventDataWorker) {
    // do nothing
  }
}
