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

package py.infocenter.rebalance.struct;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentMetadata;
import py.icshare.InstanceMetadata;
import py.infocenter.store.StorageStore;
import py.informationcenter.StoragePool;
import py.instance.InstanceId;
import py.volume.VolumeMetadata;

/**
 * simple pool information.
 */
public class SimulatePool {

  private Logger logger = LoggerFactory.getLogger(SimulatePool.class);

  private long poolId;
  private long domainId;
  private Multimap<Long, Long> archivesInDataNode;    // <datanodeId, archiveId>
  private Set<Long> volumeIds;



  public SimulatePool(StoragePool storagePool) {
    this.poolId = storagePool.getPoolId();
    this.domainId = storagePool.getDomainId();
    this.archivesInDataNode = HashMultimap.create(storagePool.getArchivesInDataNode());
    this.volumeIds = new HashSet<>(storagePool.getVolumeIds());
  }

  public Multimap<Long, Long> getArchivesInDataNode() {
    return archivesInDataNode;
  }

  public void setArchivesInDataNode(Multimap<Long, Long> archivesInDataNode) {
    this.archivesInDataNode = archivesInDataNode;
  }

  /**
   * Determine whether the volume environment is changed(For simple judge, we don't care mix
   * group).
   *
   * @param storagePool    newest pool information
   * @param volumeMetadata volume info
   * @return PoolChangedStatus
   */
  public PoolChangedStatus isPoolEnvChanged(StoragePool storagePool,
      VolumeMetadata volumeMetadata) {
    if (storagePool.getPoolId() != poolId) {
      logger.warn("pool id changed, it means storage pool changed and archive was decreased!");
      return PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME;
    }

    if (storagePool.getDomainId() != domainId) {
      logger.warn(
          "pool's domain id changed, it means storage pool changed and archive was decreased!");
      return PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME;
    }

    PoolChangedStatus poolChangedStatus = PoolChangedStatus.NO_CHANGE;
    Multimap<Long, Long> archivesInDataNodeInStoragePool = storagePool.getArchivesInDataNode();

    //get datanode which distributed segment unit
    Set<InstanceId> volumeDatanodeSet = new HashSet<>();
    for (SegmentMetadata segmentMetadata : volumeMetadata.getSegmentTable().values()) {
      volumeDatanodeSet.addAll(segmentMetadata.getSegmentUnitMetadataTable().keySet());
    }
    Set<Long> volumeDatanodeIdSet = new HashSet<>();
    for (InstanceId instanceId : volumeDatanodeSet) {
      volumeDatanodeIdSet.add(instanceId.getId());
    }

    if (archivesInDataNode.size() != archivesInDataNodeInStoragePool.size()) {
      poolChangedStatus = PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME;
    }

    for (long datanodeId : archivesInDataNode.keySet()) {
      boolean hasPartOfVolume = volumeDatanodeIdSet.contains(datanodeId);
      Set<Long> archivesInSimulateSet = new HashSet<>(archivesInDataNode.get(datanodeId));
      Set<Long> newestArchivesSet = new HashSet<>(archivesInDataNodeInStoragePool.get(datanodeId));
      Set<Long> retainSet = new HashSet<>(archivesInSimulateSet);
      retainSet.retainAll(newestArchivesSet);

      if (retainSet.size() < archivesInSimulateSet.size()) {
        if (hasPartOfVolume) {
          //if any archives which was distributed segment unit was pull from pool
          poolChangedStatus = PoolChangedStatus.ARCHIVE_DECREASE_AND_RELATED_VOLUME;
          logger.warn(
              "pool environment changed status:{}, have some archives(distributed segment) lost",
              poolChangedStatus);
          break;
        } else {
          poolChangedStatus = PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME;
          logger.warn(
              "pool environment changed status:{}, have some archives(not distributed segment) "
                  + "lost",
              poolChangedStatus);
        }
      } else if (retainSet.size() < newestArchivesSet.size()) {
        //has some archive add to pool
        poolChangedStatus = PoolChangedStatus.CHANGED_BUT_NO_RELATED_VOLUME;
        logger.warn(
            "pool environment changed status:{}, have some archives(not distributed segment) add",
            poolChangedStatus);
      }
    }

    logger.debug(
        "pool environment changed status:{}, volume datanode id:{}, archivesInDataNode:{}, "
            + "archivesInDataNodeInStoragePool:{}",
        poolChangedStatus, volumeDatanodeIdSet, archivesInDataNode,
        archivesInDataNodeInStoragePool);

    return poolChangedStatus;
  }

  /**
   * xx.
   *
   * @param storageStore           storage store db
   * @param simpleDatanodeManager  all simple datanode manager object
   * @param normalIdSet            all normal data node in pool
   * @param simpleIdSet            all simple data node in pool
   * @param simpleGroupIdSet       all simple group in pool
   * @param normalGroupSet         all normal data node group
   * @param instanceId2InstanceMap all instance information in  pool
   */
  public void getDatanodeInfo(StorageStore storageStore,
      SimpleDatanodeManager simpleDatanodeManager,
      Set<Long> normalIdSet, Set<Long> simpleIdSet,
      Set<Integer> normalGroupSet, Set<Integer> simpleGroupIdSet,
      Set<Integer> mixGroupIdSet, Map<Long, InstanceMetadata> instanceId2InstanceMap) {
    //simpleDatanode may be have no archives, so it cannot add to pool
    //we will create arbiter at all simpleDatanode which be owned domain
    Set<Long> allSimpleDatanodeIdSet = simpleDatanodeManager.getSimpleDatanodeInstanceIdSet();

    Set<Integer> normalGroupSetBac = new HashSet<>();
    Set<Integer> simpleGroupIdSetBac = new HashSet<>();

    long domainId = -1;
    // for (simpleDatanodeManager.getSimpleDatanodeGroupIdSet())
    // initialize group set and simple datanode hosts
    for (InstanceMetadata instanceMetadata : storageStore.list()) {
      if (instanceMetadata.getDomainId() != null) {
        domainId = instanceMetadata.getDomainId();
      }

      if (instanceId2InstanceMap != null) {
        instanceId2InstanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
      }

      if (!allSimpleDatanodeIdSet.contains(instanceMetadata.getInstanceId().getId())) {
        normalGroupSetBac.add(instanceMetadata.getGroup().getGroupId());
        if (normalIdSet != null) {
          normalIdSet.add(instanceMetadata.getInstanceId().getId());
        }
      }
    }

    for (long simpleDatanodeInstanceId : allSimpleDatanodeIdSet) {
      InstanceMetadata instanceMetadata = storageStore.get(simpleDatanodeInstanceId);

      if (instanceMetadata.getDomainId() != null && instanceMetadata.getDomainId() == domainId) {
        if (instanceId2InstanceMap != null) {
          instanceId2InstanceMap.put(instanceMetadata.getInstanceId().getId(), instanceMetadata);
        }

        simpleGroupIdSetBac.add(instanceMetadata.getGroup().getGroupId());
        if (simpleIdSet != null) {
          simpleIdSet.add(simpleDatanodeInstanceId);
        }
      }
    }

    if (normalGroupSet == null && simpleGroupIdSet == null && mixGroupIdSet == null) {
      return;
    }

    Set<Integer> mixGroupIdSetBac = new HashSet<>(normalGroupSetBac);
    mixGroupIdSetBac.retainAll(simpleGroupIdSetBac);

    normalGroupSetBac.removeAll(mixGroupIdSetBac);
    simpleGroupIdSetBac.removeAll(mixGroupIdSetBac);

    if (normalGroupSet != null) {
      normalGroupSet.addAll(normalGroupSetBac);
    }
    if (simpleGroupIdSet != null) {
      simpleGroupIdSet.addAll(simpleGroupIdSetBac);
    }
    if (mixGroupIdSet != null) {
      mixGroupIdSet.addAll(mixGroupIdSetBac);
    }
  }

  @Override
  public String toString() {
    return "SimulatePool{"

        + "poolId=" + poolId

        + ", domainId=" + domainId

        + ", archivesInDataNode=" + archivesInDataNode

        + ", volumeIds=" + volumeIds

        + '}';
  }


  public enum PoolChangedStatus {
    NO_CHANGE,
    ARCHIVE_DECREASE_AND_RELATED_VOLUME,

    CHANGED_BUT_NO_RELATED_VOLUME,


  }
}
