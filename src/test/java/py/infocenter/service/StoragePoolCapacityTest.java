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

package py.infocenter.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.icshare.InstanceMetadata.DatanodeStatus.OK;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import py.archive.RawArchiveMetadata;
import py.common.RequestIdBuilder;
import py.icshare.Domain;
import py.icshare.InstanceMetadata;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.test.utils.StorageMemStore;
import py.infocenter.worker.StoragePoolSpaceCalculator;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Group;
import py.instance.InstanceDomain;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.thrift.share.ListStoragePoolCapacityRequestThrift;
import py.thrift.share.ListStoragePoolCapacityResponseThrift;
import py.thrift.share.StoragePoolCapacityThrift;


public class StoragePoolCapacityTest extends TestBase {

  private StorageMemStore storageMemStore = new StorageMemStore();

  @Mock
  private StoragePoolStore storagePoolStore;

  private InformationCenterImpl informationCenterImpl;


  @Before
  public void init() throws Exception {
    super.init();
    InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    informationCenterImpl = new InformationCenterImpl();
    informationCenterImpl.setStorageStore(storageMemStore);
    informationCenterImpl.setStoragePoolStore(storagePoolStore);
    informationCenterImpl.setAppContext(appContext);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testcapacity() throws IOException, SQLException {
    storageMemStore.clearMemoryData();

    Long domainId = RequestIdBuilder.get();
    Long storagePoolId1 = RequestIdBuilder.get();
    final Long storagePoolId2 = RequestIdBuilder.get();
    List<Long> storagePoolIdFirst = new ArrayList<>();
    storagePoolIdFirst.add(storagePoolId1);
    final String storagePoolName1 = "STORAGEPOOLNAME1";
    final String storagePoolName2 = "STORAGEPOOLNAME2";
    Domain domain = mock(Domain.class);
    when(domain.getDomainId()).thenReturn(domainId);
    StoragePool storagePool1 = new StoragePool();
    storagePool1.setDomainId(domainId);
    storagePool1.setPoolId(storagePoolId1);
    storagePool1.setName(storagePoolName1);
    StoragePool storagePool2 = new StoragePool();
    storagePool2.setDomainId(domainId);
    storagePool2.setPoolId(storagePoolId2);
    storagePool2.setName(storagePoolName2);

    // for storage pool 1
    long[] totalCapacityArray1 = {3L, 2L, 5L, 3L, 4L, 10L, 3L, 6L, 7L, 9L};
    long[] freeCapacityArray1 = {2L, 1L, 3L, 2L, 4L, 5L, 0L, 3L, 5L, 6L};
    long totalCapacity1 = 0L;
    long freeCapacity1 = 0L;
    for (long capacity : totalCapacityArray1) {
      totalCapacity1 += capacity;
    }
    for (long capacity : freeCapacityArray1) {
      freeCapacity1 += capacity;
    }

    Multimap<Long, Long> archivesInDataNode1 = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    int index1 = 0;
    for (int i = 0; i < 5; i++) {
      InstanceDomain instanceDomain = new InstanceDomain(domainId);
      InstanceId datanodeId = new InstanceId((long) i);
      InstanceMetadata datanode = new InstanceMetadata(datanodeId);
      List<RawArchiveMetadata> archiveMetadataList1 = new ArrayList<>();
      for (int k = 0; k < 2; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId(Long.valueOf(k));
        archive.setInstanceId(datanodeId);
        archive.setStoragePoolId(storagePoolId1);
        archive.setLogicalSpace(totalCapacityArray1[index1]);
        archive.setLogicalFreeSpace(freeCapacityArray1[index1]);
        archiveMetadataList1.add(archive);
        archivesInDataNode1.put(Long.valueOf(i), Long.valueOf(k));
        index1++;
      }
      datanode.setArchives(archiveMetadataList1);
      datanode.setInstanceDomain(instanceDomain);
      datanode.setDatanodeStatus(OK);
      storageMemStore.save(datanode);
    }
    storagePool1.setArchivesInDataNode(archivesInDataNode1);

    // for storage pool 2
    long[] totalCapacityArray2 = {3L, 4L, 5L, 7L, 2L, 3L, 5L, 5L, 6L};
    long[] freeCapacityArray2 = {1L, 0L, 5L, 4L, 1L, 2L, 4L, 3L, 5L};
    long totalCapacity2 = 0L;
    long freeCapacity2 = 0L;
    for (long capacity : totalCapacityArray2) {
      totalCapacity2 += capacity;
    }
    for (long capacity : freeCapacityArray2) {
      freeCapacity2 += capacity;
    }

    Multimap<Long, Long> archivesInDataNode2 = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    int index2 = 0;
    for (int i = 1000; i < 1003; i++) {
      InstanceId datanodeId = new InstanceId((long) i);
      InstanceMetadata datanode = new InstanceMetadata(datanodeId);
      List<RawArchiveMetadata> archiveMetadataList = new ArrayList<>();
      for (int k = 1000; k < 1003; k++) {
        RawArchiveMetadata archive = new RawArchiveMetadata();
        archive.setArchiveId(Long.valueOf(k));
        archive.setInstanceId(datanodeId);
        archive.setStoragePoolId(storagePoolId2);
        archive.setLogicalSpace(totalCapacityArray2[index2]);
        archive.setLogicalFreeSpace(freeCapacityArray2[index2]);
        archiveMetadataList.add(archive);
        archivesInDataNode2.put(Long.valueOf(i), Long.valueOf(k));
        index2++;
      }
      datanode.setArchives(archiveMetadataList);
      datanode.setDatanodeStatus(OK);
      storageMemStore.save(datanode);
    }
    storagePool2.setArchivesInDataNode(archivesInDataNode2);

    // for storagePool list
    List<StoragePool> allStoragePools = new ArrayList<StoragePool>();
    List<StoragePool> firstStoragePool = new ArrayList<StoragePool>();
    allStoragePools.add(storagePool1);
    allStoragePools.add(storagePool2);
    firstStoragePool.add(storagePool1);
    when(storagePoolStore.listStoragePools(any(Long.class))).thenReturn(allStoragePools);
    when(storagePoolStore.listStoragePools(Mockito.anyList())).thenReturn(firstStoragePool);

    ListStoragePoolCapacityRequestThrift request1 = new ListStoragePoolCapacityRequestThrift();
    request1.setRequestId(RequestIdBuilder.get());
    request1.setDomainId(domainId);
    request1.setStoragePoolIdList(storagePoolIdFirst);
    ListStoragePoolCapacityResponseThrift response1 = new ListStoragePoolCapacityResponseThrift();
    ListStoragePoolCapacityRequestThrift request2 = new ListStoragePoolCapacityRequestThrift();
    request2.setRequestId(RequestIdBuilder.get());
    request2.setDomainId(domainId);
    ListStoragePoolCapacityResponseThrift response2 = new ListStoragePoolCapacityResponseThrift();
    try {
      response1 = informationCenterImpl.listStoragePoolCapacity(request1);
      response2 = informationCenterImpl.listStoragePoolCapacity(request2);
    } catch (TException e) {
      logger.error("catch an exception {}", e);
    }

    List<StoragePoolCapacityThrift> storagePoolCapacityList1 = response1
        .getStoragePoolCapacityList();
    List<StoragePoolCapacityThrift> storagePoolCapacityList2 = response2
        .getStoragePoolCapacityList();

    // assert
    assertEquals(storagePoolCapacityList1.size(), 1);
    assertEquals(storagePoolCapacityList2.size(), 2);

    StoragePoolCapacityThrift storagePoolCapacityThrift1 = storagePoolCapacityList1.get(0);
    assertTrue(storagePoolCapacityThrift1.getDomainId() == domainId);
    assertTrue(storagePoolCapacityThrift1.getStoragePoolId() == storagePoolId1);
    assertEquals(storagePoolCapacityThrift1.getStoragePoolName(), "STORAGEPOOLNAME1");
    assertEquals(storagePoolCapacityThrift1.getFreeSpace(), freeCapacity1);
    assertEquals(storagePoolCapacityThrift1.getTotalSpace(), totalCapacity1);

    boolean foundStoragePoolId1 = false;
    boolean foundStoragePoolId2 = false;
    for (StoragePoolCapacityThrift storagePoolCapacityThrift : storagePoolCapacityList2) {
      assertTrue(storagePoolCapacityThrift.getDomainId() == domainId);
      if (storagePoolCapacityThrift.getStoragePoolId() == storagePoolId1) {
        assertEquals(storagePoolCapacityThrift.getStoragePoolName(), "STORAGEPOOLNAME1");
        assertEquals(storagePoolCapacityThrift.getFreeSpace(), freeCapacity1);
        assertEquals(storagePoolCapacityThrift.getTotalSpace(), totalCapacity1);
        foundStoragePoolId1 = true;
      }
      if (storagePoolCapacityThrift.getStoragePoolId() == storagePoolId2) {
        assertEquals(storagePoolCapacityThrift.getStoragePoolName(), "STORAGEPOOLNAME2");
        assertEquals(storagePoolCapacityThrift.getFreeSpace(), freeCapacity2);
        assertEquals(storagePoolCapacityThrift.getTotalSpace(), totalCapacity2);
        foundStoragePoolId2 = true;
      }
    }
    assertTrue(foundStoragePoolId1);
    assertTrue(foundStoragePoolId2);
  }

  @Test
  public void testStoragePoolSpaceCalculator1() {
    // prepare storage pool
    final StoragePool storagePool = new StoragePool();
    Multimap<Long, Long> archivesInDataNode = HashMultimap.create();
    archivesInDataNode.put(1L, 1L);
    archivesInDataNode.put(1L, 2L);
    archivesInDataNode.put(2L, 3L);
    archivesInDataNode.put(2L, 4L);
    archivesInDataNode.put(3L, 5L);
    archivesInDataNode.put(3L, 6L);
    archivesInDataNode.put(4L, 7L);
    archivesInDataNode.put(4L, 8L);
    storagePool.setArchivesInDataNode(archivesInDataNode);

    // prepare instanceId2InstanceMetadata
    final Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
    InstanceMetadata datanode1 = new InstanceMetadata(new InstanceId(1L));
    datanode1.setGroup(new Group(1));
    InstanceMetadata datanode2 = new InstanceMetadata(new InstanceId(2L));
    datanode2.setGroup(new Group(2));
    InstanceMetadata datanode3 = new InstanceMetadata(new InstanceId(3L));
    datanode3.setGroup(new Group(3));
    InstanceMetadata datanode4 = new InstanceMetadata(new InstanceId(4L));
    datanode4.setGroup(new Group(4));
    instanceId2InstanceMetadata.put(1L, datanode1);
    instanceId2InstanceMetadata.put(2L, datanode2);
    instanceId2InstanceMetadata.put(3L, datanode3);
    instanceId2InstanceMetadata.put(4L, datanode4);

    // prepare archiveId2Archive
    final Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
    RawArchiveMetadata archive1 = new RawArchiveMetadata();
    archive1.setLogicalFreeSpace(20);
    archive1.setInstanceId(new InstanceId(1L));
    RawArchiveMetadata archive2 = new RawArchiveMetadata();
    archive2.setLogicalFreeSpace(20);
    archive2.setInstanceId(new InstanceId(1L));
    RawArchiveMetadata archive3 = new RawArchiveMetadata();
    archive3.setLogicalFreeSpace(18);
    archive3.setInstanceId(new InstanceId(2L));
    RawArchiveMetadata archive4 = new RawArchiveMetadata();
    archive4.setLogicalFreeSpace(24);
    archive4.setInstanceId(new InstanceId(2L));
    RawArchiveMetadata archive5 = new RawArchiveMetadata();
    archive5.setLogicalFreeSpace(30);
    archive5.setInstanceId(new InstanceId(3L));
    RawArchiveMetadata archive6 = new RawArchiveMetadata();
    archive6.setLogicalFreeSpace(36);
    archive6.setInstanceId(new InstanceId(3L));
    RawArchiveMetadata archive7 = new RawArchiveMetadata();
    archive7.setLogicalFreeSpace(40);
    archive7.setInstanceId(new InstanceId(4L));
    RawArchiveMetadata archive8 = new RawArchiveMetadata();
    archive8.setLogicalFreeSpace(50);
    archive8.setInstanceId(new InstanceId(4L));
    archiveId2Archive.put(1L, archive1);
    archiveId2Archive.put(2L, archive2);
    archiveId2Archive.put(3L, archive3);
    archiveId2Archive.put(4L, archive4);
    archiveId2Archive.put(5L, archive5);
    archiveId2Archive.put(6L, archive6);
    archiveId2Archive.put(7L, archive7);
    archiveId2Archive.put(8L, archive8);

    assertEquals(119,
        StoragePoolSpaceCalculator.calculateFreeSpace(storagePool, instanceId2InstanceMetadata,
            archiveId2Archive, 2, 1));
    assertEquals(74,
        StoragePoolSpaceCalculator.calculateFreeSpace(storagePool, instanceId2InstanceMetadata,
            archiveId2Archive, 3, 1));
  }

  @Test
  public void testStoragePoolSpaceCalculator2() {
    // prepare storage pool
    final StoragePool storagePool = new StoragePool();
    Multimap<Long, Long> archivesInDataNode = HashMultimap.create();
    archivesInDataNode.put(1L, 1L);
    archivesInDataNode.put(2L, 2L);
    archivesInDataNode.put(3L, 3L);
    archivesInDataNode.put(4L, 4L);
    storagePool.setArchivesInDataNode(archivesInDataNode);

    // prepare instanceId2InstanceMetadata
    final Map<Long, InstanceMetadata> instanceId2InstanceMetadata = new HashMap<>();
    InstanceMetadata datanode1 = new InstanceMetadata(new InstanceId(1L));
    datanode1.setGroup(new Group(1));
    InstanceMetadata datanode2 = new InstanceMetadata(new InstanceId(2L));
    datanode2.setGroup(new Group(2));
    InstanceMetadata datanode3 = new InstanceMetadata(new InstanceId(3L));
    datanode3.setGroup(new Group(1));
    InstanceMetadata datanode4 = new InstanceMetadata(new InstanceId(4L));
    datanode4.setGroup(new Group(3));
    instanceId2InstanceMetadata.put(1L, datanode1);
    instanceId2InstanceMetadata.put(2L, datanode2);
    instanceId2InstanceMetadata.put(3L, datanode3);
    instanceId2InstanceMetadata.put(4L, datanode4);

    // prepare archiveId2Archive
    final Map<Long, RawArchiveMetadata> archiveId2Archive = new HashMap<>();
    RawArchiveMetadata archive1 = new RawArchiveMetadata();
    archive1.setLogicalFreeSpace(57);
    archive1.setInstanceId(new InstanceId(1L));
    RawArchiveMetadata archive2 = new RawArchiveMetadata();
    archive2.setLogicalFreeSpace(52);
    archive2.setInstanceId(new InstanceId(2L));
    RawArchiveMetadata archive3 = new RawArchiveMetadata();
    archive3.setLogicalFreeSpace(57);
    archive3.setInstanceId(new InstanceId(3L));
    RawArchiveMetadata archive4 = new RawArchiveMetadata();
    archive4.setLogicalFreeSpace(52);
    archive4.setInstanceId(new InstanceId(4L));
    archiveId2Archive.put(1L, archive1);
    archiveId2Archive.put(2L, archive2);
    archiveId2Archive.put(3L, archive3);
    archiveId2Archive.put(4L, archive4);

    assertEquals(104,
        StoragePoolSpaceCalculator.calculateFreeSpace(storagePool, instanceId2InstanceMetadata,
            archiveId2Archive, 2, 1));
    assertEquals(52,
        StoragePoolSpaceCalculator.calculateFreeSpace(storagePool, instanceId2InstanceMetadata,
            archiveId2Archive, 3, 1));
  }
}
