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

package py.infocenter.worker;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import py.archive.RawArchiveMetadata;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.icshare.InstanceMetadata;
import py.informationcenter.StoragePool;


public class StoragePoolSpaceCalculator {

  /**
   * xx.
   *
   * @param instanceId2InstanceMetadata all OK datanode
   * @param volumeRequiredGroupCount    For PSS volume, group count is 3; for PSA volume, group
   *                                    count is 2.
   */
  public static long calculateFreeSpace(StoragePool storagePool,
      Map<Long, InstanceMetadata> instanceId2InstanceMetadata,
      Map<Long, RawArchiveMetadata> archiveId2Archive, int volumeRequiredGroupCount,
      long segmentSize) {
    Set<Integer> allGroupIds = new HashSet<>();

    ObjectCounter<Integer> freeSpaceCounterByGroup = new TreeSetObjectCounter<>();
    for (Long archiveId : storagePool.getArchivesInDataNode().values()) {
      RawArchiveMetadata archiveMetadata = archiveId2Archive.get(archiveId);
      if (archiveMetadata != null) {
        int groupId = instanceId2InstanceMetadata.get(archiveMetadata.getInstanceId().getId())
            .getGroup().getGroupId();
        allGroupIds.add(groupId);
        freeSpaceCounterByGroup.increment(groupId, archiveMetadata.getLogicalFreeSpace());
      }
    }

    if (allGroupIds.size() < 3) {
      return 0;
    }

    BucketWithBarrier spaceCalculator = new BucketWithBarrier(volumeRequiredGroupCount);
    Iterator<Integer> iterator = freeSpaceCounterByGroup.descendingIterator();
    while (iterator.hasNext()) {
      spaceCalculator.fill(freeSpaceCounterByGroup.get(iterator.next()));
    }
    long spaceAvailableInStoragePool = spaceCalculator.getLowest();

    return spaceAvailableInStoragePool - spaceAvailableInStoragePool % segmentSize;
  }
}
