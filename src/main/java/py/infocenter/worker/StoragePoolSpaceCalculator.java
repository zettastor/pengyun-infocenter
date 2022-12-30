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
