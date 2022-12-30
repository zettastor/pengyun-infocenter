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
import com.google.common.collect.Multimaps;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * About simple datanode information manager.
 */
public class SimpleDatanodeManager {

  private static final Logger logger = LoggerFactory.getLogger(SimpleDatanodeManager.class);
  private Multimap<Integer, Long> groupId2InstanceIdMap;

  public SimpleDatanodeManager() {
    groupId2InstanceIdMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());
  }

  /**
   * update simple datanode group id and instance map(groupId2InstanceIdMap), when any datanode's
   * type is SIMPLE,.
   *
   * @param instanceId       datanode id
   * @param groupId          datanode's belong to group id
   * @param isSimpleDatanode is this datanode is simple datanode
   */
  public void updateSimpleDatanodeInfo(long instanceId, int groupId, boolean isSimpleDatanode) {
    logger.debug("[updateSimpleDatanodeInfo] instanceId: {}; groupId: {}; isSimpleDatanode: {}; ",
        instanceId, groupId, isSimpleDatanode);

    logger.debug("[updateSimpleDatanodeGroupIdSet] before update! groupId2InstanceIdMap: {}",
        groupId2InstanceIdMap);

    if (isSimpleDatanode) {
      if (!groupId2InstanceIdMap.containsEntry(groupId, instanceId)) {


        groupId2InstanceIdMap.put(groupId, instanceId);
      }
    } else {
      if (groupId2InstanceIdMap.containsEntry(groupId, instanceId)) {


        groupId2InstanceIdMap.remove(groupId, instanceId);
      }
    }

    logger.debug("[updateSimpleDatanodeGroupIdSet] after update! groupId2InstanceIdMap: {}",
        groupId2InstanceIdMap);
  }

  /**
   * get simple datanode's group id set.
   *
   * @return all simple datanode's group id set
   */
  public Set<Integer> getSimpleDatanodeGroupIdSet() {
    return groupId2InstanceIdMap.keySet();
  }

  /**
   * get simple datanode's instance id set.
   *
   * @return all simple datanode's instance id set
   */
  public Set<Long> getSimpleDatanodeInstanceIdSet() {
    Set<Long> simpleDatanodeIdSet = new HashSet<>();
    simpleDatanodeIdSet.addAll(groupId2InstanceIdMap.values());
    return simpleDatanodeIdSet;
  }

  /**
   * get all simple datanode who's group id == groupId.
   *
   * @param groupId datanode's group id
   * @return all datanode id who belong to groupId
   */
  public Set<Long> getSimpleDatanodeIdSetByGroupId(int groupId) {
    Set<Long> simpleDatanodeIdSet = new HashSet<>();
    simpleDatanodeIdSet.addAll(groupId2InstanceIdMap.get(groupId));
    return simpleDatanodeIdSet;
  }

  @Override
  public String toString() {
    return "SimpleDatanodeManager{"

        + "groupId2InstanceIdMap=" + groupId2InstanceIdMap

        + '}';
  }
}
