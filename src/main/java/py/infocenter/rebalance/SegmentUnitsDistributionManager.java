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

import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TException;
import py.icshare.InstanceMetadata;
import py.infocenter.rebalance.exception.NoNeedToRebalance;
import py.infocenter.rebalance.struct.SendRebalanceTask;
import py.instance.InstanceId;
import py.rebalance.RebalanceTask;
import py.thrift.infocenter.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.DatanodeTypeNotSetExceptionThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.NotEnoughGroupExceptionThrift;
import py.thrift.share.NotEnoughNormalGroupExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeType;


public interface SegmentUnitsDistributionManager {

  /**
   * select a rebalance task if needed.
   *
   * @param record whether to record this selection, this should be given according to whether the
   *               returned task will be processed.
   * @throws NoNeedToRebalance if there is no need to do rebalance
   */
  @Deprecated
  RebalanceTask selectRebalanceTask(boolean record) throws NoNeedToRebalance;

  /**
   * select rebalance tasks when migrate source is instanceId.
   *
   * @param instanceId migrate source instance id
   * @return rebalance tasks (volumeId, task)
   * @throws NoNeedToRebalance if no need to rebalance, this exception will be caught
   */
  Multimap<Long, SendRebalanceTask> selectRebalanceTasks(InstanceId instanceId)
      throws NoNeedToRebalance;

  /**
   * update send rebalance task list according to new volume metadata if task is over, remove task
   * from task list.
   *
   * @param volumeMetadata new volume matadata
   */
  void updateSendRebalanceTasks(VolumeMetadata volumeMetadata)
      throws DatanodeTypeNotSetExceptionThrift, DatanodeTypeNotSetExceptionThrift;

  /**
   * clear all volume information, when volume is DEAD.
   *
   * @param volumeMetadata dead volume
   */
  void clearDeadVolumeRebalanceTasks(VolumeMetadata volumeMetadata);

  @Deprecated
  boolean discardRebalanceTask(long taskId);

  /**
   * return a segment units' distribution map for a new volume.
   *
   * @param expectedSize    the new volume size
   * @param volumeType      volume type
   * @param segmentWrapSize volume could be divided into several wrappers according to the
   *                        segmentWrapSize, for example, a 30G volume with segmentWrapSize 10G will
   *                        be divided into 3 wrappers, we will try to ensure any two segment units
   *                        in the same wrapper doesn't lay on the same disk.
   * @param storagePoolId   storage pool id
   */
  Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> reserveVolume(
      long expectedSize,
      VolumeType volumeType, boolean isSimpleConfiguration, int segmentWrapSize, Long storagePoolId)
      throws NotEnoughGroupExceptionThrift, NotEnoughSpaceExceptionThrift,
      NotEnoughNormalGroupExceptionThrift, TException;

  /**
   * update simple datanode group id and instance map(groupId2InstanceIdMap), when any datanode's
   * type is SIMPLE,.
   *
   * @param instanceMetadata datanode information
   */
  void updateSimpleDatanodeInfo(InstanceMetadata instanceMetadata);

  /**
   * get simple datanode's instance id set.
   *
   * @return all simple datanode's instance id set
   */
  Set<Long> getSimpleDatanodeInstanceIdSet();

  /**
   * return rebalance enable or unable?.
   *
   * @return if rebalance can work, return true
   */
  boolean isRebalanceEnable();

  /**
   * set rebalance can work? or forbidden?.
   *
   * @param isEnable if can work, make it true; if forbidden rebalance, make it false
   */
  void setRebalanceEnable(boolean isEnable);

  /**
   * get rebalance progress info of volume.
   *
   * @param volumeId volume id
   * @return rebalance ratio information
   */
  VolumeRebalanceInfo getRebalanceProgressInfo(long volumeId);


  /**
   * for reserveSegUnits.
   */
  ReserveSegUnitResult reserveSegUnits(ReserveSegUnitsInfo reserveSegUnitsInfo)
      throws InternalErrorThrift, DatanodeTypeNotSetExceptionThrift,
      SegmentNotFoundExceptionThrift, NotEnoughSpaceExceptionThrift,
      NotEnoughGroupExceptionThrift, TException;
}
