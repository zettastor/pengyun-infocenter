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

package py.infocenter.create.volume;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.common.AccountMetadataJsonParser;
import py.common.RequestIdBuilder;
import py.common.VolumeMetadataJsonParser;
import py.common.struct.EndPoint;
import py.icshare.AccountMetadata;
import py.icshare.VolumeCreationRequest;
import py.icshare.VolumeCreationRequest.RequestType;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.CreateSegmentUnitBatchRequest;
import py.thrift.datanode.service.CreateSegmentUnitNode;
import py.thrift.share.CloneTypeThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;


public class CreateSegmentUnitsRequest {

  private static final Logger logger = LoggerFactory.getLogger(CreateSegmentUnitsRequest.class);
  // the list of ids of data node instances that can be used to create segment units
  private final Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap;
  // the order of this list comes from information center, who is responsible for the balance 
  // between instances
  private final List<InstanceId> normalInstanceIdListInOrder;
  private final List<InstanceId> arbiterInstanceIdListInOrder;
  private final VolumeCreationRequest volumeRequest;
  private final AccountMetadata account;
  private final int currentSegIndex;
  private final SegmentUnitTypeThrift normalSegmentUnitsType;
  public Map<InstanceId, CreateSegmentUnitBatchRequest> rebuildCreateSegmentUnitRequest;

  // for extend volume;
  private VolumeMetadata rootVolume;

  private SegmentCreatorControllerFactory.SegmentCreatorController controller;
  private VolumeMetadata volumeToBeCreated;

  
  public CreateSegmentUnitsRequest(Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap,
      List<InstanceId> arbiterInstanceIdListInOrder, List<InstanceId> normalInstanceIdListInOrder,
      VolumeCreationRequest volumeRequest, AccountMetadata account, int currentSegIndex,
      VolumeMetadata volumeToBeCreated, SegmentUnitTypeThrift normalSegmentUnitsType,
      Map<InstanceId, CreateSegmentUnitBatchRequest> rebuildCreateSegmentUnitRequest) {
    this.instanceMap = instanceMap;
    this.normalInstanceIdListInOrder = normalInstanceIdListInOrder;
    this.arbiterInstanceIdListInOrder = arbiterInstanceIdListInOrder;
    this.volumeRequest = volumeRequest;
    this.account = account;
    this.currentSegIndex = currentSegIndex;
    this.volumeToBeCreated = volumeToBeCreated;
    this.normalSegmentUnitsType = normalSegmentUnitsType;
    this.rebuildCreateSegmentUnitRequest = rebuildCreateSegmentUnitRequest;
  }

  
  public Boolean makeRequest() throws Exception {
    // the number of members in a segment. Usually it is 3.
    VolumeType volumeType = VolumeType.valueOf(volumeRequest.getVolumeType());
    int quorum = volumeType.getVotingQuorumSize();
    boolean createSegmentUnitOk = false;

    //check the have more number to create segment
    if (normalInstanceIdListInOrder.size() + arbiterInstanceIdListInOrder.size() < quorum) {
      logger.warn("there is not enough instance for create segment, the volume id :{}",
          volumeRequest.getVolumeId());
      return false;
    }

    Queue<SegmentUnitTypeThrift> segmentUnitTypeQueue = new ConcurrentLinkedQueue<>();
    fillSegmentUnitStatusQueue(segmentUnitTypeQueue, volumeType);
    SegmentMembershipThrift initMembership = null;
    int epoch = 0;
    Set<Long> arbiterInstanceIds = null;

    if (segmentUnitTypeQueue.size() > 0 && segmentUnitTypeQueue.size()
        <= normalInstanceIdListInOrder.size() + arbiterInstanceIdListInOrder.size()) {
     
     
      List<InstanceId> arbiterCandidates = new ArrayList<>();
      List<InstanceId> normalCandidates = new ArrayList<>();
      for (SegmentUnitTypeThrift unitType : segmentUnitTypeQueue) {
        if (SegmentUnitTypeThrift.Arbiter == unitType) {
          arbiterCandidates.add(arbiterInstanceIdListInOrder.remove(0));
        } else {
          normalCandidates.add(normalInstanceIdListInOrder.remove(0));
        }
      }

     
      List<EndPoint> endpoints = buildEndPoints(normalCandidates);
      if (endpoints == null || endpoints.size() != normalCandidates.size()) {
        logger.warn("can't broadcast createSegmentUnits because some endpoints can't be built");
        return false;
      }
      endpoints = buildEndPoints(arbiterCandidates);
      if (endpoints == null || endpoints.size() != arbiterCandidates.size()) {
        logger.warn("can't broadcast createSegmentUnits because some endpoints can't be built");
        return false;
      }

      long primaryCandidateId = normalCandidates.get(0).getId();
      if (volumeType.getNumArbiters() > 0) {
        // we need to create a arbiters
        arbiterInstanceIds = new HashSet<>();
        for (int i = 0; i < volumeType.getNumArbiters(); i++) {
          // add from index 1
          arbiterInstanceIds.add(arbiterCandidates.get(i).getId());
        }
      }
      Set<Long> secondaryCandidates = new HashSet<>();
      for (InstanceId id : normalCandidates.subList(1, normalCandidates.size())) {
        secondaryCandidates.add(id.getId());
      }

      // take the first candidate as the primary and the rest of them as the secondaries
      initMembership = new SegmentMembershipThrift(volumeRequest.getVolumeId(), currentSegIndex,
          epoch, 0,
          primaryCandidateId, secondaryCandidates, arbiterInstanceIds);

      int arbiterIndex = 0;
      int normalIndex = 0;

      while (!segmentUnitTypeQueue.isEmpty()) {

        SegmentUnitTypeThrift segmentUnitTypeThrift = segmentUnitTypeQueue.poll();
        InstanceId instanceId;
        if (SegmentUnitTypeThrift.Arbiter == segmentUnitTypeThrift) {
          instanceId = arbiterCandidates.get(arbiterIndex++);
        } else {
          instanceId = normalCandidates.get(normalIndex++);
        }

        CreateSegmentUnitBatchRequest createSegmentUnitBatchRequest;
        if (rebuildCreateSegmentUnitRequest.containsKey(instanceId)) {
          createSegmentUnitBatchRequest = rebuildCreateSegmentUnitRequest.get(instanceId);

        } else {
          createSegmentUnitBatchRequest = new CreateSegmentUnitBatchRequest();
          rebuildCreateSegmentUnitRequest.put(instanceId, createSegmentUnitBatchRequest);
        }

        generateCreateUnitRequest(createSegmentUnitBatchRequest);

        CreateSegmentUnitNode createSegmentUnitNode = new CreateSegmentUnitNode();
        createSegmentUnitNode.setSegIndex(currentSegIndex);
        createSegmentUnitNode.setInitMembership(initMembership);
        createSegmentUnitNode.setSegmentUnitType(segmentUnitTypeThrift);
        createSegmentUnitNode.setSegmentWrapSize(volumeToBeCreated.getSegmentWrappCount());
        createSegmentUnitNode.setVolumeSource(RequestResponseHelper.buildThriftVolumeSource(
            RequestResponseHelper
                .buildVolumeSourceTypeFrom(volumeRequest.getRequestTypeString())));

        createSegmentUnitBatchRequest.addToSegmentUnits(createSegmentUnitNode);
        logger.info("the current createSegmentUnitBatchRequest is :{}",
            createSegmentUnitBatchRequest);
      }

      createSegmentUnitOk = true;
    }

    return createSegmentUnitOk;
  }

  private void fillSegmentUnitStatusQueue(Queue<SegmentUnitTypeThrift> segmentUnitStatusQueue,
      VolumeType volumeType) {
    for (int i = 0; i < volumeType.getNumMembers() - volumeType.getNumArbiters(); i++) {
      segmentUnitStatusQueue.add(normalSegmentUnitsType);
    }
    for (int i = 0; i < volumeType.getNumArbiters(); i++) {
      segmentUnitStatusQueue.add(SegmentUnitTypeThrift.Arbiter);
    }
  }

  
  public void close() {
    if (null != controller) {
      controller.release();
    }
  }

  private void generateCreateUnitRequest(
      CreateSegmentUnitBatchRequest createSegmentUnitBatchRequest)
      throws JsonProcessingException {

    createSegmentUnitBatchRequest.setRequestId(RequestIdBuilder.get());
    createSegmentUnitBatchRequest.setVolumeId(volumeRequest.getVolumeId());
    createSegmentUnitBatchRequest
        .setVolumeType(VolumeType.valueOf(volumeRequest.getVolumeType()).getVolumeTypeThrift());
    createSegmentUnitBatchRequest.setStoragePoolId(volumeRequest.getStoragePoolId());
    createSegmentUnitBatchRequest
        .setEnableLaunchMultiDrivers(volumeRequest.isEnableLaunchMultiDrivers());

    if (currentSegIndex == 0) {
     
     
     

      // build accountmetadata and volumemetadata json string
      final String eachTimeExtendVolumeSize =
          String.valueOf(volumeRequest.getTotalSegmentCount()) + ",";

      final ObjectMapper mapper = new ObjectMapper();
      VolumeMetadata volumeToBuild = new VolumeMetadata();
      volumeToBuild.setAccountId(volumeRequest.getAccountId());
      volumeToBuild.setVolumeId(volumeRequest.getVolumeId());
      volumeToBuild.setName(volumeRequest.getName());
      volumeToBuild.setVolumeSize(volumeRequest.getVolumeSize());
      volumeToBuild.setSegmentSize(volumeRequest.getSegmentSize());
      volumeToBuild.setVolumeType(VolumeType.valueOf(volumeRequest.getVolumeType()));
      volumeToBuild.setRootVolumeId(volumeRequest.getRootVolumeId());
      volumeToBuild.setDomainId(volumeRequest.getDomainId());
      volumeToBuild.setStoragePoolId(volumeRequest.getStoragePoolId());
      volumeToBuild.initVolumeLayout();
      Validate.notNull(volumeToBeCreated.getVolumeCreatedTime());
      volumeToBuild.setVolumeCreatedTime(volumeToBeCreated.getVolumeCreatedTime());
      volumeToBuild.setPageWrappCount(volumeToBeCreated.getPageWrappCount());
      volumeToBuild.setSegmentWrappCount(volumeToBeCreated.getSegmentCount());
      volumeToBuild.setLastExtendedTime(volumeToBeCreated.getLastExtendedTime());
      Validate.notNull(volumeToBeCreated.getVolumeSource());
      volumeToBuild.setVolumeSource(volumeToBeCreated.getVolumeSource());
      volumeToBuild.setReadWrite(VolumeMetadata.ReadWriteType.READWRITE);
      volumeToBuild.setPageWrappCount(volumeToBeCreated.getPageWrappCount());
      volumeToBuild.setSegmentWrappCount(volumeToBeCreated.getSegmentWrappCount());
      volumeToBuild.setEnableLaunchMultiDrivers(volumeRequest.isEnableLaunchMultiDrivers());
      volumeToBuild.setEachTimeExtendVolumeSize(volumeToBeCreated.getEachTimeExtendVolumeSize());

      volumeToBuild.setVolumeDescription(volumeToBeCreated.getVolumeDescription());

      VolumeMetadataJsonParser volumeJsonParser = new VolumeMetadataJsonParser(
          volumeToBuild.getVersion(),
          mapper.writeValueAsString(volumeToBuild));
      AccountMetadataJsonParser accountJsonParser = new AccountMetadataJsonParser(
          volumeToBuild.getAccountId(),
          mapper.writeValueAsString(account));
      createSegmentUnitBatchRequest
          .setVolumeMetadataJson(volumeJsonParser.getCompositedVolumeMetadataJson());
      logger.warn("the create volume :{}, eachTimeExtendVolumeSize :{}, the json :{}",
          volumeToBuild.getVolumeId(), eachTimeExtendVolumeSize,
          volumeJsonParser.getCompositedVolumeMetadataJson());

    }
  }

  private List<EndPoint> buildEndPoints(Collection<InstanceId> dataNodes) {
    List<EndPoint> endPoints = new ArrayList<>();
    for (InstanceId instanceId : dataNodes) {
      endPoints.add(buildEndPoint(instanceId));
    }
    return endPoints;
  }

  private EndPoint buildEndPoint(InstanceId instanceId) {
    InstanceIdAndEndPointThrift instanceIdAndEndPoint = instanceMap.get(instanceId);
    return EndPoint.fromString(instanceIdAndEndPoint.getEndPoint());
  }

  public void setRootVolume(VolumeMetadata rootVolume) {
    this.rootVolume = rootVolume;
  }
}
