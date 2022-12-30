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

import edu.emory.mathcs.backport.java.util.concurrent.RejectedExecutionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.exception.InternalErrorException;
import py.exception.InvalidFormatException;
import py.exception.InvalidInputException;
import py.exception.NotEnoughSpaceException;
import py.icshare.AccountMetadata;
import py.icshare.VolumeCreationRequest;
import py.icshare.VolumeCreationRequest.RequestType;
import py.icshare.authorization.AccountStore;
import py.icshare.exception.VolumeNotFoundException;
import py.infocenter.InfoCenterConfiguration;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.store.VolumeStore;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.thrift.datanode.service.CreateSegmentUnitBatchRequest;
import py.thrift.datanode.service.CreateSegmentUnitFailedCodeThrift;
import py.thrift.datanode.service.CreateSegmentUnitFailedNode;
import py.thrift.datanode.service.CreateSegmentUnitNode;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DeleteSegmentUnitRequest;
import py.thrift.infocenter.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeId;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;


public class CreateVolumeManager {

  private static final Logger logger = LoggerFactory.getLogger(CreateVolumeManager.class);

  private final int addOne = 1;
  private final SegmentCreatorControllerFactory segmentCreatorControllerFactory =
      new SegmentCreatorControllerFactory();

  public ExecutorService createSegmentsExecutor;
  private ReserveInformation reserveInformation;
  private VolumeInformationManger volumeInformationManger;
  private InstanceStore instanceStore;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
  private GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory;
  private int createSegmentUnitThreadPoolCoreSize;
  private int createSegmentUnitThreadPoolMaxSize;
  private int createVolumeTimeOutSecond;
  private int thriftTimeOut;
  private int passelCount;
  private AccountStore accountStore;
  private InfoCenterConfiguration infoCenterConfiguration;

  private Map<SegId, CreateSegmentUnitsRequest> segmentCreatorTable = new ConcurrentHashMap<>();
  private VolumeStore volumeStore;



  public CreateVolumeManager(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory,
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory,
      InfoCenterConfiguration infoCenterConfiguration, int createVolumeTimeOutSecond,
      int passelCount, int createSegmentUnitThreadPoolCoreSize,
      int createSegmentUnitThreadPoolMaxSize) {
    this.dataNodeClientFactory = dataNodeClientFactory;
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    this.createVolumeTimeOutSecond = createVolumeTimeOutSecond;
    this.passelCount = passelCount;
    this.createSegmentUnitThreadPoolCoreSize = createSegmentUnitThreadPoolCoreSize;
    this.createSegmentUnitThreadPoolMaxSize = createSegmentUnitThreadPoolMaxSize;
    this.createSegmentsExecutor = createSegmentUnitsExecutor();
    this.infoCenterConfiguration = infoCenterConfiguration;
  }


  public CreateVolumeManager() {
  }


  public static Logger getLogger() {
    return logger;
  }


  public void init() {
    this.createSegmentsExecutor = createSegmentUnitsExecutor();
  }


  public void createVolume(VolumeCreationRequest volumeRequest) throws InvalidInputException,
      TException, NotEnoughSpaceException, InternalErrorException,
      GenericThriftClientFactoryException {

    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> segIndex2Instances =
        reserveInformation.reserveVolume(volumeRequest);
    logger.info("Successfully reserved volume, got {}", segIndex2Instances);
    createSegmentUnits(volumeRequest, segIndex2Instances, 0);
  }


  public void createSegmentUnits(VolumeCreationRequest volumeRequest,
      Map<Integer, Map<SegmentUnitTypeThrift,
          List<InstanceIdAndEndPointThrift>>> segIndex2Instances, int startSegmentIndex)
      throws InvalidInputException, InternalErrorException, GenericThriftClientFactoryException,
      InternalErrorThrift {
    logger.warn("createSegmentUnits run! volumeRequest:{}, startSegmentIndex:{}", volumeRequest,
        startSegmentIndex);

    AccountMetadata account = accountStore.getAccountById(volumeRequest.getAccountId());
    SegmentUnitTypeThrift normalSegmentUnitsType = SegmentUnitTypeThrift.Normal;
    if (account == null) {
      throw new InvalidInputException("account not found");
    }
    int numberOfMembers = VolumeType.valueOf(volumeRequest.getVolumeType()).getNumMembers();
    //** the SimpleConfiguration volume change, create all segment ***/

    long volumeSize = volumeRequest.getVolumeSize();
    if (volumeSize % volumeRequest.getSegmentSize() != 0) {
      throw new InvalidInputException("volume size is not integral multiple of segment size");
    }

    int numOfSegments = (int) (volumeSize / volumeRequest.getSegmentSize());
    volumeRequest.setTotalSegmentCount(numOfSegments);

    for (int segIndex : segIndex2Instances.keySet()) {
      List<InstanceIdAndEndPointThrift> arbiterInstances = segIndex2Instances.get(segIndex)
          .get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceIdAndEndPointThrift> normalInstances = segIndex2Instances.get(segIndex)
          .get(normalSegmentUnitsType);
      if (arbiterInstances.size() + normalInstances.size() < numberOfMembers) {
        logger.error(
            "The minimum number of instances needed is {} there are {} available. Can't create or"
                + " extend a volume {}",
            numberOfMembers, segIndex2Instances.get(segIndex).size(), volumeRequest);
        throw new InternalErrorException(" no enough instances to create requested volume");
      }
    }

    RequestType createType = volumeRequest.getRequestType();
    // if the volume is created for clone volume, get the source volume



    VolumeMetadata rootVolume = null;
    if (createType == RequestType.EXTEND_VOLUME) {
      try {
        rootVolume = this
            .getFullVolumeInfo(volumeRequest.getRootVolumeId(), volumeRequest.getAccountId());
        // set the startSegmentIndex, get the extend volume lastSegmentIndex
        int lastSegmentIndex = rootVolume.getSegmentCount();
        startSegmentIndex = lastSegmentIndex;
      } catch (VolumeNotFoundException e) {
        logger.error("catch an exception when get volume", e);
        throw new InternalErrorException();
      }
    }

    // get volume info to build volume metadata json when create first segment
    VolumeMetadata volumeToBeCreated;
    try {
      volumeToBeCreated = this.getVolumeForCreateVolume(volumeRequest.getVolumeId(),
          volumeRequest.getAccountId());
    } catch (VolumeNotFoundException e) {
      logger.error("catch an exception when get volume", e);
      throw new InternalErrorException();
    }

    VolumeType volumeType = VolumeType.valueOf(volumeRequest.getVolumeType());
    int quorum = volumeType.getVotingQuorumSize();

    logger.warn("createSegmentUnits: create passel segmentUnit, passelCount:{}, numOfSegments:{},",
        passelCount, numOfSegments);
    List<Future<Boolean>> futuresForCreateSegmentUnit = new ArrayList<>();

    // get the all InstanceId for Create Segment Unit
    Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap = new HashMap<>();


    Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
        segIndex2InstancesForUsed = new HashMap<>();
    for (int i = 0; i < numOfSegments; i++) {
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> segmentUnitTypeThriftListMap
          = segIndex2Instances
          .get(i);
      if (segmentUnitTypeThriftListMap != null) {
        int realSegIndex = i + startSegmentIndex;
        segIndex2InstancesForUsed.put(realSegIndex, segmentUnitTypeThriftListMap);
      }
    }

    while (true) {
      //the end create segment unit request
      Map<InstanceId, CreateSegmentUnitBatchRequest> rebuildCreateSegmentUnitRequest =
          new ConcurrentHashMap<>();

      // get the create segment unit request

      makeCreateSegmentUnit(volumeRequest, segIndex2InstancesForUsed, account,
          normalSegmentUnitsType, numOfSegments,
          createType, rootVolume, volumeToBeCreated, rebuildCreateSegmentUnitRequest,
          instanceMap);

      if (rebuildCreateSegmentUnitRequest.isEmpty()) {
        logger.error("the rebuildCreateSegmentUnitRequest is empty,about volume : {}",
            volumeRequest.getVolumeId());
        throw new RuntimeException(
            "Failed to create all segments because too many creation tasks are running");
      } else {
        logger.warn("the rebuildCreateSegmentUnitRequest size :{} ",
            rebuildCreateSegmentUnitRequest.size());
      }


      Map<InstanceId, List<Integer>> goodSegIndex = new ConcurrentHashMap<>();
      Map<InstanceId, List<CreateSegmentUnitFailedNode>> badSegIndex = new ConcurrentHashMap<>();


      int instanceNumber = rebuildCreateSegmentUnitRequest.size();
      int sentinelCount = passelCount < instanceNumber ? passelCount : instanceNumber;
      int currentRequestIndex = 0;
      int requestNumber;

      for (Map.Entry<InstanceId, CreateSegmentUnitBatchRequest> request :
          rebuildCreateSegmentUnitRequest
              .entrySet()) {
        try {
          InstanceId instanceId = request.getKey();
          CreateSegmentUnitBatchRequest createSegmentUnitBatchRequest = request.getValue();
          SegmentCreatorChange segmentCreatorChange = new SegmentCreatorChange(instanceId,
              createSegmentUnitBatchRequest,
              dataNodeAsyncClientFactory, createVolumeTimeOutSecond, instanceMap, goodSegIndex,
              badSegIndex);
          if (infoCenterConfiguration.isSegmentCreatorEnabled()) {
            segmentCreatorChange.setController(
                segmentCreatorControllerFactory
                    .create(infoCenterConfiguration.getSegmentCreatorBlockingTimeoutMillis()));
          }

          futuresForCreateSegmentUnit.add(createSegmentsExecutor.submit(segmentCreatorChange));
        } catch (InterruptedException e) {
          logger.warn("submitting creation of {} was rejected. It is impossible", volumeRequest);
          throw new RuntimeException(
              "Failed to create all segments because too many creation tasks are running");
        }
        currentRequestIndex++;
        requestNumber = currentRequestIndex;
        if ((requestNumber % sentinelCount != 0) && (requestNumber != instanceNumber)) {
          continue;
        }

        int successCountForCreateSegmentUnit = 0;
        logger.warn("Wait all futuresForCreateSegmentUnit");
        for (Future<Boolean> future : futuresForCreateSegmentUnit) {
          try {
            if (future.get()) {
              // just for wait
              successCountForCreateSegmentUnit++;
            }
          } catch (InterruptedException e) {
            logger.warn("creation of a segment unit was interrupted. Throw an exception", e);
          } catch (ExecutionException e) {
            logger.warn("the creation of a segment unit failed. Throw an exception", e);
          }
        }

        futuresForCreateSegmentUnit.clear();
      }

      logger.warn("the end to get value instanceMap :{}, goodSegIndex :{}, badSegIndex :{}",
          instanceMap, goodSegIndex, badSegIndex);


      Map<Integer, Integer> collectSegment = new ConcurrentHashMap<>();
      for (Map.Entry<InstanceId, InstanceIdAndEndPointThrift> eachInstanceId : instanceMap
          .entrySet()) {
        InstanceId instanceId = eachInstanceId.getKey();
        List<Integer> getList = goodSegIndex.get(instanceId);
        if (getList != null) {
          logger.warn("the current id :{}, good segment size :{}", instanceId, getList.size());
          for (Integer integer : getList) {
            if (collectSegment.containsKey(integer)) {
              int number = collectSegment.get(integer);
              collectSegment.put(integer, ++number);
            } else {
              collectSegment.put(integer, addOne);
            }
          }
        }
      }

      //** if the exception is SegmentExistingException, make the segment unit create success **//
      if (!badSegIndex.isEmpty()) {
        for (Map.Entry<InstanceId, InstanceIdAndEndPointThrift> eachInstanceId : instanceMap
            .entrySet()) {
          InstanceId instanceId = eachInstanceId.getKey();
          List<CreateSegmentUnitFailedNode> failedNodeForGetSegmentExistingException = badSegIndex
              .get(instanceId);
          if (failedNodeForGetSegmentExistingException != null) {
            logger.warn("the current id :{}, failed segment size :{}", instanceId,
                failedNodeForGetSegmentExistingException.size());
            for (CreateSegmentUnitFailedNode failedNode :
                failedNodeForGetSegmentExistingException) {
              int segIndex = failedNode.getSegIndex();
              CreateSegmentUnitFailedCodeThrift exception = failedNode.getErrorCode();
              if (exception == CreateSegmentUnitFailedCodeThrift.SegmentExistingException) {
                if (collectSegment.containsKey(segIndex)) {
                  int number = collectSegment.get(segIndex);
                  collectSegment.put(segIndex, ++number);
                } else {
                  collectSegment.put(segIndex, addOne);
                }
              }
            }
          }
        }
      }


      for (Map.Entry<Integer, Integer> entry : collectSegment.entrySet()) {
        int segIndex = entry.getKey();
        int number = entry.getValue();

        //* not care the create ok segment, remove the create ok segment */
        if (number >= quorum) {
          segIndex2InstancesForUsed.remove(segIndex);
        }
      }

      //each Instance which to delete seg index
      Map<InstanceId, List<Integer>> deleteIndex = new ConcurrentHashMap<>();

      //** all segment create ok **/
      if (segIndex2InstancesForUsed.isEmpty()) {
        if (createType == RequestType.EXTEND_VOLUME) {
          logger.warn("extend the volume :{} and the extend size:{} is ok",
              volumeRequest.getVolumeId(), volumeSize);
        } else {
          //create volume
          logger.warn("create the volume :{} ok", volumeRequest.getVolumeId());
        }

      } else {

        List<Integer> failedIndex = new ArrayList<>();

        for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
            entry : segIndex2InstancesForUsed.entrySet()) {
          //** get the failed index list ***/
          Integer segmentIndex = entry.getKey();
          failedIndex.add(segmentIndex);
        }



        for (Map.Entry<InstanceId, List<Integer>> entry : goodSegIndex.entrySet()) {
          InstanceId instanceId = entry.getKey();
          List<Integer> indexList = entry.getValue();
          List<Integer> recordDeleteIndex = new ArrayList<>();
          //find the want to delete index, get the instanceId and initMembership for unit
          for (Integer index : failedIndex) {
            if (indexList.contains(index)) {
              recordDeleteIndex.add(index);
            }
          }

          //** back the result to delete next step **/
          if (!recordDeleteIndex.isEmpty()) {
            deleteIndex.put(instanceId, recordDeleteIndex);
          }
        }
      }





      for (Map.Entry<InstanceId, List<Integer>> entry : deleteIndex.entrySet()) {
        //get the initMembership
        InstanceId instanceId = entry.getKey();
        List<Integer> list = entry.getValue();
        CreateSegmentUnitBatchRequest createSegmentUnitBatchRequest =
            rebuildCreateSegmentUnitRequest
                .get(instanceId);
        List<CreateSegmentUnitNode> segmentUnitNodeList = createSegmentUnitBatchRequest
            .getSegmentUnits();
        for (CreateSegmentUnitNode segmentUnitNode : segmentUnitNodeList) {
          int segIndex = segmentUnitNode.getSegIndex();
          SegmentUnitTypeThrift segmentUnitType = segmentUnitNode.getSegmentUnitType();

          if (list.contains(segIndex)) {
            SegmentMembershipThrift segmentMembershipThrift = segmentUnitNode.getInitMembership();
            InstanceId dataNodesDeleted = deleteSegmentUnits(volumeRequest.getVolumeId(),
                segIndex, instanceId, segmentMembershipThrift, thriftTimeOut, instanceMap);



            if (dataNodesDeleted != null) {
              removeInstanceInReserveTable(segIndex2InstancesForUsed, segIndex, dataNodesDeleted,
                  segmentUnitType);
            }
          }
        }
      }


      for (Map.Entry<InstanceId, List<CreateSegmentUnitFailedNode>> entry : badSegIndex
          .entrySet()) {
        InstanceId instanceId = entry.getKey();
        List<CreateSegmentUnitFailedNode> failedNodeList = entry.getValue();
        for (CreateSegmentUnitFailedNode node : failedNodeList) {
          int segIndex = node.getSegIndex();
          CreateSegmentUnitFailedCodeThrift errorCode = node.getErrorCode();
          //** if the exception is SegmentExistingException,we think the Segment unit create ok **/
          if (errorCode != CreateSegmentUnitFailedCodeThrift.SegmentExistingException) {
            removeInstanceInReserveTable(segIndex2InstancesForUsed, segIndex, instanceId);
          }
        }
      }

      //** reset the next time create segment number **/
      numOfSegments = segIndex2InstancesForUsed.size();
      logger.info("the next segIndex2InstancesForUsed is :{}", segIndex2InstancesForUsed);
      if (numOfSegments == 0) {
        segmentCreatorTable.clear();

        if (createType == RequestType.EXTEND_VOLUME) {
          logger.warn("extend the volume :{} and the extend size:{} is ok",
              volumeRequest.getVolumeId(), volumeSize);
        } else {
          //create volume
          logger.warn("create volume ok, the volume id:{}", volumeRequest.getVolumeId());
        }
        break;
      } else {
        logger.warn("next time to create segment number :{}", numOfSegments);
      }
    }

    return;
  }

  /**
   * delete the failed instance in segIndex2InstancesForUsed.
   **/
  private void removeInstanceInReserveTable(Map<Integer, Map<SegmentUnitTypeThrift,
      List<InstanceIdAndEndPointThrift>>> segIndex2InstancesForUsed,
      int segIndex, InstanceId instanceId, SegmentUnitTypeThrift segmentUnitType) {
    if (segIndex2InstancesForUsed.isEmpty()) {
      return;
    }

    Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> segmentUnitTypeMap =
        segIndex2InstancesForUsed
            .get(segIndex);
    if (segmentUnitTypeMap != null) {
      List<InstanceIdAndEndPointThrift> instanceList = segmentUnitTypeMap.get(segmentUnitType);
      if (instanceList != null && !instanceList.isEmpty()) {
        instanceList.remove(instanceId);
        logger.warn(
            "removeInstanceInReserveTable, the segIndex :{}, InstanceId :{}, segmentUnitType :{} ",
            segIndex, instanceId, segmentUnitType);
      }
    }

  }

  /**
   * delete the failed instance in segIndex2InstancesForUsed.
   **/
  private void removeInstanceInReserveTable(Map<Integer, Map<SegmentUnitTypeThrift,
      List<InstanceIdAndEndPointThrift>>> segIndex2InstancesForUsed,
      int segIndex, InstanceId instanceId) {
    if (segIndex2InstancesForUsed.isEmpty()) {
      return;
    }

    Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> segmentUnitTypeMap =
        segIndex2InstancesForUsed
            .get(segIndex);
    if (segmentUnitTypeMap != null) {
      for (Map.Entry<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> entry :
          segmentUnitTypeMap
              .entrySet()) {
        SegmentUnitTypeThrift segmentUnitType = entry.getKey();
        List<InstanceIdAndEndPointThrift> instanceList = entry.getValue();
        if (instanceList != null && !instanceList.isEmpty()) {
          for (int i = 0; i < instanceList.size(); i++) {
            if (instanceList.get(i).getInstanceId() == instanceId.getId()) {
              instanceList.remove(instanceList.get(i));
              logger.warn(
                  "removeInstanceInReserveTable, the segIndex :{}, InstanceId :{}, "
                      + "segmentUnitType :{} ",
                  segIndex, instanceId, segmentUnitType);
            }

          }
        }
      }
    }
  }

  /**
   * Delete a list of segment units located in the specified datanodes. The return is a list of data
   * nodes where the corresponding segment units have been unsuccessfully deleted
   */
  private InstanceId deleteSegmentUnits(long volumeId, int segIndex, InstanceId instanceId,
      SegmentMembershipThrift initMembership,
      long timeout, Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap)
      throws InternalErrorThrift, GenericThriftClientFactoryException {
    logger.warn("Deleting volumeId {} segIndex {} from the data nodes: {}", volumeId, segIndex,
        instanceId.toString());

    List<InstanceId> deletedUnits = new ArrayList<>();
    boolean deleteStatus = true;
    DeleteSegmentUnitRequest deleteSegmentUnitRequest = buildDeleteSegmentUnitBroadcastRequest(
        new VolumeId(volumeId), segIndex, initMembership);

    try {
      EndPoint endpoint = buildEndPoint(instanceId, instanceMap);
      DataNodeService.Iface client = dataNodeClientFactory.generateSyncClient(endpoint, timeout);
      // we have to delete all segment units. Otherwise, we have to fail the whole creation process.
      client.deleteSegmentUnit(deleteSegmentUnitRequest);
      logger.debug("We have deleted segment unit at the endpoint {}", endpoint);
    } catch (InvalidFormatException e) {
      deleteStatus = false;
      logger.error("Can't build end point for {} ignore this data node,the exception is:",
          instanceMap.get(instanceId).getEndPoint(), e);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger
          .error("Tried to delete a nonexisting segment unit at the datanode {},the exception is ",
              instanceMap.get(instanceId).getEndPoint(), e);
    } catch (Exception e) {
      deleteStatus = false;
      logger.error("Caught unknown exception when delete the segment units :{} segIndex :{}",
          volumeId, segIndex, e);
    }
    if (deleteStatus) {
      return null;
    } else {
      return instanceId;
    }
  }

  private EndPoint buildEndPoint(InstanceId instanceId,
      Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap) {
    InstanceIdAndEndPointThrift instanceIdAndEndPoint = instanceMap.get(instanceId);
    return EndPoint.fromString(instanceIdAndEndPoint.getEndPoint());
  }

  private DeleteSegmentUnitRequest buildDeleteSegmentUnitBroadcastRequest(VolumeId volumeId,
      int segIndex,
      SegmentMembershipThrift initMembership) {
    return new DeleteSegmentUnitRequest(RequestIdBuilder.get(), volumeId.getId(), segIndex,
        initMembership);
  }

  private void makeCreateSegmentUnit(VolumeCreationRequest volumeRequest,
      Map<Integer, Map<SegmentUnitTypeThrift,
          List<InstanceIdAndEndPointThrift>>> segIndex2InstancesForUsed, AccountMetadata account,
      SegmentUnitTypeThrift normalSegmentUnitsType, int numOfSegments, RequestType createType,
      VolumeMetadata rootVolume, VolumeMetadata volumeToBeCreated,
      Map<InstanceId, CreateSegmentUnitBatchRequest> rebuildCreateSegmentUnitRequest,
      Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap) throws RuntimeException {
    instanceMap.clear();
    rebuildCreateSegmentUnitRequest.clear();
    int successCount = 0;
    for (Map.Entry<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>> entry
        : segIndex2InstancesForUsed
        .entrySet()) {
      int currentSegIndex = entry.getKey();
      List<InstanceId> normalInstanceIdListInOrder = new ArrayList<>();
      List<InstanceId> arbiterInstanceIdListInOrder = new ArrayList<>();
      Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>> unitType2InstanceListMap = entry
          .getValue();

      List<InstanceIdAndEndPointThrift> arbiterInstance = unitType2InstanceListMap
          .get(SegmentUnitTypeThrift.Arbiter);
      List<InstanceIdAndEndPointThrift> normalInstance = unitType2InstanceListMap
          .get(normalSegmentUnitsType);
      for (InstanceIdAndEndPointThrift instanceIdAndEndPoint : normalInstance) {
        InstanceId instanceId = new InstanceId(instanceIdAndEndPoint.getInstanceId());
        instanceMap.put(instanceId, instanceIdAndEndPoint);
        normalInstanceIdListInOrder.add(instanceId);
      }
      for (InstanceIdAndEndPointThrift instanceIdAndEndPoint : arbiterInstance) {
        InstanceId instanceId = new InstanceId(instanceIdAndEndPoint.getInstanceId());
        instanceMap.put(instanceId, instanceIdAndEndPoint);
        arbiterInstanceIdListInOrder.add(instanceId);
      }

      try {
        logger.info("new segmentCreator with request: {} and realSegIndex: [{}]", volumeRequest,
            currentSegIndex);
        CreateSegmentUnitsRequest segmentCreator = new CreateSegmentUnitsRequest(instanceMap,
            arbiterInstanceIdListInOrder,
            normalInstanceIdListInOrder, volumeRequest, account, currentSegIndex,
            volumeToBeCreated, normalSegmentUnitsType, rebuildCreateSegmentUnitRequest);

        if (createType == RequestType.EXTEND_VOLUME) {
          segmentCreator.setRootVolume(rootVolume);
        }

        boolean makeRequestStatus = segmentCreator.makeRequest();
        if (makeRequestStatus) {
          successCount++;
          segmentCreatorTable
              .put(new SegId(volumeRequest.getVolumeId(), currentSegIndex), segmentCreator);
        }
      } catch (Exception e) {
        logger.warn("build create volume request:{} caught an exception", volumeRequest, e);
        throw new RuntimeException();
      }
    }


    if (successCount != segIndex2InstancesForUsed.size()) {
      logger.warn("failed to create this passel: {} segmentUnits @volumeId:{},  successCount: {}",
          passelCount,
          volumeRequest.getVolumeId(), successCount);
      throw new RuntimeException(
          "Failed to create this passel segments. ");
    } else {
      logger.info(
          "successfully created this passel segments from volume: {}, passel: {}",
          volumeRequest.getVolumeId(), passelCount);
    }

    return;
  }


  public void createSegments(VolumeCreationRequest request, int startSegmentIndex)
      throws Exception {
    try {
      Map<Integer, Map<SegmentUnitTypeThrift, List<InstanceIdAndEndPointThrift>>>
          mapSegIndexToInstances = reserveInformation.reserveVolume(request);
      createSegmentUnits(request, mapSegIndexToInstances, startSegmentIndex);
    } catch (NotEnoughSpaceException e) {
      logger.error("no space to create segments", e);
      throw e;
    } catch (Exception e) {
      logger.error("cannot create segments", e);
      throw e;
    }
  }


  @Transactional
  public VolumeMetadata getFullVolumeInfo(long volumeId, long accountId)
      throws InternalError, VolumeNotFoundException {
    VolumeMetadata volume;
    try {

      volume = volumeInformationManger.getVolumeNew(volumeId, accountId);
      logger.debug("the volume is {}, the number of segments is {}", volume,
          volume.getSegmentTableSize());
    } catch (VolumeNotFoundException e) {
      logger.error("when create volume, getFullVolumeInfo :{} caught an exception ", volumeId, e);
      throw new VolumeNotFoundException();
    }

    return volume;
  }

  /*
   * Get volume regardless root volume or child volume
   */

  public VolumeMetadata getVolumeForCreateVolume(long volumeId, long accountId)
      throws VolumeNotFoundException {
    VolumeMetadata volume;
    volume = volumeStore.getVolume(volumeId);

    if (volume == null) {
      logger.error("getVolumeForCreateVolume :{}, can not find volume", volumeId);
      throw new VolumeNotFoundException();
    }

    logger.warn("getVolumeForCreateVolume volume: {}", volume);
    return volume;
  }

  private ExecutorService createSegmentUnitsExecutor() {
    return new ThreadPoolExecutor(this.createSegmentUnitThreadPoolCoreSize,
        this.createSegmentUnitThreadPoolMaxSize,
        60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new NamedThreadFactory("CreateSegmentUnitWorker"),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }


  public CreateSegmentUnitsRequest getSegmentCreator(SegId segId) {
    return segmentCreatorTable.get(segId);
  }


  public int getAddOne() {
    return addOne;
  }


  public ReserveInformation getReserveInformation() {
    return reserveInformation;
  }


  public void setReserveInformation(ReserveInformation reserveInformation) {
    this.reserveInformation = reserveInformation;
  }


  public VolumeInformationManger getVolumeInformationManger() {
    return volumeInformationManger;
  }


  public void setVolumeInformationManger(VolumeInformationManger volumeInformationManger) {
    this.volumeInformationManger = volumeInformationManger;
  }


  public InstanceStore getInstanceStore() {
    return instanceStore;
  }


  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }


  public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeClientFactory() {
    return dataNodeClientFactory;
  }


  public void setDataNodeClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory) {
    this.dataNodeClientFactory = dataNodeClientFactory;
  }


  public GenericThriftClientFactory<DataNodeService.AsyncIface> getDataNodeAsyncClientFactory() {
    return dataNodeAsyncClientFactory;
  }


  public void setDataNodeAsyncClientFactory(
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory) {
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
  }


  public ExecutorService getCreateSegmentsExecutor() {
    return createSegmentsExecutor;
  }


  public void setCreateSegmentsExecutor(ExecutorService createSegmentsExecutor) {
    this.createSegmentsExecutor = createSegmentsExecutor;
  }


  public int getCreateSegmentUnitThreadPoolCoreSize() {
    return createSegmentUnitThreadPoolCoreSize;
  }


  public void setCreateSegmentUnitThreadPoolCoreSize(int createSegmentUnitThreadPoolCoreSize) {
    this.createSegmentUnitThreadPoolCoreSize = createSegmentUnitThreadPoolCoreSize;
  }


  public int getCreateSegmentUnitThreadPoolMaxSize() {
    return createSegmentUnitThreadPoolMaxSize;
  }


  public void setCreateSegmentUnitThreadPoolMaxSize(int createSegmentUnitThreadPoolMaxSize) {
    this.createSegmentUnitThreadPoolMaxSize = createSegmentUnitThreadPoolMaxSize;
  }


  public int getCreateVolumeTimeOutSecond() {
    return createVolumeTimeOutSecond;
  }


  public void setCreateVolumeTimeOutSecond(int createVolumeTimeOutSecond) {
    this.createVolumeTimeOutSecond = createVolumeTimeOutSecond;
  }


  public int getThriftTimeOut() {
    return thriftTimeOut;
  }


  public void setThriftTimeOut(int thriftTimeOut) {
    this.thriftTimeOut = thriftTimeOut;
  }


  public int getPasselCount() {
    return passelCount;
  }


  public void setPasselCount(int passelCount) {
    this.passelCount = passelCount;
  }


  public AccountStore getAccountStore() {
    return accountStore;
  }


  public void setAccountStore(AccountStore accountStore) {
    this.accountStore = accountStore;
  }


  public SegmentCreatorControllerFactory getSegmentCreatorControllerFactory() {
    return segmentCreatorControllerFactory;
  }


  public InfoCenterConfiguration getInfoCenterConfiguration() {
    return infoCenterConfiguration;
  }


  public void setInfoCenterConfiguration(InfoCenterConfiguration infoCenterConfiguration) {
    this.infoCenterConfiguration = infoCenterConfiguration;
  }


  public Map<SegId, CreateSegmentUnitsRequest> getSegmentCreatorTable() {
    return segmentCreatorTable;
  }


  public void setSegmentCreatorTable(Map<SegId, CreateSegmentUnitsRequest> segmentCreatorTable) {
    this.segmentCreatorTable = segmentCreatorTable;
  }


  public VolumeStore getVolumeStore() {
    return volumeStore;
  }


  public void setVolumeStore(VolumeStore volumeStore) {
    this.volumeStore = volumeStore;
  }
}
