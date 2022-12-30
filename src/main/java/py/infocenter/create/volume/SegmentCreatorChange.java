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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.counter.ObjectCounter;
import py.common.counter.TreeSetObjectCounter;
import py.common.struct.EndPoint;
import py.instance.InstanceId;
import py.thrift.datanode.service.CreateSegmentUnitBatchRequest;
import py.thrift.datanode.service.CreateSegmentUnitBatchResponse;
import py.thrift.datanode.service.CreateSegmentUnitFailedCodeThrift;
import py.thrift.datanode.service.CreateSegmentUnitFailedNode;
import py.thrift.datanode.service.CreateSegmentUnitNode;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.AsyncIface;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.SegmentExistingExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;


public class SegmentCreatorChange implements Callable<Boolean> {

  private static final Logger logger = LoggerFactory.getLogger(SegmentCreatorChange.class);
  // the list of ids of data node instances that can be used to create segment units

  private final Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap;
  private final GenericThriftClientFactory<AsyncIface> dataNodeAsyncClientFactory;
  private final long timeoutSeconds;
  private final CreateSegmentUnitBatchRequest createSegmentUnitRequest;
  private final InstanceId instanceId;
  private SegmentCreatorControllerFactory.SegmentCreatorController controller;
  private boolean resultStatus;
  private Map<InstanceId, List<Integer>> goodSegIndex;
  private Map<InstanceId, List<CreateSegmentUnitFailedNode>> badSegIndex;
  private volatile AtomicInteger numGoodResponses;
  private volatile AtomicInteger numBadResponses;
  private volatile AtomicInteger disconnectSet;
  private volatile AtomicInteger setFailedValue;


  public SegmentCreatorChange(InstanceId instanceId,
      CreateSegmentUnitBatchRequest createSegmentUnitRequest,
      GenericThriftClientFactory<AsyncIface> dataNodeAsyncClientFactory, long timeoutSeconds,
      Map<InstanceId, InstanceIdAndEndPointThrift> instanceMap,
      Map<InstanceId, List<Integer>> goodSegIndex,
      Map<InstanceId, List<CreateSegmentUnitFailedNode>> badSegIndex) throws InterruptedException {

    this.instanceId = instanceId;
    this.createSegmentUnitRequest = createSegmentUnitRequest;
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    this.timeoutSeconds = timeoutSeconds;
    this.instanceMap = instanceMap;
    this.goodSegIndex = goodSegIndex;
    this.badSegIndex = badSegIndex;
    this.numGoodResponses = new AtomicInteger(0);
    this.numBadResponses = new AtomicInteger(0);
    this.disconnectSet = new AtomicInteger(0);
    this.setFailedValue = new AtomicInteger(0);
  }

  @Override
  public Boolean call() throws Exception {
    // send async requests to the candidates to create segment units
    createSegmentUnits(instanceId, createSegmentUnitRequest, numGoodResponses, numBadResponses,
        disconnectSet);
    if (resultStatus) {
      if (null != controller) {
        controller.block();
      }
      return true;
    } else {
      return false;
    }
  }


  public void close() {
    if (null != controller) {
      controller.release();
    }
  }

  public void setController(SegmentCreatorControllerFactory.SegmentCreatorController controller) {
    this.controller = controller;
  }

  private void createSegmentUnits(InstanceId instanceId,
      CreateSegmentUnitBatchRequest createSegmentUnitRequest,
      AtomicInteger numGoodResponses, AtomicInteger numBadResponses, AtomicInteger disconnectSet) {
    CountDownLatch latch = new CountDownLatch(1);
    EndPoint endpoint = null;
    long currentInstanceId = instanceId.getId();

    ObjectCounter<Long> primaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> secondaryIdCounter = new TreeSetObjectCounter<>();
    ObjectCounter<Long> arbiterIdCounter = new TreeSetObjectCounter<>();
    Map<Long, ObjectCounter<Long>> primaryId2SecondaryIdCounterMap = new HashMap<>();

    try {
      endpoint = buildEndPoint(instanceId);
      AsyncIface dataNodeClient = dataNodeAsyncClientFactory
          .generateAsyncClient(endpoint, TimeUnit.SECONDS.toMillis(timeoutSeconds));
      dataNodeClient.createSegmentUnitBatch(createSegmentUnitRequest,
          new NewCreateSegmentUnitMethodCallback(createSegmentUnitRequest, instanceId, endpoint,
              latch));

      List<CreateSegmentUnitNode> segmentUnits = createSegmentUnitRequest.getSegmentUnits();
      for (CreateSegmentUnitNode createSegmentUnitNode : segmentUnits) {
        SegmentMembershipThrift membershipThrift = createSegmentUnitNode.getInitMembership();
        long primaryId = membershipThrift.getPrimary();
        Set<Long> secondaries = membershipThrift.getSecondaries();
        Set<Long> arbiters = membershipThrift.getArbiters();

        //get the p
        if (currentInstanceId == primaryId) {
          primaryIdCounter.increment(primaryId);

          ObjectCounter<Long> secondaryOfPrimaryCounter = primaryId2SecondaryIdCounterMap
              .computeIfAbsent(primaryId,
                  k -> new TreeSetObjectCounter<>());

          //count necessary secondary
          for (Long secondary : secondaries) {
            secondaryOfPrimaryCounter.increment(secondary);
          }
          continue;
        }

        //get s
        if (secondaries.contains(currentInstanceId)) {
          secondaryIdCounter.increment(currentInstanceId);
          continue;
        }

        //get a
        if (arbiters.contains(currentInstanceId)) {
          arbiterIdCounter.increment(currentInstanceId);
        }
      }

      logger.warn("first createSegmentUnit request to dataNode:{} {}, the primaryIdCounter :{}",
          endpoint,
          currentInstanceId, primaryIdCounter);
      logger.warn(
          "first createSegmentUnit request to dataNode:{} {}, when as primary, the secondary and "
              + "arbiter is :{}",
          endpoint,
          currentInstanceId, primaryId2SecondaryIdCounterMap);
      logger.warn(
          "first send createSegmentUnit request to dataNode:{} {}, the secondaryIdCounter :{}",
          endpoint, currentInstanceId, secondaryIdCounter);
      logger.warn(
          "first createSegmentUnit request send to dataNode:{} {}, arbiterIdCounter :{}, and  the"
              + " all count :{}",
          endpoint, currentInstanceId, arbiterIdCounter,
          primaryIdCounter.total() + secondaryIdCounter.total() + arbiterIdCounter.total());
      logger.info("first createSegmentUnit request send to dataNode:{}! request: {}", endpoint,
          createSegmentUnitRequest);
    } catch (Throwable e) {
      badSegIndex.put(instanceId, failedSegmentIndexForCreateSegmentUnit());
      latch.countDown();
      logger.error(
          "Caught an exception to first createSegmentUnit request, endpoint: {},such exceptions.",
          endpoint.toString(), e);
    }

    /* if time out, resend once again */
    try {
      if (!latch.await(timeoutSeconds, TimeUnit.SECONDS)) {
        badSegIndex.put(instanceId, failedSegmentIndexForCreateSegmentUnit());
        logger.error("first createSegments request timeout! the request:{}",
            createSegmentUnitRequest);
      }

      logger.warn(
          "about volume :{} first createSegments request over to {}! good Responses number:{}, "
              + "bad Responses number:{}, "

              + "and disconnect :{}", createSegmentUnitRequest.getVolumeId(), endpoint,
          numGoodResponses.get(),
          numBadResponses.get(), disconnectSet);

      /* resend once when disconnect */
      if (disconnectSet.get() == 1) {
        CountDownLatch latchResend = new CountDownLatch(1);
        numGoodResponses.set(0);
        numBadResponses.set(0);
        try {
          endpoint = buildEndPoint(instanceId);
          AsyncIface dataNodeClient = dataNodeAsyncClientFactory
              .generateAsyncClient(endpoint, TimeUnit.SECONDS.toMillis(timeoutSeconds));
          dataNodeClient.createSegmentUnitBatch(createSegmentUnitRequest,
              new NewCreateSegmentUnitMethodCallback(createSegmentUnitRequest, instanceId, endpoint,
                  latchResend));

          logger.warn(
              "when disconnect, resend createSegmentUnit request to dataNode:{} {}, the "
                  + "primaryIdCounter :{}",
              endpoint,
              currentInstanceId, primaryIdCounter);
          logger.warn(
              "when disconnect, resend createSegmentUnit request to dataNode:{} {}, when as "
                  + "primary, the secondary and arbiter is :{}",
              endpoint, currentInstanceId, primaryId2SecondaryIdCounterMap);
          logger.warn(
              "when disconnect, resend createSegmentUnit request to dataNode:{} {}, the "
                  + "secondaryIdCounter :{}",
              endpoint, currentInstanceId, secondaryIdCounter);
          logger.warn(
              "when disconnect, resend createSegmentUnit request send to dataNode:{} {}, "
                  + "arbiterIdCounter :{}, and  the all count :{}",
              endpoint, currentInstanceId, arbiterIdCounter,
              primaryIdCounter.total() + secondaryIdCounter.total() + arbiterIdCounter.total());

          logger
              .info("when disconnect, resend createSegmentUnit request to dataNode:{}! request:{}",
                  endpoint, createSegmentUnitRequest);
        } catch (Throwable e) {
          badSegIndex.put(instanceId, failedSegmentIndexForCreateSegmentUnit());
          latchResend.countDown();
          logger.error(
              "Caught an exception to disconnect createSegmentUnit request, endpoint: {}, such "
                  + "exceptions.",
              endpoint.toString(), e);
        }

        if (!latchResend.await(timeoutSeconds, TimeUnit.SECONDS)) {
          //** time out again, set the all SegmentUnit create failed **/
          badSegIndex.put(instanceId, failedSegmentIndexForCreateSegmentUnit());
          logger.error("resend createSegments request timeout! , last segment unit request:{} ",
              createSegmentUnitRequest);
        }
      }

      logger.info("createSegments request over! the request:{}", createSegmentUnitRequest);

    } catch (InterruptedException e) {
      badSegIndex.put(instanceId, failedSegmentIndexForCreateSegmentUnit());
      logger.warn("interrupted while can't wait for all push request coming back ", e);
    }
  }

  private EndPoint buildEndPoint(InstanceId instanceId) {
    InstanceIdAndEndPointThrift instanceIdAndEndPoint = instanceMap.get(instanceId);
    return EndPoint.fromString(instanceIdAndEndPoint.getEndPoint());
  }

  private List<CreateSegmentUnitFailedNode> failedSegmentIndexForCreateSegmentUnit() {
    List<CreateSegmentUnitFailedNode> failedIndex = new ArrayList<>();
    CreateSegmentUnitFailedNode createSegmentUnitFailedNode = new CreateSegmentUnitFailedNode();
    List<CreateSegmentUnitNode> segmentUnitNodes = createSegmentUnitRequest.getSegmentUnits();
    for (CreateSegmentUnitNode node : segmentUnitNodes) {
      CreateSegmentUnitFailedCodeThrift errorCode =
          CreateSegmentUnitFailedCodeThrift.UnknownException;
      createSegmentUnitFailedNode.setSegIndex(node.getSegIndex());
      createSegmentUnitFailedNode.setErrorCode(errorCode);

      failedIndex.add(createSegmentUnitFailedNode);
    }

    return failedIndex;
  }

  class NewCreateSegmentUnitMethodCallback implements
      AsyncMethodCallback<DataNodeService.AsyncClient.createSegmentUnitBatch_call> {

    private final EndPoint endpoint;
    private final InstanceId instanceId;
    private CreateSegmentUnitBatchRequest request;
    private volatile CountDownLatch latch;

    public NewCreateSegmentUnitMethodCallback(
        CreateSegmentUnitBatchRequest createSegmentUnitRequest, InstanceId instanceId,
        EndPoint endpoint,
        CountDownLatch latch) {
      this.request = createSegmentUnitRequest;
      this.endpoint = endpoint;
      this.instanceId = instanceId;
      this.latch = latch;
    }

    @Override
    public void onComplete(
        DataNodeService.AsyncClient.createSegmentUnitBatch_call createSegmentUnitBatchCall) {
      try {
        resultStatus = true;
        CreateSegmentUnitBatchResponse response = createSegmentUnitBatchCall.getResult();
        int totalSize = response.getSuccessedSegsSize() + response.getFailedSegsSize();
        if (totalSize == 0) {
          logger.error(
              "when create volume onComplete, but the response number is error : {}, endpoint is:"
                  + " {}",
              totalSize, endpoint);
          throw new RuntimeException(
              "Failed to create all segments because the response number is error");
        }

        goodSegIndex.put(instanceId, response.getSuccessedSegs());
        badSegIndex.put(instanceId, response.getFailedSegs());
        numGoodResponses.incrementAndGet();

      } catch (SegmentExistingExceptionThrift e) {
        numGoodResponses.incrementAndGet();
        logger.error("the segment unit that we want to create already exists", e);
      } catch (Throwable e) {
        numBadResponses.incrementAndGet();
        logger.error(
            "onComplete. The request is {} The response has an exception: {}, endpoint is: {}",
            request,
            e, endpoint);
      }

      logger
          .warn("onComplete the instanceId is :{} , goodSegIndex :{}, badSegIndex :{}", instanceId,
              goodSegIndex, badSegIndex);
      //** have one data node have response **/
      latch.countDown();
    }

    // client side exception was throw, the response does not come from the
    // server side
    public void onError(Exception e) {
      setFailedValue.incrementAndGet();
      resultStatus = false;
      if (e instanceof TTransportException || e instanceof IOException) {
        disconnectSet.incrementAndGet();
        logger.error("may be connect is reset by peer, put it to resend list", e);
      }

      //** so, we think the all SegmentUnit create failed **/
      numBadResponses.incrementAndGet();


      if (setFailedValue.get() == 2 || disconnectSet.get() == 0) {
        badSegIndex.put(instanceId, failedSegmentIndexForCreateSegmentUnit());
      }

      logger.error(
          "onError. failed to send request:{} to the datanode:{}, the goodSegIndex :{}, "
              + "badSegIndex :{}, failed value :{}, "

              + "disconnect value :{}", request, endpoint, goodSegIndex, badSegIndex,
          setFailedValue.get(), disconnectSet.get(), e);

      //** have one datanode have response **/
      latch.countDown();
    }
  }
}
