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

package py.infocenter.rebalance.old.processor;

import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.infocenter.rebalance.old.RebalanceTaskContext;
import py.infocenter.rebalance.old.RebalanceTaskExecutionResult;
import py.infocenter.rebalance.old.RebalanceTaskProcessor;
import py.infocenter.service.InformationCenterImpl;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.rebalance.RebalanceTask;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.MigratePrimaryRequest;
import py.thrift.icshare.GetSegmentRequest;
import py.thrift.icshare.GetSegmentResponse;
import py.thrift.share.VolumeNotFoundExceptionThrift;


public class MigratePrimaryProcessor extends RebalanceTaskProcessor {

  private static final Logger logger = LoggerFactory.getLogger(MigratePrimaryProcessor.class);

  private final InstanceStore instanceStore;
  private final GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;


  public MigratePrimaryProcessor(BasicRebalanceTaskContext context, InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
      InformationCenterImpl informationCenter) {
    super(context, informationCenter);
    this.instanceStore = instanceStore;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
  }

  private RebalanceTaskExecutionResult sendRequest(RebalanceTask rebalanceTask) {
    SegmentUnitMetadata segUnit = rebalanceTask.getSourceSegmentUnit();

    InstanceId sourceId = rebalanceTask.getInstanceToMigrateFrom();
    InstanceId destinationId = rebalanceTask.getDestInstanceId();

    SegmentMetadata segment;
    try {
      segment = getSegment(segUnit.getSegId());
    } catch (VolumeNotFoundExceptionThrift e) {
      logger.warn("volume not found ? discard myself", e);
      return discardMyself();
    } catch (Exception e) {
      logger.warn("exception caught", e);
      return stayHere(false);
    }

    SegmentMembership currentMembership = segment.getLatestMembership();
    if (!currentMembership.isPrimary(sourceId) || !currentMembership.isSecondary(destinationId)) {
      logger
          .warn("membership changed for {}, the latest membership {}", segUnit, currentMembership);
      return discardMyself();
    } else if (currentMembership.isPrimaryCandidate(destinationId)) {
      logger.info("destination is already primary candidate in membership");
      return nextPhase();
    } else if (currentMembership.getPrimaryCandidate() != null) {
      logger.error("there is another primary candidate in membership {} {}", currentMembership,
          segUnit);
      return discardMyself();
    } else if (currentMembership.getSecondaryCandidate() != null) {
      logger
          .error("there is a secondary candidate in membership {} {}", currentMembership, segUnit);
      return discardMyself();
    } else if (!currentMembership.getInactiveSecondaries().isEmpty() || !currentMembership
        .getJoiningSecondaries()
        .isEmpty()) {
      logger.warn("current membership has joining secondary or inactive secondary, wait .. {}",
          currentMembership);
      return stayHere(true);
    }

    Instance source = instanceStore.get(sourceId);
    if (source == null) {
      logger.warn("can't get source instance from instance store {}", sourceId);
      return discardMyself();
    }

    EndPoint ep = source.getEndPoint();
    DataNodeService.Iface dnClient;
    try {
      dnClient = dataNodeSyncClientFactory.generateSyncClient(ep);
    } catch (GenericThriftClientFactoryException e) {
      logger.info("can't generate data node client", e);
      return stayHere(false);
    }

    MigratePrimaryRequest request = new MigratePrimaryRequest(RequestIdBuilder.get(),
        rebalanceTask.getDestInstanceId().getId(),
        RequestResponseHelper.buildThriftSegIdFrom(segUnit.getSegId()));
    try {
      dnClient.migratePrimary(request);
      return nextPhase();
    } catch (TException e) {
      logger.warn("exception caught while migrating primary for {}", segUnit.getSegId(), e);
      return stayHere(false);
    }
  }

  private RebalanceTaskExecutionResult checkProgress(RebalanceTask rebalanceTask) {
    SegmentUnitMetadata segUnit = rebalanceTask.getSourceSegmentUnit();

    InstanceId sourceId = rebalanceTask.getInstanceToMigrateFrom();
    InstanceId destinationId = rebalanceTask.getDestInstanceId();

    SegmentMetadata segment;
    try {
      segment = getSegment(segUnit.getSegId());
    } catch (VolumeNotFoundExceptionThrift e) {
      logger.warn("volume not found ? discard myself", e);
      return discardMyself();
    } catch (Exception e) {
      logger.warn("exception caught", e);
      return stayHere(false);
    }

    // check membership
    SegmentMembership membership = segment.getLatestMembership();
    if (membership.getPrimary()
        .equals(sourceId)) { // source primary is still primary in membership, we should wait
      if (membership.isPrimaryCandidate(destinationId)) {
        // if the destination is already primary candidate in membership, then we could wait forever
        return stayHere(true);
      } else {
       
       
       
        return stayHere(false);
      }
    } else if (membership.getPrimary()
        .equals(destinationId)) { // destination is already primary in membership
      int primaryCount = 0;
      int otherCount = 0;
      for (SegmentUnitMetadata segmentUnitMetadata : segment.getSegmentUnits()) {
        switch (segmentUnitMetadata.getStatus()) {
          case Primary:
            primaryCount++;
            break;
          case Secondary:
            break;
          case Arbiter:
            break;
          default:
            otherCount++;
            break;
        }
      }

      if (primaryCount == 1 && otherCount == 0) {
        if (segment.getSegmentUnitMetadata(destinationId).getStatus()
            == SegmentUnitStatus.Primary) {
          // only when the destination is already primary, will we go to next phase
          return nextPhase();
        } else {
          return stayHere(true);
        }
      } else {
        return stayHere(true);
      }
    } else { // someone else has become primary
      return discardMyself();
    }
  }

  private SegmentMetadata getSegment(SegId segId)
      throws VolumeNotFoundExceptionThrift, TException {
    GetSegmentRequest getSegmentRequest = new GetSegmentRequest();
    getSegmentRequest.setRequestId(RequestIdBuilder.get());
    getSegmentRequest.setVolumeId(segId.getVolumeId().getId());
    getSegmentRequest.setSegmentIndex(segId.getIndex());

    GetSegmentResponse getSegmentResponse = informationCenter.getSegment(getSegmentRequest);
    return RequestResponseHelper.buildSegmentMetadataFrom(getSegmentResponse.getSegment());
  }

  @Override
  public RebalanceTaskExecutionResult call() {
    try {
      RebalanceTaskContext context = getContext();
      MigratePrimaryContext myContext = (MigratePrimaryContext) context;

      logger.info("migrate primary processing {}", myContext);
      RebalanceTask rebalanceTask = myContext.getRebalanceTask();
      Validate
          .isTrue(rebalanceTask.getTaskType() == RebalanceTask.RebalanceTaskType.PrimaryRebalance);

      if (myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN) {
        return sendRequest(rebalanceTask);
      } else if (myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT) {
        return checkProgress(rebalanceTask);
      } else {
        logger.error("unsupported phase {}", myContext);
        throw new IllegalArgumentException();
      }
    } catch (Throwable t) {
      logger.warn("caught an mystery throwable", t);
      return stayHere(false);
    }
  }
}
