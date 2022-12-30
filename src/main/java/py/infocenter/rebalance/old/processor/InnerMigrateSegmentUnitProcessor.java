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
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.infocenter.rebalance.old.RebalanceTaskContext;
import py.infocenter.rebalance.old.RebalanceTaskExecutionResult;
import py.infocenter.rebalance.old.RebalanceTaskProcessor;
import py.infocenter.service.InformationCenterImpl;
import py.instance.Instance;
import py.instance.InstanceStore;
import py.rebalance.RebalanceTask;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.InnerMigrateSegmentUnitRequest;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.icshare.GetSegmentRequest;
import py.thrift.icshare.GetSegmentResponse;
import py.thrift.share.ArchiveNotFoundExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;


public class InnerMigrateSegmentUnitProcessor extends RebalanceTaskProcessor {

  private static final Logger logger = LoggerFactory
      .getLogger(InnerMigrateSegmentUnitProcessor.class);

  private final InstanceStore instanceStore;
  private final GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;


  public InnerMigrateSegmentUnitProcessor(BasicRebalanceTaskContext context,
      InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
      InformationCenterImpl informationCenter) {
    super(context, informationCenter);
    this.instanceStore = instanceStore;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
  }

  private RebalanceTaskExecutionResult internalProcess(InnerMigrateSegmentUnitContext myContext) {
    logger.info("inner migrating processing {}", myContext);
    if (myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN) {
      Instance destination = instanceStore.get(myContext.getRebalanceTask().getDestInstanceId());
      if (destination == null) {
        logger.error("cannot get the destination {}, discard myself",
            myContext.getRebalanceTask().getDestInstanceId());
        return discardMyself();
      }
      EndPoint destinationEndPoint = destination.getEndPoint();
      RebalanceTask task = myContext.getRebalanceTask();
      try {
        DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
            .generateSyncClient(destinationEndPoint);
        InnerMigrateSegmentUnitRequest request = new InnerMigrateSegmentUnitRequest(
            RequestIdBuilder.get(),
            RequestResponseHelper.buildThriftSegIdFrom(task.getSourceSegmentUnit().getSegId()),
            task.getTargetArchiveId());
        dataNodeClient.innerMigrateSegmentUnit(request);
        return nextPhase();
      } catch (GenericThriftClientFactoryException e) {
        logger.warn("can't build data node client");
        return stayHere(false);
      } catch (ArchiveNotFoundExceptionThrift e) {
        logger.warn("archive not found {}", myContext);
        return discardMyself();
      } catch (SegmentNotFoundExceptionThrift segmentNotFoundExceptionThrift) {
        logger.warn("segment not found {}", myContext);
        return discardMyself();
      } catch (ServiceHavingBeenShutdownThrift e) {
        logger.warn("service having been shutdown");
        return discardMyself();
      } catch (TException e) {
        logger.warn("catch a mystery exception {}", myContext, e);
        return stayHere(false);
      }
    } else {
      RebalanceTask task = myContext.getRebalanceTask();
      SegId segId = task.getSourceSegmentUnit().getSegId();
      GetSegmentRequest getSegmentRequest = new GetSegmentRequest();
      getSegmentRequest.setRequestId(RequestIdBuilder.get());
      getSegmentRequest.setVolumeId(segId.getVolumeId().getId());
      getSegmentRequest.setSegmentIndex(segId.getIndex());
      GetSegmentResponse getSegmentResponse = null;
      try {
        getSegmentResponse = informationCenter.getSegment(getSegmentRequest);
      } catch (TException e) {
        logger.warn("can't get segment from infocenter", e);
        return stayHere(false);
      }

      SegmentMetadata segment = RequestResponseHelper
          .buildSegmentMetadataFrom(getSegmentResponse.getSegment());
      SegmentUnitMetadata segmentUnit = segment.getSegmentUnitMetadata(task.getDestInstanceId());

      if (segmentUnit.getArchiveId() == task.getTargetArchiveId()) {
        logger.warn("task done !  context {}", myContext);
        return nextPhase();
      }

      if (segmentUnit.isInnerMigrating()) {
        myContext.setStartedMigrating(true);
        return stayHere(true);
      } else {
        if (myContext.isStartedMigrating()) {
          return discardMyself();
        } else {
          return stayHere(false);
        }
      }
    }
  }

  @Override
  public RebalanceTaskExecutionResult call() {
    try {
      RebalanceTaskContext context = getContext();
      InnerMigrateSegmentUnitContext myContext = (InnerMigrateSegmentUnitContext) context;
      Validate.isTrue(myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN
              || myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT,
          "phase must be TASK_GOTTEN or REQUEST_SENT");
      return internalProcess(myContext);
    } catch (Throwable t) {
      logger.warn("caught an mystery throwable", t);
      return stayHere(false);
    }
  }
}
