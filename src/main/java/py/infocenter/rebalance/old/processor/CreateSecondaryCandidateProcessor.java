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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.infocenter.rebalance.old.RebalanceTaskContext;
import py.infocenter.rebalance.old.RebalanceTaskExecutionResult;
import py.infocenter.rebalance.old.RebalanceTaskProcessor;
import py.infocenter.service.InformationCenterImpl;
import py.instance.Instance;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.icshare.GetSegmentRequest;
import py.thrift.icshare.GetSegmentResponse;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.SegmentExistingExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;


public class CreateSecondaryCandidateProcessor extends RebalanceTaskProcessor {

  private static final Logger logger = LoggerFactory
      .getLogger(CreateSecondaryCandidateProcessor.class);

  private final InstanceStore instanceStore;
  private final GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;



  public CreateSecondaryCandidateProcessor(BasicRebalanceTaskContext context,
      InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
      InformationCenterImpl informationCenter) {
    super(context, informationCenter);
    this.instanceStore = instanceStore;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
  }

  @Override
  public RebalanceTaskExecutionResult call() {
    try {
      RebalanceTaskContext context = getContext();
      CreateSecondaryCandidateContext myContext = (CreateSecondaryCandidateContext) context;
      Validate.isTrue(myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN
              || myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT,
          "phase must be TASK_GOTTEN or REQUEST_SENT");

      return internalProcess(myContext);

    } catch (Throwable t) {
      logger.warn("caught an mystery throwable", t);
      return stayHere(false);
    }
  }

  private int retrieveSegmentWrapSizeFromInfoCenter(long volumeId)
      throws InternalErrorThrift {
    try {
      ListVolumesRequest listVolumesRequest = new ListVolumesRequest(RequestIdBuilder.get(),
          Constants.SUPERADMIN_ACCOUNT_ID);
      Set<Long> volumeToList = new HashSet<>();
      volumeToList.add(volumeId);
      listVolumesRequest.setVolumesCanBeList(volumeToList);
      ListVolumesResponse response = informationCenter.listVolumes(listVolumesRequest);
      List<VolumeMetadataThrift> volumes = response.getVolumes();
      Validate.isTrue(volumes.size() == 1);
      return volumes.get(0).getSegmentWrappCount();
    } catch (Exception e) {
      logger.warn("can not get segment wrap size from info center for volume {}", volumeId, e);
      throw new InternalErrorThrift().setDetail(e.toString());
    }
  }

  private RebalanceTaskExecutionResult internalProcess(CreateSecondaryCandidateContext myContext) {
    logger.info("creating secondary candidate processing {}", myContext);
    if (myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN) {
      SegmentUnitMetadata replacee = myContext.getRebalanceTask().getSourceSegmentUnit();

      try {
        GetSegmentRequest getSegmentRequest = new GetSegmentRequest();
        getSegmentRequest.setRequestId(RequestIdBuilder.get());
        getSegmentRequest.setVolumeId(replacee.getSegId().getVolumeId().getId());
        getSegmentRequest.setSegmentIndex(replacee.getSegId().getIndex());
        GetSegmentResponse getSegmentResponse = informationCenter.getSegment(getSegmentRequest);
        SegmentMetadata segment = RequestResponseHelper
            .buildSegmentMetadataFrom(getSegmentResponse.getSegment());
        if (!segment.getSegId().equals(replacee.getSegId())) {
          logger.error("no way get here");
          return discardMyself();
        }

        long storagePoolId = getSegmentResponse.getStoragePoolId();
        Instance destination = instanceStore.get(myContext.getRebalanceTask().getDestInstanceId());
        if (destination == null) {
          logger.error("cannot get the destination {}, discard myself",
              myContext.getRebalanceTask().getDestInstanceId());
          return discardMyself();
        }

        int segmentWrapSize = retrieveSegmentWrapSizeFromInfoCenter(
            segment.getSegId().getVolumeId().getId());
        EndPoint destinationEndPoint = destination.getEndPoint();
        DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
            .generateSyncClient(destinationEndPoint);
        CreateSegmentUnitRequest createSegmentUnitRequest = new CreateSegmentUnitRequest(
            RequestIdBuilder.get(),
            segment.getSegId().getVolumeId().getId(), replacee.getSegId().getIndex(),
            replacee.getVolumeType().getVolumeTypeThrift(),
            storagePoolId, replacee.getSegmentUnitType().getSegmentUnitTypeThrift(),
            segmentWrapSize,
            replacee.isEnableLaunchMultiDrivers(),
            RequestResponseHelper.buildThriftVolumeSource(replacee.getVolumeSource()));
        createSegmentUnitRequest.setInitMembership(RequestResponseHelper
            .buildThriftMembershipFrom(segment.getSegId(), segment.getLatestMembership()));
        createSegmentUnitRequest.setReplacee(replacee.getInstanceId().getId());
        createSegmentUnitRequest.setSecondaryCandidate(true);
        try {
          dataNodeClient.createSegmentUnit(createSegmentUnitRequest);
          return nextPhase();
        } catch (NotEnoughSpaceExceptionThrift e) {
          logger.warn("no enough space on the destination");
          return discardMyself();
        } catch (SegmentExistingExceptionThrift e) {
          logger.warn("segment unit exist");
          return discardMyself();
        } catch (ServiceHavingBeenShutdownThrift e) {
          logger.warn("destination is being shutdown");
          return discardMyself();
        }
      } catch (VolumeNotFoundExceptionThrift e) {
        logger.warn("volume not found ? discard myself", e);
        return discardMyself();
      } catch (Exception e) {
        logger.warn("exception caught", e);
        return stayHere(false);
      }
    } else { // check if task done
      SegmentUnitMetadata replacee = myContext.getRebalanceTask().getSourceSegmentUnit();

      try {
        GetSegmentRequest request = new GetSegmentRequest();
        request.setRequestId(RequestIdBuilder.get());
        request.setVolumeId(replacee.getSegId().getVolumeId().getId());
        request.setSegmentIndex(replacee.getSegId().getIndex());
        GetSegmentResponse response = informationCenter.getSegment(request);
        SegmentMetadata segment = RequestResponseHelper
            .buildSegmentMetadataFrom(response.getSegment());
        if (!segment.getSegId().equals(replacee.getSegId())) {
          logger.error("no way get here");
          return discardMyself();
        }

        int primaryCount = 0;
        int secondaryCount = 0;
        int arbiterCount = 0;
        int otherCount = 0;
        List<SegmentUnitMetadata> latestSegmentUnits = segment.getSegmentUnits();
        for (SegmentUnitMetadata latestSegmentUnit : latestSegmentUnits) {
          if (latestSegmentUnit.getStatus() == SegmentUnitStatus.Primary) {
            primaryCount++;
          } else if (latestSegmentUnit.getStatus() == SegmentUnitStatus.Secondary) {
            secondaryCount++;
          } else if (latestSegmentUnit.getStatus() == SegmentUnitStatus.Arbiter) {
            arbiterCount++;
          } else {
            otherCount++;
          }
        }
        logger.debug("segment members : primary {}, secondary {}, other {}", primaryCount,
            secondaryCount,
            otherCount);
        if (primaryCount != 1 || otherCount != 0) {
          return stayHere(true);
        } else if (secondaryCount + arbiterCount == replacee.getVolumeType().getNumMembers() - 1) {
          // segment is stable
          SegmentMembership membership = segment.getLatestMembership();
          if (!membership.contain(replacee.getInstanceId()) && membership
              .isSecondary(myContext.getRebalanceTask().getDestInstanceId())) {
            logger.warn("task done ! the latest membership {}, context {}", membership, myContext);
            return nextPhase();
          } else {
            if (membership.getSecondaryCandidate() == null) {
              return stayHere(false);
            } else {
              return stayHere(true);
            }
          }
        } else {
          return stayHere(true);
        }

      } catch (VolumeNotFoundExceptionThrift e) {
        logger.warn("volume not found ? discard myself", e);
        return discardMyself();
      } catch (TException e) {
        logger.warn("exception caught", e);
        return stayHere(false);
      }
    }
  }

}
