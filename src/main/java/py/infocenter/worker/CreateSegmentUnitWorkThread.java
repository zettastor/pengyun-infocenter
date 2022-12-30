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

import static py.RequestResponseHelper.convertFromSegmentUnitTypeThrift;
import static py.connection.pool.ConnectionRequest.logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.transport.TTransportException;
import py.archive.segment.SegId;
import py.archive.segment.SegmentVersion;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.icshare.InstanceMaintenanceDbStore;
import py.icshare.InstanceMaintenanceInformation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.rebalance.ReserveSegUnitResult;
import py.infocenter.rebalance.ReserveSegUnitsInfo;
import py.infocenter.rebalance.SegmentUnitsDistributionManager;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.infocenter.service.CreateSegmentUnitRequest;
import py.thrift.share.CloneTypeThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitRoleThrift;
import py.thrift.share.SegmentUnitTypeThrift;


public class CreateSegmentUnitWorkThread {

  private static final int DEFAULT_CONCURRENT_INNER_CREATE_REQUEST = 500;
  private static final int EXIT_SEGMENT_INDEX = -1;
  /*
   * this lock was for threadStop concurrence
   * */
  private final ReentrantLock lock;
  private BlockingQueue<CreateSegmentUnitRequest> createRequests = new LinkedBlockingQueue<>();
  private Thread thread;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
  private SegmentUnitsDistributionManager segmentUnitsDistributionManager;
  private InstanceMaintenanceDbStore instanceMaintenanceStore;
  private long segmentSize;
  private int timeout;
  //use a map to reject duplicated request
  private Map<SegId, SegmentVersion> pendingRequest = new ConcurrentHashMap<>();
  private Boolean threadStop = false;
  private InfoCenterAppContext infoCenterAppContext;


  
  public CreateSegmentUnitWorkThread(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory,
      SegmentUnitsDistributionManager segmentUnitsDistributionManager, long segmentSize,
      int timeout,
      InstanceMaintenanceDbStore instanceMaintenanceStore) {
    this.dataNodeClientFactory = dataNodeClientFactory;
    this.segmentUnitsDistributionManager = segmentUnitsDistributionManager;
    this.segmentSize = segmentSize;
    this.timeout = timeout;
    this.lock = new ReentrantLock();
    this.instanceMaintenanceStore = instanceMaintenanceStore;
  }


  
  public void start() {
    thread = new Thread("create-segment-unit") {
      @Override
      public void run() {
        boolean threadRunning = true;
        List<CreateSegmentUnitRequest> requestList = new ArrayList<>();
        while (threadRunning) {
          requestList.clear();
          try {
            int listSize = createRequests.drainTo(requestList);
            if (listSize <= 0) {
              Thread.sleep(1000);
              logger.info("no request got");
              continue;
            }

            for (int index = 0; index < listSize; index++) {
              CreateSegmentUnitRequest request = requestList.get(index);

              // was request for exit requested ?
              if (request.getSegIndex() == EXIT_SEGMENT_INDEX) {
                logger.warn("exit thread which creating segment unit internally");
                threadRunning = false;
                break;
              }

              long timeout = request.getRequestTimeoutMillis();
              if (timeout > 0) {
                long nowTime = System.currentTimeMillis();
                if (nowTime > timeout + request.getRequestTimestampMillis()) {
                  /*  Timeout*/
                  logger.warn("CreateSegmentUnitRequest Timeout:{}", request);
                  pendingRequest.remove(new SegId(request.getVolumeId(), request.getSegIndex()));
                  continue;
                }
              }

              SegmentMembershipThrift initMembership = request.getInitMembership();
              if (initMembership != null) {
                boolean duplicate = false; /* the last request don't check, so this bool should 
                be set false */
                /* check duplicate request */
                for (int offset = index + 1; offset < listSize; offset++) {
                  CreateSegmentUnitRequest remainedRequest = requestList.get(offset);
                  if (remainedRequest.getSegIndex() == EXIT_SEGMENT_INDEX) {
                    //don't match EXIT_SEGMENT_INDEX
                    continue;
                  }


                }

                int numberOfInstancesInMaintenance = 0;
                for (long instanceId : initMembership.getInactiveSecondaries()) {
                  InstanceMaintenanceInformation instanceInMaintenance = instanceMaintenanceStore
                      .getById(instanceId);
                  if (instanceInMaintenance != null) {
                    if (System.currentTimeMillis() < instanceInMaintenance.getEndTime()) {
                      numberOfInstancesInMaintenance++;
                    } else {
                      instanceMaintenanceStore.delete(instanceInMaintenance);
                    }
                  }
                }

                if (initMembership.getInactiveSecondariesSize() == numberOfInstancesInMaintenance) {
                  pendingRequest.remove(new SegId(request.getVolumeId(), request.getSegIndex()));
                  continue;
                }
              }

              logger.warn("start to processing {}", request);
              process(request);
              logger.warn("done processing {}", request);
            }
          } catch (Throwable t) {
            logger.error("caught an exception", t);
            if (threadRunning == false) {
              //thread will stop.
              break;
            }
            // when catch Throwable. check EXIT_SEGMENT in the reqestList.
            int listSize = requestList.size();
            for (int index = 0; index < listSize; index++) {
              if (requestList.get(index).getSegIndex() == EXIT_SEGMENT_INDEX) {
                logger
                    .warn("finally exit thread which creating segment unit internally. listSize:{}",
                        listSize);
                threadRunning = false;
                break;
              }
            }

          }
        }
      }
    };
    thread.start();
  }


  
  public boolean add(CreateSegmentUnitRequest request) {
    Validate.isTrue(request.isSetSegmentWrapSize());
    boolean ret = true;
    if (thread == null) {
      logger.warn("thread is null !");
      return process(request);
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentVersion version = new SegmentVersion(request.getInitMembership().getEpoch(),
        request.getInitMembership().getGeneration());
    if (version.equals(pendingRequest.get(segId))) {
      if (createRequests.size() > 0) {
        logger.warn("there is a pending create segment unit request, request: {}, {}", segId,
            version);
        return false;
      } else {
        logger.error(
            "what is wrong, there is a pending create segment unit request, request: {}, {}, but "
                + "not in queue",
            segId, version);
      }
    }

    lock.lock();
    try {
      if (threadStop) {
        logger.warn("thread is Stopped");
        ret = false;
      } else {
        pendingRequest.put(segId, version);
        createRequests.add(request);
      }
    } finally {
      lock.unlock();
    }

    return ret;
  }


  
  public boolean process(CreateSegmentUnitRequest request) {
    if (request.isFixVolume()) {
      return processFixVolume(request);
    } else {
      return processNormally(request);
    }
  }

  private boolean processFixVolume(CreateSegmentUnitRequest request) {
    boolean created = false;

    SegmentUnitTypeThrift segmentUnitType;
    if (SegmentUnitRoleThrift.Arbiter == request.getSegmentRole()) {
      segmentUnitType = SegmentUnitTypeThrift.Arbiter;
    } else {
      // TODO consider SegmentUnitTypeThrift.Flexible
      segmentUnitType = SegmentUnitTypeThrift.Normal;
    }

    for (Map.Entry<InstanceIdAndEndPointThrift, SegmentMembershipThrift> entry : request
        .getSegmentMembershipMap().entrySet()) {
      py.thrift.datanode.service.CreateSegmentUnitRequest createSegmentUnitRequest =
          new py.thrift.datanode.service.CreateSegmentUnitRequest(
              request.getRequestId(), request.getVolumeId(), request.getSegIndex(),
              request.getVolumeType(), request.getStoragePoolId(), segmentUnitType,
              request.getSegmentWrapSize(),
              request.isEnableLaunchMultiDrivers(),
              request.getVolumeSource());

      createSegmentUnitRequest.setInitMembership(entry.getValue());
      EndPoint endpoint = EndPoint.fromString(entry.getKey().getEndPoint());
      int tryTime = 3;
      for (int i = 0; i < tryTime; i++) {
        try {

          DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endpoint, timeout);
          logger
              .warn("fix volume, going to create segment unit: {} at: {}", createSegmentUnitRequest,
                  endpoint);
          dataNodeClient.createSegmentUnit(createSegmentUnitRequest);
          created = true;
          break;
        } catch (TTransportException te) {
          logger
              .error("Problem calling data node {} to create a segment unit, request:{}", endpoint,
                  createSegmentUnitRequest, te);
          continue;
        } catch (Exception e) {
          // this log may be over flood, so turn to it for debug.
          // If want to see this log, you can turn
          // the debug. Debug log level not affect control center
          // performance
          logger
              .error("Problem calling data node {} to create a segment unit, request:{}", endpoint,
                  createSegmentUnitRequest, e);
          break;
        }
      }
    }
    return created;
  }

  private boolean processNormally(CreateSegmentUnitRequest request) {
    long primary = request.getInitMembership().getPrimary();
    Set<Long> secondaries = request.getInitMembership().getSecondaries();
    Set<Long> arbiters = request.getInitMembership().getArbiters();
    Set<Long> excludedInstanceIds = new HashSet<>();
    excludedInstanceIds.add(primary);
    excludedInstanceIds.addAll(secondaries);
    excludedInstanceIds.addAll(arbiters);
    /*
     * now we exactly need one secondary segmentUnit to join this membership
     */

    ReserveSegUnitsInfo reserveSegUnitsInfo = new ReserveSegUnitsInfo(segmentSize,
        excludedInstanceIds,
        1, request.getVolumeId(), request.getSegIndex(),
        convertFromSegmentUnitTypeThrift(request.getSegmentUnitType()));

    ReserveSegUnitResult reserveSegUnitResult;
    boolean created = false;
    try {
      logger.warn("clientIC.reserveSegUnits reserveSegUnitRequest: {}", reserveSegUnitsInfo);
      reserveSegUnitResult = segmentUnitsDistributionManager.reserveSegUnits(reserveSegUnitsInfo);
      logger.debug("reserve segment unit response:{}", reserveSegUnitResult);
      if (reserveSegUnitResult == null || reserveSegUnitResult.getInstances().size() == 0) {
        logger.error("can not reserve segment unit from infocenter by request:{}", request);
        pendingRequest.remove(new SegId(request.getVolumeId(), request.getSegIndex()));
        return created;
      }
      List<InstanceMetadataThrift> instances = reserveSegUnitResult.getInstances();
      SegmentMembershipThrift segMembership = request.getInitMembership();
      for (InstanceMetadataThrift instance : instances) {
        py.thrift.datanode.service.CreateSegmentUnitRequest createSegmentUnitRequest =
            new py.thrift.datanode.service.CreateSegmentUnitRequest(
                request.getRequestId(), request.getVolumeId(), request.getSegIndex(),
                request.getVolumeType(),
                reserveSegUnitResult.getStoragePoolId(),
                request.getSegmentUnitType(), request.getSegmentWrapSize(),
                request.isEnableLaunchMultiDrivers(), request.getVolumeSource());
        try {
          createSegmentUnitRequest.setInitMembership(segMembership);
          EndPoint endpoint = EndPoint.fromString(instance.getEndpoint());
          DataNodeService.Iface dataNodeClient = dataNodeClientFactory
              .generateSyncClient(endpoint, timeout);
          logger.warn("dataNodeClient {} createSegmentUnit createSegmentUnitRequest: {}", endpoint,
              createSegmentUnitRequest);
          dataNodeClient.createSegmentUnit(createSegmentUnitRequest);
          created = true;
          break;
        } catch (Exception e) {
          // this log may be over flood, so turn to it for debug.
          // If want to see this log, you can turn
          // the debug. Debug log level not affect control center
          // performance
          logger.info("Problem calling data node {} to create a segment unit, request:{}",
              instance.getInstanceId(), createSegmentUnitRequest, e);
          segMembership.getSecondaries().remove(instance.getInstanceId());
        } finally {
          pendingRequest.remove(new SegId(request.getVolumeId(), request.getSegIndex()));
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      pendingRequest.remove(new SegId(request.getVolumeId(), request.getSegIndex()));
    }

    return created;
  }


  
  public void stop() {
    if (thread != null) {
      try {
        CreateSegmentUnitRequest request = new CreateSegmentUnitRequest();
        request.setSegIndex(EXIT_SEGMENT_INDEX);
        addExitSegmentIndex(request);
        thread.join();
      } catch (InterruptedException e) {
        logger.error("caught exception", e);
      }
    }
  }

  private void addExitSegmentIndex(CreateSegmentUnitRequest request) {
    lock.lock();
    try {
      threadStop = true;
      createRequests.add(request);
    } finally {
      lock.unlock();
    }
  }
}