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

package py.infocenter.service2;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.icshare.InstanceMaintenanceDbStore;
import py.infocenter.authorization.InformationCenterDbConfigTest;
import py.infocenter.client.InformationCenterClientWrapper;
import py.infocenter.rebalance.ReserveSegUnitsInfo;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.worker.CreateSegmentUnitWorkThread;
import py.test.TestBase;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.infocenter.service.CreateSegmentUnitRequest;
import py.thrift.share.SegmentMembershipThrift;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class CreateSegmentUnitWorkThreadTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CreateSegmentUnitWorkThread.class);
  @Mock
  DataNodeService.Iface syncClient;
  @Autowired
  InstanceMaintenanceDbStore instanceMaintenanceStore;
  @Mock
  private InformationCenterClientWrapper icClientWrapper;
  @Mock
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;
  @Mock
  private SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;


  @Before
  public void setup() throws Exception {
    dataNodeClientFactory = mock(GenericThriftClientFactory.class);
    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(syncClient);
    instanceMaintenanceStore.clear();
  }


  // test Timeout
  @Test
  @Ignore(value = "todo")
  public void testTimeout() throws Exception {
    final CreateSegmentUnitWorkThread createSegmentUnitWorkThread = new CreateSegmentUnitWorkThread(
        dataNodeClientFactory, segmentUnitsDistributionManager, 16L, 5000,
        instanceMaintenanceStore);

    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest();
    request.setFixVolumeIsSet(false);
    //set timeout
    request.setRequestTimeoutMillis(3000);

    //set timestamp less than (currentTimeMillis + setRequestTimeoutMillis)
    request.setRequestTimestampMillis(System.currentTimeMillis() - 3000);
    request.setSegmentWrapSize(8);
    request.setVolumeId(1L);
    request.setSegIndex(1);
    SegmentMembershipThrift segmentMembershipThrift = new SegmentMembershipThrift();
    segmentMembershipThrift.setEpoch(1);
    segmentMembershipThrift.setGeneration(1);
    segmentMembershipThrift.setInactiveSecondariesIsSet(true);
    segmentMembershipThrift.setInactiveSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setArbiters(Sets.newHashSet(9L));
    request.setInitMembership(segmentMembershipThrift);

    createSegmentUnitWorkThread.start();
    createSegmentUnitWorkThread.add(request);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      //check this request be processed
      verify(segmentUnitsDistributionManager, never())
          .reserveSegUnits(any(ReserveSegUnitsInfo.class));
    } catch (Exception e) {
      assertFalse(true);
    }
    createSegmentUnitWorkThread.stop();
  }


  @Test
  public void testNotTimeout() throws Exception {
    final CreateSegmentUnitWorkThread createSegmentUnitWorkThread = new CreateSegmentUnitWorkThread(
        dataNodeClientFactory, segmentUnitsDistributionManager, 16L, 5000,
        instanceMaintenanceStore);

    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest();
    request.setFixVolumeIsSet(false);
    //set timeout
    request.setRequestTimeoutMillis(3000);

    //set timestamp less than (currentTimeMillis + setRequestTimeoutMillis)
    request.setRequestTimestampMillis(System.currentTimeMillis() - 1000);
    request.setSegmentWrapSize(8);
    request.setVolumeId(1L);
    request.setSegIndex(1);
    SegmentMembershipThrift segmentMembershipThrift = new SegmentMembershipThrift();
    segmentMembershipThrift.setEpoch(1);
    segmentMembershipThrift.setGeneration(1);
    segmentMembershipThrift.setInactiveSecondariesIsSet(true);
    segmentMembershipThrift.setInactiveSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setArbiters(Sets.newHashSet(9L));
    request.setInitMembership(segmentMembershipThrift);

    createSegmentUnitWorkThread.start();
    createSegmentUnitWorkThread.add(request);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      //check this request be processed
      verify(segmentUnitsDistributionManager, times(1))
          .reserveSegUnits(any(ReserveSegUnitsInfo.class));
    } catch (Exception e) {
      assertFalse(true);
    }
    createSegmentUnitWorkThread.stop();
  }

  @Test
  public void testRequestDuplicate() {
    /*
     * the same as VolumeID SegIndex Epoch and Generation.
     */
    final CreateSegmentUnitWorkThread createSegmentUnitWorkThread = new CreateSegmentUnitWorkThread(
        dataNodeClientFactory, segmentUnitsDistributionManager, 16L, 5000,
        instanceMaintenanceStore);
    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest();
    request.setFixVolumeIsSet(false);
    request.setRequestTimeoutMillis(20000);
    request.setRequestTimestampMillis(System.currentTimeMillis());
    request.setSegmentWrapSize(8);
    request.setVolumeId(1L);
    request.setSegIndex(1);
    SegmentMembershipThrift segmentMembershipThrift = new SegmentMembershipThrift();
    segmentMembershipThrift.setEpoch(1);
    segmentMembershipThrift.setGeneration(1);
    segmentMembershipThrift.setInactiveSecondariesIsSet(true);
    segmentMembershipThrift.setInactiveSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setArbiters(Sets.newHashSet(9L));
    request.setInitMembership(segmentMembershipThrift);

    CreateSegmentUnitRequest request2 = new CreateSegmentUnitRequest();
    request2.setFixVolume(false);
    request2.setRequestTimeoutMillis(20000);
    request2.setRequestTimestampMillis(System.currentTimeMillis());
    request2.setSegmentWrapSize(8);
    request2.setVolumeId(1L);
    request2.setSegIndex(1);
    SegmentMembershipThrift segmentMembershipThrift2 = new SegmentMembershipThrift();
    segmentMembershipThrift2.setSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift2.setEpoch(1);
    segmentMembershipThrift2.setGeneration(1);
    segmentMembershipThrift2.setInactiveSecondariesIsSet(true);
    segmentMembershipThrift2.setInactiveSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift2.setArbiters(Sets.newHashSet(9L));
    request2.setInitMembership(segmentMembershipThrift2);

    createSegmentUnitWorkThread.start();
    createSegmentUnitWorkThread.add(request);
    createSegmentUnitWorkThread.add(request2);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      //check this request be processed
      verify(segmentUnitsDistributionManager, times(1))
          .reserveSegUnits(any(ReserveSegUnitsInfo.class));
    } catch (Exception e) {
      assertFalse(true);
    }
    createSegmentUnitWorkThread.stop();
  }

  @Test
  public void testNotRequestDuplicate() {

    /*
     * the different as VolumeID SegIndex Epoch and Generation.
     */
    final CreateSegmentUnitWorkThread createSegmentUnitWorkThread = new CreateSegmentUnitWorkThread(
        dataNodeClientFactory, segmentUnitsDistributionManager, 16L, 5000,
        instanceMaintenanceStore);
    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest();
    request.setFixVolumeIsSet(false);
    request.setRequestTimeoutMillis(20000);
    request.setRequestTimestampMillis(System.currentTimeMillis());
    request.setSegmentWrapSize(8);
    request.setVolumeId(2L);
    request.setSegIndex(2);
    SegmentMembershipThrift segmentMembershipThrift = new SegmentMembershipThrift();
    segmentMembershipThrift.setEpoch(1);
    segmentMembershipThrift.setGeneration(1);
    segmentMembershipThrift.setInactiveSecondariesIsSet(true);
    segmentMembershipThrift.setInactiveSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift.setArbiters(Sets.newHashSet(9L));
    request.setInitMembership(segmentMembershipThrift);

    CreateSegmentUnitRequest request2 = new CreateSegmentUnitRequest();
    request2.setFixVolume(false);
    request2.setRequestTimeoutMillis(20000);
    request2.setRequestTimestampMillis(System.currentTimeMillis());
    request2.setSegmentWrapSize(8);
    request2.setVolumeId(1L);
    request2.setSegIndex(1);
    SegmentMembershipThrift segmentMembershipThrift2 = new SegmentMembershipThrift();
    segmentMembershipThrift2.setSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift2.setEpoch(1);
    segmentMembershipThrift2.setGeneration(1);
    segmentMembershipThrift2.setInactiveSecondariesIsSet(true);
    segmentMembershipThrift2.setInactiveSecondaries(Sets.newHashSet(1L, 2L, 3L));
    segmentMembershipThrift2.setArbiters(Sets.newHashSet(9L));
    request2.setInitMembership(segmentMembershipThrift2);

    createSegmentUnitWorkThread.start();
    createSegmentUnitWorkThread.add(request);
    createSegmentUnitWorkThread.add(request2);

    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      //check this request be processed
      verify(segmentUnitsDistributionManager, times(2))
          .reserveSegUnits(any(ReserveSegUnitsInfo.class));
    } catch (Exception e) {
      assertFalse(true);
    }
    createSegmentUnitWorkThread.stop();
  }

  @Test
  public void testThreadStop() {
    final CreateSegmentUnitWorkThread createSegmentUnitWorkThread = new CreateSegmentUnitWorkThread(
        dataNodeClientFactory, segmentUnitsDistributionManager, 16L, 1000,
        instanceMaintenanceStore);
    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest();
    request.setRequestTimeoutMillis(100);
    request.setRequestTimestampMillis(System.currentTimeMillis() + 5000);
    request.setSegmentWrapSize(8);
    request.setVolumeId(1L);
    request.setSegIndex(1);
    SegmentMembershipThrift segmentMembershipThrift = new SegmentMembershipThrift();
    segmentMembershipThrift.setEpoch(1);
    segmentMembershipThrift.setGeneration(1);
    request.setInitMembership(segmentMembershipThrift);

    createSegmentUnitWorkThread.start();
    createSegmentUnitWorkThread.stop(); /* Thread Stoped */

    assertFalse(createSegmentUnitWorkThread.add(request));
    assertFalse(createSegmentUnitWorkThread.add(request));
  }
}
