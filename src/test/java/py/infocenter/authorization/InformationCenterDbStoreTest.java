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

package py.infocenter.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.testng.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.archive.segment.CloneType;
import py.common.Constants;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.TargetType;
import py.icshare.VolumeCreationRequest;
import py.infocenter.store.control.DeleteVolumeRequest;
import py.infocenter.store.control.OperationDbStore;
import py.infocenter.store.control.OperationStore;
import py.infocenter.store.control.VolumeJobStoreDb;
import py.infocenter.test.utils.InfocenterUtils;
import py.test.TestBase;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.share.CloneTypeThrift;
import py.volume.CacheType;
import py.volume.VolumeType;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class InformationCenterDbStoreTest extends TestBase {

  private static long dayToMillsecond = 24 * 60 * 60 * 1000L; // MS

  @Autowired
  OperationDbStore dbStore;

  @Autowired
  OperationStore operationStore;

  @Autowired
  VolumeJobStoreDb volumeJobStoreDb;


  @Before
  public void init() throws Exception {
    // clear DB and memory
    operationStore.clearMemory();
    dbStore.clearDb();
  }

  @Test
  public void testloademptydb() {
    List<Operation> allOperatonList = new ArrayList<>(); // store all operation list form db or 
    // memory
    List<Operation> activeOperatonList = new ArrayList<>(); // store active operation list from 
    // db or memory
    List<Operation> endOperatonList = new ArrayList<>(); // store end operation list from db or 
    // memory

    // get from memory or db
    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    endOperatonList = operationStore.getEndOperation();

    // assert empty
    assertEquals(allOperatonList.size(), 0);
    assertEquals(activeOperatonList.size(), 0);
    assertEquals(endOperatonList.size(), 0);
  }

  @Test
  public void testSaveOperation() {
    int amountOfOperations = 10;
    int activeCount = 4;
    int endCount = amountOfOperations - activeCount;
    // store active  operation list form setting
    List<Operation> activeOperatonListFromSetting = new ArrayList<>();
    activeOperatonListFromSetting = InfocenterUtils
        .buildActiveOperations(activeCount, TargetType.VOLUME,
            OperationType.CREATE, "TARGETVOLUME");
    // store end operation list form setting build operation
    List<Operation> endOperatonListFromSetting = new ArrayList<>();
    endOperatonListFromSetting = InfocenterUtils.buildEndOperations(endCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");

    // save to memory or db
    // store all operation list form setting
    List<Operation> allOperatonListFromSetting = new ArrayList<>();
    for (Operation operation : activeOperatonListFromSetting) {
      allOperatonListFromSetting.add(operation);
      operationStore.saveOperation(operation);
    }
    for (Operation operation : endOperatonListFromSetting) {
      allOperatonListFromSetting.add(operation);
      operationStore.saveOperation(operation);
    }

    // get from memory or db
    // store all operation list form db or memory
    List<Operation> allOperatonList = new ArrayList<>();
    allOperatonList = operationStore.getAllOperation();
    // store active operation list from db or memory
    List<Operation> activeOperatonList = new ArrayList<>();
    activeOperatonList = operationStore.getActiveOperation();
    // store end operation list from db or memory
    List<Operation> endOperatonList = new ArrayList<>();
    endOperatonList = operationStore.getEndOperation();

    // assert numbers of operation will the same
    assertEquals(allOperatonList.size(), amountOfOperations);
    assertEquals(allOperatonListFromSetting.size(), amountOfOperations);
    assertEquals(activeOperatonList.size(), activeCount);
    assertEquals(activeOperatonListFromSetting.size(), activeCount);
    assertEquals(endOperatonList.size(), endCount);
    assertEquals(endOperatonListFromSetting.size(), endCount);

    // assert operation will the same
    for (Operation activeOperationFromDb : activeOperatonList) {
      for (Operation activeOperationFromSetting : activeOperatonListFromSetting) {
        if (activeOperationFromDb.getOperationId() == activeOperationFromSetting.getOperationId()) {
          assertTrue(activeOperationFromSetting.equals(activeOperationFromDb));
        }
      }
    } // for
    for (Operation endOperationFromDb : endOperatonList) {
      for (Operation endOperationFromSetting : endOperatonListFromSetting) {
        if (endOperationFromDb.getOperationId() == endOperationFromSetting.getOperationId()) {
          assertTrue(endOperationFromSetting.equals(endOperationFromDb));
        }
      }
    } // for
  }

  @Test
  public void testSaveExistOperation() {
    int amountOfOperations = 10;
    int activeCount = 4;
    int endCount = amountOfOperations - activeCount;
    // build operations
    List<Operation> activeOperatonList = operationStore.getActiveOperation();
    activeOperatonList = InfocenterUtils.buildActiveOperations(activeCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");
    List<Operation> endOperatonList = operationStore.getEndOperation();
    endOperatonList = InfocenterUtils
        .buildEndOperations(endCount, TargetType.VOLUME, OperationType.CREATE,
            "TARGETVOLUME");

    // save to memory and db
    for (Operation operation : activeOperatonList) {
      operationStore.saveOperation(operation);
    }
    for (Operation operation : endOperatonList) {
      operationStore.saveOperation(operation);
    }

    // get from memory or db
    List<Operation> allOperatonList = operationStore.getAllOperation();

    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    endOperatonList = operationStore.getEndOperation();

    // assert numbers of operation will the same
    assertEquals(allOperatonList.size(), amountOfOperations);
    assertEquals(activeOperatonList.size(), activeCount);
    assertEquals(endOperatonList.size(), endCount);

    // save again
    for (Operation operation : activeOperatonList) {
      operationStore.saveOperation(operation);
    }
    for (Operation operation : endOperatonList) {
      operationStore.saveOperation(operation);
    }

    // assert numbers of operation will the same
    assertEquals(allOperatonList.size(), amountOfOperations);
    assertEquals(activeOperatonList.size(), activeCount);
    assertEquals(endOperatonList.size(), endCount);
  }

  @Test
  public void testActiveToEndOperation() {

    int amountOfOperations = 10;
    int activeCount = 4;
    int endCount = amountOfOperations - activeCount;

    // build operations
    List<Operation> activeOperatonList = new ArrayList<>();

    activeOperatonList = InfocenterUtils.buildActiveOperations(activeCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");
    List<Operation> endOperatonList = new ArrayList<>();

    endOperatonList = InfocenterUtils
        .buildEndOperations(endCount, TargetType.VOLUME, OperationType.CREATE,
            "TARGETVOLUME");

    // save to memory and db
    for (Operation operation : activeOperatonList) {
      operationStore.saveOperation(operation);
    }
    for (Operation operation : endOperatonList) {
      operationStore.saveOperation(operation);
    }

    // get form memory or db
    List<Operation> allOperatonListFromSetting = operationStore.getAllOperation();
    List<Operation> activeOperatonListFromSetting = operationStore.getActiveOperation();
    List<Operation> endOperatonListFromSetting = operationStore.getEndOperation();

    // assert number of list will unchange
    assertEquals(allOperatonListFromSetting.size(), amountOfOperations);
    assertEquals(activeOperatonListFromSetting.size(), activeCount);
    assertEquals(endOperatonListFromSetting.size(), endCount);

    // change some active operation to end operation
    // operation-list from setting will do the same operation, so it will be same with memory
    int accoutActiveToEnd = 2; // the number of operations that from active to end

    for (int i = 0; i < accoutActiveToEnd; i++) {
      Operation newOperation = InfocenterUtils.buildOperation(activeOperatonListFromSetting.get(0));
      newOperation.setStatus(OperationStatus.FAILED);
      activeOperatonListFromSetting
          .remove(0); // every time remove(0),because after remove(0),the second
      // list-element will move to the first
      endOperatonListFromSetting.add(newOperation);
      operationStore.saveOperation(newOperation);
    }
    // load from memory
    List<Operation> allOperatonList = new ArrayList<>();

    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    endOperatonList = operationStore.getEndOperation();

    // assert the number of active-opertion will deduce accoutActiveToEnd, while the number of 
    // end-operation will
    // increase accoutActiveToEnd
    assertEquals(allOperatonList.size(), amountOfOperations);
    assertEquals(activeOperatonList.size(), activeCount - accoutActiveToEnd);
    assertEquals(activeOperatonListFromSetting.size(), activeCount - accoutActiveToEnd);
    assertEquals(endOperatonList.size(), endCount + accoutActiveToEnd);
    assertEquals(endOperatonListFromSetting.size(), endCount + accoutActiveToEnd);

    // assert list form setting will the same with memory
    for (Operation activeOperationFromDb : activeOperatonList) {
      boolean find = false;
      for (Operation activeOperationFromSetting : activeOperatonListFromSetting) {
        if (activeOperationFromDb.getOperationId() == activeOperationFromSetting.getOperationId()) {
          find = true;
          assertTrue(activeOperationFromSetting.equals(activeOperationFromDb));
        }
      }
      assertTrue(find);
    }
    for (Operation endOperationFromDb : endOperatonList) {
      boolean find = false;
      for (Operation endOperationFromSetting : endOperatonListFromSetting) {
        if (endOperationFromDb.getOperationId() == endOperationFromSetting.getOperationId()) {
          find = true;
          assertTrue(endOperationFromSetting.equals(endOperationFromDb));
        }
      }
      assertTrue(find);
    }
  }

  @Test
  public void testGetOperation() {

    int amountOfOperations = 10;
    int activeCount = 4;
    int endCount = amountOfOperations - activeCount;

    // build operations
    List<Operation> activeOperatonList = new ArrayList<>();

    activeOperatonList = InfocenterUtils.buildActiveOperations(activeCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");
    List<Operation> endOperatonList = new ArrayList<>();
    endOperatonList = InfocenterUtils
        .buildEndOperations(endCount, TargetType.VOLUME, OperationType.CREATE,
            "TARGETVOLUME");

    // save to memory and db
    for (Operation operation : activeOperatonList) {
      operationStore.saveOperation(operation);
    }
    for (Operation operation : endOperatonList) {
      operationStore.saveOperation(operation);
    }

    // load from memory or db, the number of list-operation will unchange
    List<Operation> allOperatonList = new ArrayList<>();
    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    endOperatonList = operationStore.getEndOperation();
    assertEquals(allOperatonList.size(), amountOfOperations);
    assertEquals(activeOperatonList.size(), activeCount);
    assertEquals(endOperatonList.size(), endCount);

    // get operation from db or memory, it will unchange
    for (Operation operation : allOperatonList) {
      Long operationId = operation.getOperationId();
      Operation operationGetFromDb = operationStore.getOperation(operationId);
      assertTrue(operation.equals(operationGetFromDb));
    }
  }

  @Test
  public void testDeleteOperation() {

    int amountOfOperations = 10;
    int activeCount = 4;
    int endCount = amountOfOperations - activeCount;

    // build operations
    List<Operation> activeOperatonListFromSetting = new ArrayList<>();

    activeOperatonListFromSetting = InfocenterUtils
        .buildActiveOperations(activeCount, TargetType.VOLUME,
            OperationType.CREATE, "TARGETVOLUME");
    List<Operation> endOperatonListFromSetting = new ArrayList<>();

    endOperatonListFromSetting = InfocenterUtils.buildEndOperations(endCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");

    // save to memory and db, and union active-operation and end-operation to all-operation from 
    // setting
    List<Operation> allOperatonListFromSetting = new ArrayList<>();

    for (Operation operation : activeOperatonListFromSetting) {
      operationStore.saveOperation(operation);
      allOperatonListFromSetting.add(operation);
    }
    for (Operation operation : endOperatonListFromSetting) {
      operationStore.saveOperation(operation);
      allOperatonListFromSetting.add(operation);
    }

    // load from memory or db
    List<Operation> allOperatonList = new ArrayList<>();
    List<Operation> activeOperatonList = new ArrayList<>();

    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    List<Operation> endOperatonList = new ArrayList<>();

    endOperatonList = operationStore.getEndOperation();

    // assert the number of active-operation will deduce to 0 and end-operation will decrease
    // acountDeleteOperations;
    assertEquals(allOperatonList.size(), allOperatonListFromSetting.size());
    assertEquals(allOperatonList.size(), amountOfOperations);
    assertEquals(activeOperatonList.size(), activeCount);
    assertEquals(endOperatonList.size(), endCount);

    // delete all active-operation and some end-active operation from db or memory
    // operation-list from setting will do the same operation, so it will be same with memory
    for (Operation operation : activeOperatonListFromSetting) {
      Long operationId = operation.getOperationId();
      operationStore.deleteOperation(operationId);
      allOperatonListFromSetting.remove(operation);
    }
    int acountDeleteOperations = 3; // the number of end-operation that will be delete

    for (int i = 0; i < acountDeleteOperations; i++) {
      operationStore.deleteOperation(endOperatonListFromSetting.get(0).getOperationId());
      endOperatonListFromSetting
          .remove(0); // every time we remove(0),because when we remove(0),the second
      // list-element will move to the first
      allOperatonListFromSetting.remove(endOperatonListFromSetting.get(0));
    }

    // load from memory or db
    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    endOperatonList = operationStore.getEndOperation();

    // assert the number of active-operation will deduce to 0 and end-operation will decrease
    // acountDeleteOperations;
    assertEquals(allOperatonList.size(), allOperatonListFromSetting.size());
    assertEquals(activeOperatonList.size(), 0);
    assertEquals(endOperatonList.size(), endCount - acountDeleteOperations);

    // assert the end-operation will be the same in memory with setting
    for (Operation endOperationFromDb : endOperatonList) {
      boolean find = false;
      for (Operation endOperationFromSetting : endOperatonListFromSetting) {
        if (endOperationFromDb.getOperationId() == endOperationFromSetting.getOperationId()) {
          find = true;
          assertTrue(endOperationFromSetting.equals(endOperationFromDb));
        }
      }
      assertTrue(find);
    }
  }

  @Test
  public void testDeleteOldOperation() {
    int days = 30; // the end-operation will keep "days" day
    operationStore.setSaveOperationDays(days);
    List<Operation> endOperatonListFromSetting = new ArrayList<>();

    int endCount = 10;
    int oldEndCount = 6;

    // build end-operations
    endOperatonListFromSetting = InfocenterUtils.buildEndOperations(endCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");

    // save to memory and db
    for (Operation operation : endOperatonListFromSetting) {
      operationStore.saveOperation(operation);
    }

    // set old-end-operation pass "days" day
    // the end-operation from setting will do the same operaton, so it will same to memroy
    for (int i = 0; i < oldEndCount; i++) {
      Operation operation = endOperatonListFromSetting.get(0);
      endOperatonListFromSetting.remove(0);
      long startTime = operation.getStartTime();
      long endTimeLong = startTime + days * dayToMillsecond + 500;
      operation.setEndTime(endTimeLong);
      operationStore.saveOperation(operation);
    }

    // delete old-end-operation pass "days" day
    operationStore.deleteOldOperations();

    // load memory or db
    List<Operation> endOperatonList = new ArrayList<>();

    endOperatonList = operationStore.getEndOperation();

    // assert end-operatonList will decrease oldEndCount
    assertEquals(endOperatonList.size(), endCount - oldEndCount);
    assertEquals(endOperatonListFromSetting.size(), endCount - oldEndCount);

    // assert the end-operation will be the same in memory with setting
    for (Operation endOperationFromDb : endOperatonList) {
      boolean find = false;
      for (Operation endOperationFromSetting : endOperatonListFromSetting) {
        if (endOperationFromDb.getOperationId() == endOperationFromSetting.getOperationId()) {
          find = true;
          assertTrue(endOperationFromSetting.equals(endOperationFromDb));
        }
      }
      assertTrue(find);
    }
  }

  @Test
  public void testClearMemory() {

    int amountOfOperations = 10;
    int activeCount = 4;
    int endCount = amountOfOperations - activeCount;

    // build operations
    List<Operation> activeOperatonList = new ArrayList<>();

    activeOperatonList = InfocenterUtils.buildActiveOperations(activeCount, TargetType.VOLUME,
        OperationType.CREATE, "TARGETVOLUME");
    List<Operation> endOperatonList = new ArrayList<>();

    endOperatonList = InfocenterUtils
        .buildEndOperations(endCount, TargetType.VOLUME, OperationType.CREATE,
            "TARGETVOLUME");

    // save to memory and db
    for (Operation operation : activeOperatonList) {
      operationStore.saveOperation(operation);
    }
    for (Operation operation : endOperatonList) {
      operationStore.saveOperation(operation);
    }

    // clear DB and memory
    operationStore.clearMemory();
    dbStore.clearDb();

    // load from db
    dbStore.reloadAllOperationsFromDb();
    List<Operation> allOperatonList = new ArrayList<>();

    allOperatonList = operationStore.getAllOperation();
    activeOperatonList = operationStore.getActiveOperation();
    endOperatonList = operationStore.getEndOperation();

    // assert empty
    assertEquals(allOperatonList.size(), 0);
    assertEquals(activeOperatonList.size(), 0);
    assertEquals(endOperatonList.size(), 0);
  }


  /**
   * volumeJobStoreDb  begin.
   */
  @Test
  public void testcreatevolumerequestsdbstore() {
    VolumeCreationRequest volumeCreationRequest = new VolumeCreationRequest(1, 1,
        VolumeType.REGULAR, 1, false);
    volumeCreationRequest.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);

    VolumeCreationRequest volumeCreationRequest2 = new VolumeCreationRequest(2, 1,
        VolumeType.REGULAR, 1, false);
    volumeCreationRequest2.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);

    VolumeCreationRequest volumeCreationRequest3 = new VolumeCreationRequest(3, 1,
        VolumeType.REGULAR, 1, false);
    volumeCreationRequest3.setRequestType(VolumeCreationRequest.RequestType.CREATE_VOLUME);

    try {
      //save
      volumeJobStoreDb.saveCreateOrExtendVolumeRequest(volumeCreationRequest);

      //get and check
      List<VolumeCreationRequest> volumeCreationRequestList = volumeJobStoreDb
          .getCreateOrExtendVolumeRequest();
      assertEquals(1, volumeCreationRequestList.size());
      VolumeCreationRequest creationRequest = volumeCreationRequestList.get(0);
      CreateSegmentUnitRequest segmentUnitRequest = new CreateSegmentUnitRequest();

      //save 2 and 3
      volumeJobStoreDb.saveCreateOrExtendVolumeRequest(volumeCreationRequest2);
      volumeJobStoreDb.saveCreateOrExtendVolumeRequest(volumeCreationRequest3);

      //get,only get two, there is three in db
      volumeCreationRequestList = volumeJobStoreDb.getCreateOrExtendVolumeRequest();
      assertEquals(2, volumeCreationRequestList.size());

      //delete all
      volumeJobStoreDb.deleteCreateOrExtendVolumeRequest(volumeCreationRequest);
      volumeJobStoreDb.deleteCreateOrExtendVolumeRequest(volumeCreationRequest2);
      volumeJobStoreDb.deleteCreateOrExtendVolumeRequest(volumeCreationRequest3);

      //get to check
      volumeCreationRequestList = volumeJobStoreDb.getCreateOrExtendVolumeRequest();
      assertEquals(0, volumeCreationRequestList.size());

    } catch (Exception e) {
      logger.error("Caught an exception", e);
      fail();
    }
  }

  @Test
  public void testdeletevolumerequestdbstore() {
    try {
      DeleteVolumeRequest deleteVolumeRequest = new DeleteVolumeRequest(1, "old",
          111111, 2, "new");
      //save
      volumeJobStoreDb.saveDeleteVolumeRequest(deleteVolumeRequest);

      //get
      List<DeleteVolumeRequest> deleteVolumeRequestList = volumeJobStoreDb.getDeleteVolumeRequest();
      assertEquals(1, deleteVolumeRequestList.size());
      DeleteVolumeRequest volumeRequest = deleteVolumeRequestList.get(0);
      assertEquals("new", volumeRequest.getNewVolumeName());

      //delete
      volumeJobStoreDb.deleteDeleteVolumeRequest(deleteVolumeRequest);

      //get to check
      deleteVolumeRequestList = volumeJobStoreDb.getDeleteVolumeRequest();
      Assert.assertTrue(deleteVolumeRequestList.isEmpty());

    } catch (Exception e) {
      logger.error("Caught an exception", e);
      fail();
    }
  }


  @After
  public void clean() throws Exception {
    // clear DB and memory
    operationStore.clearMemory();
    dbStore.clearDb();
  }

}
