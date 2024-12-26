/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.infocenter.test.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.icshare.Operation;
import py.icshare.OperationStatus;
import py.icshare.OperationType;
import py.icshare.TargetType;
import py.test.TestBase;


public class InfocenterUtils {

  //to prevent there exists no equal targetName
  private static long id = 0;

  
  public static Operation buildOperation(Long operationId, Long accountId, Long targetId,
      OperationType operationType,
      TargetType targetType, String targetName, OperationStatus operationStatus,
      Long targetSourceSize,
      List<EndPoint> endPointList, Long progress, Long startTime, Long endTime, String description,
      String errorMessage, Integer snapshotId) {
    Operation operation = new Operation();
    operation.setOperationId(operationId);
    operation.setStartTime(startTime);
    operation.setEndTime(endTime);
    operation.setProgress(progress);
    operation.setAccountId(accountId);
    operation.setTargetId(targetId);
    operation.setOperationType(operationType);
    operation.setTargetType(targetType);
    operation.setStatus(operationStatus);
    operation.setTargetName(targetName);
    operation.setTargetSourceSize(targetSourceSize);
    operation.setEndPointList(endPointList);
    operation.setDescription(description);
    operation.setErrorMessage(errorMessage);
    operation.setSnapshotId(snapshotId);
    return operation;
  }

  
  public static Operation buildOperation(TargetType targetType, OperationType operationType,
      OperationStatus status, String targetName) {
    Operation operation = new Operation();
    operation.setOperationId(RequestIdBuilder.get());
    operation.setTargetId(RequestIdBuilder.get());
    operation.setTargetType(targetType);
    Random r = new Random();
    operation.setProgress((long) r.nextInt(100));
    operation.setErrorMessage(TestBase.getRandomString(10));
    operation.setAccountId(RequestIdBuilder.get());
    operation.setDescription(TestBase.getRandomString(10));
    long time = System.currentTimeMillis();
    operation.setStartTime(time);
    operation.setEndTime(time);
    operation.setOperationType(operationType);
    operation.setStatus(status);
    operation.setTargetName(targetName);
    operation.setTargetSourceSize(RequestIdBuilder.get());
    operation.setEndPointList(null);
    operation.setSnapshotId(0);
    return operation;
  }

  
  public static Operation buildOperation(Operation operation) {
    Operation newOperation = new Operation();
    newOperation.setOperationId(operation.getOperationId());
    newOperation.setTargetId(operation.getTargetId());
    newOperation.setTargetType(operation.getTargetType());
    newOperation.setStartTime(operation.getStartTime());
    newOperation.setEndTime(operation.getEndTime());
    newOperation.setDescription(operation.getDescription());
    newOperation.setStatus(operation.getStatus());
    newOperation.setProgress(operation.getProgress());
    newOperation.setErrorMessage(operation.getErrorMessage());
    newOperation.setAccountId(operation.getAccountId());
    newOperation.setAccountName(operation.getAccountName());
    newOperation.setOperationType(operation.getOperationType());
    newOperation.setTargetSourceSize(operation.getTargetSourceSize());
    newOperation.setTargetName(operation.getTargetName());
    newOperation.setEndPointList(operation.getEndPointList());
    newOperation.setSnapshotId(operation.getSnapshotId());
    return newOperation;
  }

  public static Operation buildActiveOperation(TargetType targetType, OperationType operationType,
      String targetName) {
    id++;
    return buildOperation(targetType, operationType, OperationStatus.ACTIVITING, targetName + id);
  }

  
  public static Operation buildEndOperation(TargetType targetType, OperationType operationType,
      String targetName) {
    id++;
    return buildOperation(targetType, operationType, OperationStatus.SUCCESS, targetName + id);
  }

  
  public static List<Operation> buildActiveOperations(int count, TargetType targetType,
      OperationType operationType, String targetName) {
    List<Operation> operationList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      id++;
      Operation operation = buildActiveOperation(targetType, operationType, targetName + id);
      operationList.add(operation);
    }
    return operationList;
  }

  
  public static List<Operation> buildEndOperations(int count, TargetType targetType,
      OperationType operationType, String targetName) {
    List<Operation> operationList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      id++;
      Operation operation = buildEndOperation(targetType, operationType, targetName + id);
      operationList.add(operation);
    }
    return operationList;
  }
}
