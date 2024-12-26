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

package py.infocenter.qos;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.service.InformationCenterImpl;
import py.infocenter.store.DriverStore;
import py.infocenter.store.control.OperationStore;
import py.instance.InstanceStatus;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationStatus;
import py.io.qos.IoLimitationStore;
import py.test.TestBase;
import py.thrift.share.ApplyIoLimitationsRequest;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.GetIoLimitationAppliedDriversRequest;
import py.thrift.share.GetIoLimitationAppliedDriversResponse;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.IoLimitationsNotExists;


public class ApplyIoLimitationTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ApplyIoLimitationTest.class);

  private InformationCenterImpl informationCenter;

  @Mock
  private IoLimitationStore ioLimitationStore;

  @Mock
  private DriverStore driverStore;

  @Mock
  private InfoCenterAppContext appContext;

  @Mock
  private PySecurityManager securityManager;

  @Mock
  private OperationStore operationStore;



  @Before
  public void init() throws Exception {
    super.init();
    informationCenter = new InformationCenterImpl();
    informationCenter.setIoLimitationStore(ioLimitationStore);
    informationCenter.setDriverStore(driverStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setOperationStore(operationStore);
    List<IoLimitation> ioLimitationList = new ArrayList<IoLimitation>();
    when(ioLimitationStore.list()).thenReturn(ioLimitationList);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    final List<DriverMetadata> driverMetadataList = new ArrayList<>();
    DriverMetadata driver1 = new DriverMetadata();
    driver1.setDriverContainerId(0L);
    driver1.setSnapshotId(0);
    driver1.setDriverType(DriverType.ISCSI);
    driver1.setVolumeId(0L);
    driver1.setDriverStatus(DriverStatus.LAUNCHED);
    DriverMetadata driver2 = new DriverMetadata();
    driver2.setDriverContainerId(0L);
    driver2.setSnapshotId(0);
    driver2.setDriverType(DriverType.ISCSI);
    driver2.setVolumeId(1L);
    driver2.setDynamicIoLimitationId(1L);
    driver2.setDriverStatus(DriverStatus.LAUNCHED);
    driverMetadataList.add(driver1);
    driverMetadataList.add(driver2);

    when(driverStore.get(0L, 0L, DriverType.ISCSI, 0)).thenReturn(driver1);
    when(driverStore.get(0L, 1L, DriverType.ISCSI, 0)).thenReturn(driver2);
    when(driverStore.list()).thenReturn(driverMetadataList);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);

  }

  /**
   * A test for Apply IoLimitation. In the case, new rules id should save to related driver db.
   */
  @Test
  public void testApplyIoLimitation() throws InvalidInputExceptionThrift,
      TException {

    ApplyIoLimitationsRequest request = new ApplyIoLimitationsRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setDriverKeys(buildThriftDriverKeys());
    request.setRuleId(1L);
    request.setCommit(true);

    when(ioLimitationStore.get(eq(1L)))
        .thenReturn(buildIoLimitation(1L, IoLimitationStatus.AVAILABLE));
    informationCenter.applyIoLimitations(request);

    Mockito.verify(driverStore, Mockito.times(1)).save(any(DriverMetadata.class));
  }

  /**
   * A test for drivers which get Applied IoLimitation  . In the case, new rules id should save to
   * related driver db.
   */
  @Test
  public void testGetAppliedIoLimitation() throws Exception {
    List<DriverKeyThrift> driverKeys = buildThriftDriverKeys();
    GetIoLimitationAppliedDriversRequest request = new GetIoLimitationAppliedDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setRuleId(1L);
    when(ioLimitationStore.get(eq(1L)))
        .thenReturn(buildIoLimitation(1L, IoLimitationStatus.AVAILABLE));
    GetIoLimitationAppliedDriversResponse response = informationCenter
        .getIoLimitationAppliedDrivers(request);
    assert (response.getDriverList().size() == 1);
  }


  /**
   * A test for drivers which get Applied IoLimitation but IoLimitation not found .
   */
  @Test
  public void testGetAppliedIoLimitationError() throws Exception {
    List<DriverKeyThrift> driverKeys = buildThriftDriverKeys();
    GetIoLimitationAppliedDriversRequest request = new GetIoLimitationAppliedDriversRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setRuleId(1L);

    boolean isException = false;
    try {
      GetIoLimitationAppliedDriversResponse response = informationCenter
          .getIoLimitationAppliedDrivers(request);
    } catch (IoLimitationsNotExists e) {
      isException = true;
    }
    assert (isException == true);
  }



  public IoLimitation buildIoLimitation(long ruleId, IoLimitationStatus status) {
    IoLimitation ioLimitation = new IoLimitation();
    ioLimitation.setId(ruleId);
    ioLimitation.setLimitType(IoLimitation.LimitType.Dynamic);
    ioLimitation.setStatus(status);
    ioLimitation.setName("name");

    return ioLimitation;
  }

  List<DriverKeyThrift> buildThriftDriverKeys() {
    List<DriverKeyThrift> driverList = new ArrayList<>();
    DriverKeyThrift driver = new DriverKeyThrift(0L, 0L, 0, DriverTypeThrift.ISCSI);
    driverList.add(driver);
    return driverList;
  }
}
