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
import py.thrift.share.CancelIoLimitationsRequest;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.IoLimitationEntryThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.LimitTypeThrift;


public class CancelIoLimitationTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(CancelIoLimitationTest.class);

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

    DriverMetadata driver1 = new DriverMetadata();
    driver1.setDriverContainerId(0L);
    driver1.setSnapshotId(0);
    driver1.setDriverType(DriverType.ISCSI);
    driver1.setVolumeId(0L);
    driver1.setDynamicIoLimitationId(1L);

    DriverMetadata driver2 = new DriverMetadata();
    driver2.setDriverContainerId(0L);
    driver2.setSnapshotId(0);
    driver2.setDriverType(DriverType.ISCSI);
    driver2.setVolumeId(1L);
    driver2.setDynamicIoLimitationId(1L);

    when(driverStore.get(0L, 0L, DriverType.ISCSI, 0)).thenReturn(driver1);
    when(driverStore.get(0L, 1L, DriverType.ISCSI, 0)).thenReturn(driver2);
    when(driverStore.list()).thenReturn(buildDriverMetadatas());

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);

  }

  /**
   * A test for cancel IoLimitation. In the case, rule id should update to default to driver db.
   */
  @Test
  public void testCancelIoLimitation() throws InvalidInputExceptionThrift,
      TException {
    IoLimitationThrift ioLimitationFromRemote = new IoLimitationThrift();
    ioLimitationFromRemote.setLimitationId(100);
    ioLimitationFromRemote.setLimitationName("rule1");
    ioLimitationFromRemote.setLimitType(LimitTypeThrift.Dynamic);
    ioLimitationFromRemote.setEntries(new ArrayList<IoLimitationEntryThrift>());
    CancelIoLimitationsRequest request = new CancelIoLimitationsRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setDriverKeys(buildThriftDriverKeys());
    request.setRuleId(1L);
    request.setCommit(true);

    when(ioLimitationStore.get(eq(1L)))
        .thenReturn(buildIoLimitation(1L, IoLimitationStatus.AVAILABLE));
    informationCenter.cancelIoLimitations(request);
    Mockito.verify(driverStore, Mockito.times(1)).save(any(DriverMetadata.class));
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

  List<DriverMetadata> buildDriverMetadatas() {
    final List<DriverMetadata> driverMetadatas = new ArrayList<DriverMetadata>();
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("100.0.1.203");
    driverMetadata.setPort(9000);
    driverMetadata.setStaticIoLimitationId(2L);
    driverMetadata.setDynamicIoLimitationId(1L);

    DriverMetadata driverMetadata1 = new DriverMetadata();
    driverMetadata1.setHostName("100.0.1.204");
    driverMetadata1.setPort(9000);
    driverMetadata.setStaticIoLimitationId(2L);
    driverMetadata.setDynamicIoLimitationId(1L);
    driverMetadatas.add(driverMetadata);
    driverMetadatas.add(driverMetadata1);
    return driverMetadatas;
  }
}
