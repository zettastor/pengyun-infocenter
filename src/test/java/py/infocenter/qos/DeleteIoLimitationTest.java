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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.driver.DriverMetadata;
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
import py.thrift.share.DeleteIoLimitationsRequest;
import py.thrift.share.DeleteIoLimitationsResponse;


public class DeleteIoLimitationTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DeleteIoLimitationTest.class);
  @Mock
  IoLimitationStore ioLimitationStore;
  private InformationCenterImpl informationCenter;
  private SessionFactory sessionFactory;
  @Mock
  private InfoCenterAppContext appContext;
  @Mock
  private DriverStore driverStore;
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

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for deleting rules with false commit field. In the case, no rules could be delete.
   */
  @Test
  public void testDeleteIoLimitations() throws Exception {
    DeleteIoLimitationsRequest request = new DeleteIoLimitationsRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(1111);
    request.addToRuleIds(2L);
    request.addToRuleIds(3L);
    request.setCommit(true);

    final List<DriverMetadata> drivers = new ArrayList<DriverMetadata>();
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("100.0.1.112");
    driverMetadata.setPort(9000);
    driverMetadata.setStaticIoLimitationId(2);
    driverMetadata.setStaticIoLimitationId(3);

    DriverMetadata driverMetadata2 = new DriverMetadata();
    driverMetadata2.setHostName("100.0.1.112");
    driverMetadata2.setPort(9001);
    driverMetadata.setStaticIoLimitationId(4);
    driverMetadata.setStaticIoLimitationId(5);

    drivers.add(driverMetadata);

    when(driverStore.list()).thenReturn(drivers);
    when(ioLimitationStore.get(eq(5L)))
        .thenReturn(buildIoLimitation(5L, IoLimitationStatus.DELETING));
    when(ioLimitationStore.get(eq(1L)))
        .thenReturn(buildIoLimitation(1L, IoLimitationStatus.AVAILABLE));
    when(ioLimitationStore.get(eq(2L)))
        .thenReturn(buildIoLimitation(2L, IoLimitationStatus.AVAILABLE));
    when(ioLimitationStore.get(eq(3L)))
        .thenReturn(buildIoLimitation(3L, IoLimitationStatus.AVAILABLE));
    when(ioLimitationStore.get(eq(4L)))
        .thenReturn(buildIoLimitation(4L, IoLimitationStatus.AVAILABLE));

    DeleteIoLimitationsResponse response = informationCenter.deleteIoLimitations(request);

    Mockito.verify(ioLimitationStore, Mockito.times(2)).delete(anyLong());
  }



  public IoLimitation buildIoLimitation(long ruleId, IoLimitationStatus status) {
    IoLimitation ioLimitation = new IoLimitation();
    ioLimitation.setId(ruleId);
    ioLimitation.setLimitType(IoLimitation.LimitType.Dynamic);
    ioLimitation.setStatus(status);
    ioLimitation.setName("name");

    return ioLimitation;
  }
}
