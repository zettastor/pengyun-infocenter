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
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.IoLimitationEntryThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.LimitTypeThrift;
import py.thrift.share.ListIoLimitationsRequest;
import py.thrift.share.UpdateIoLimitationRulesRequest;


public class UpdateIoLimitationTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(UpdateIoLimitationTest.class);

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

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  /**
   * A test for update IoLimitation. In the case, new rules should save to db.
   */
  @Test
  public void testUpdateIoLimitationTest() throws InvalidInputExceptionThrift,
      TException {
    IoLimitationThrift ioLimitationFromRemote = new IoLimitationThrift();
    ioLimitationFromRemote.setLimitationId(100);
    ioLimitationFromRemote.setLimitationName("rule1");
    ioLimitationFromRemote.setLimitType(LimitTypeThrift.Dynamic);
    ioLimitationFromRemote.setEntries(new ArrayList<IoLimitationEntryThrift>());
    UpdateIoLimitationRulesRequest request = new UpdateIoLimitationRulesRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);
    request.setIoLimitation(ioLimitationFromRemote);

    when(ioLimitationStore.get(eq(100L)))
        .thenReturn(buildIoLimitation(100L, IoLimitationStatus.AVAILABLE));
    informationCenter.updateIoLimitations(request);
    Mockito.verify(ioLimitationStore, Mockito.times(1)).update(any(IoLimitation.class));
  }


  /**
   * A test for list IoLimitation. In the case, new rules should save to db.
   */
  @Test
  public void testListIoLimitationTest() throws InvalidInputExceptionThrift, TException {
    ListIoLimitationsRequest request = new ListIoLimitationsRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setAccountId(100000);

    informationCenter.listIoLimitations(request);
    Mockito.verify(ioLimitationStore, Mockito.times(1)).list();
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
