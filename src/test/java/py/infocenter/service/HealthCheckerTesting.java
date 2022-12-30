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

package py.infocenter.service;

import static org.junit.Assert.assertNull;

import java.lang.reflect.Method;
import org.junit.Test;
import py.app.context.AppContextImpl;
import py.app.thrift.ThriftAppEngine;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.instance.PortType;
import py.test.TestBase;


public class HealthCheckerTesting extends TestBase {

  public void init() throws Exception {
    super.init();
  }

  @Test
  public void checkService() {
    Exception exception = null;
    ThriftAppEngine engine = null;
    try {
      AppContextImpl appContext = new AppContextImpl("InfoCenter");
      appContext.putEndPoint(PortType.CONTROL, new EndPoint(null, 45447));
      InformationCenterImpl serviceImpl = new InformationCenterImpl();
      engine = new ThriftAppEngine(serviceImpl);
      engine.setContext(appContext);
      engine.setHealthChecker(null);
      engine.start();

      // get heart beat factory.we'll use this factory to create a heart beat worker,which is a 
      // periodic thread
      // object.
      GenericThriftClientFactory<?> genericThriftClientFactory = GenericThriftClientFactory
          .create(py.thrift.infocenter.service.InformationCenter.Iface.class);

      for (EndPoint endPoint : appContext.getEndPoints().values()) {
        Object serviceClient = genericThriftClientFactory.generateSyncClient(endPoint);
        Method method = serviceClient.getClass().getMethod("ping");
        Object obj = method.invoke(serviceClient);
      }

    } catch (Exception e) {
      logger.warn("caught an exception", e);
      exception = e;
    } finally {
      if (engine != null) {
        engine.stop();
      }
    }

    assertNull(exception);
  }
}
