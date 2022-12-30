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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import py.common.struct.EndPoint;
import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.infocenter.service.selection.BalancedDriverContainerSelectionStrategy;
import py.infocenter.service.selection.DriverContainerSelectionStrategy;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.PortType;


public class DriverContainerBalanceTest {

  private DriverContainerSelectionStrategy driverContainerSelectionStrategy;

  @Before
  public void init() {
    driverContainerSelectionStrategy = new BalancedDriverContainerSelectionStrategy();
  }

  @Test
  public void testDriverContainerSelectionWhenOneDriverContainer() {
    List<Instance> toSelectInstances = new ArrayList<Instance>();
    Instance toSelectInstance = new Instance(new InstanceId(1), null, "DriverContainer",
        InstanceStatus.HEALTHY);
    toSelectInstance
        .putEndPointByServiceName(PortType.CONTROL, EndPoint.fromString("10.0.1.112:9000"));
    toSelectInstance.putEndPointByServiceName(PortType.IO, EndPoint.fromString("100.0.1.112:9000"));
    toSelectInstances.add(toSelectInstance);

    List<DriverMetadata> drivers = new ArrayList<DriverMetadata>();
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("100.0.1.112");
    driverMetadata.setPort(9000);
    drivers.add(driverMetadata);

    List<DriverContainerCandidate> expectInstances = driverContainerSelectionStrategy
        .select(toSelectInstances,
            null);

    Assert.assertEquals(expectInstances.size(), 1);

    DriverContainerCandidate driverContainerCandidate = expectInstances.get(0);

    Assert.assertEquals("10.0.1.112", driverContainerCandidate.getHostName());

    expectInstances = driverContainerSelectionStrategy.select(toSelectInstances, drivers);

    Assert.assertEquals(expectInstances.size(), 1);

    driverContainerCandidate = expectInstances.get(0);

    Assert.assertEquals("10.0.1.112", driverContainerCandidate.getHostName());
  }

  @Test
  public void testDriverContainerSelectionWhenMultipeDriverContainer() {

    final List<Instance> toSelectInstances = new ArrayList<Instance>();
    Instance toSelectInstance0 = new Instance(new InstanceId(1), null, "DriverContainer",
        InstanceStatus.HEALTHY);
    toSelectInstance0
        .putEndPointByServiceName(PortType.CONTROL, EndPoint.fromString("10.0.1.203:9000"));
    toSelectInstance0
        .putEndPointByServiceName(PortType.IO, EndPoint.fromString("100.0.1.203:9000"));

    Instance toSelectInstance1 = new Instance(new InstanceId(1), null, "DriverContainer",
        InstanceStatus.HEALTHY);
    toSelectInstance1
        .putEndPointByServiceName(PortType.CONTROL, EndPoint.fromString("10.0.1.204:9000"));
    toSelectInstance1
        .putEndPointByServiceName(PortType.IO, EndPoint.fromString("100.0.1.204:9000"));

    Instance toSelectInstance2 = new Instance(new InstanceId(1), null, "DriverContainer",
        InstanceStatus.HEALTHY);
    toSelectInstance2
        .putEndPointByServiceName(PortType.CONTROL, EndPoint.fromString("10.0.1.205:9000"));
    toSelectInstance2
        .putEndPointByServiceName(PortType.IO, EndPoint.fromString("100.0.1.205:9000"));

    toSelectInstances.add(toSelectInstance0);
    toSelectInstances.add(toSelectInstance1);
    toSelectInstances.add(toSelectInstance2);

    final List<DriverMetadata> drivers = new ArrayList<DriverMetadata>();
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setHostName("100.0.1.203");
    driverMetadata.setPort(9000);
    DriverMetadata driverMetadata1 = new DriverMetadata();
    driverMetadata1.setHostName("100.0.1.204");
    driverMetadata1.setPort(9000);

    drivers.add(driverMetadata);
    drivers.add(driverMetadata1);

    List<DriverContainerCandidate> expectInstances = driverContainerSelectionStrategy
        .select(toSelectInstances,
            drivers);
    System.out.println(expectInstances.get(0).getHostName());
    Assert.assertEquals(expectInstances.size(), 3);
    Assert.assertEquals(expectInstances.get(0).getHostName(),
        toSelectInstance2.getEndPointByServiceName(PortType.CONTROL).getHostName());
    Assert.assertEquals(expectInstances.get(1).getHostName(),
        toSelectInstance0.getEndPointByServiceName(PortType.CONTROL).getHostName());
  }

  @Ignore
  @Test
  public void testShuffleSet() {
    DriverContainerCandidate one = generate("10.0.1.1");
    DriverContainerCandidate two = generate("10.0.1.2");
    DriverContainerCandidate three = generate("10.0.1.3");
    DriverContainerCandidate four = generate("10.0.1.4");
    Set<DriverContainerCandidate> testSet = new HashSet<DriverContainerCandidate>();
    testSet.add(one);
    testSet.add(two);
    testSet.add(three);
    testSet.add(four);

    List<DriverContainerCandidate> testList = new ArrayList<>(testSet);
    Collections.shuffle(testList);

    int diffTime = 0;
    for (int i = 0; i < testSet.size(); i++) {
      if (!testList.get(i).equals(testSet.toArray()[i])) {
        diffTime++;
      }
    }
    assertTrue(diffTime > 0);
  }

  private DriverContainerCandidate generate(String hostname) {
    DriverContainerCandidate one = new DriverContainerCandidate();
    one.setHostName(hostname);
    return one;
  }
}
