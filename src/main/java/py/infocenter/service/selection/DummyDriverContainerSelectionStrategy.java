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

package py.infocenter.service.selection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import py.common.struct.EndPoint;
import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.instance.Instance;
import py.instance.PortType;

/**
 * select three driver containers as candidates for control center to launch a volume selecting
 * strategy based on balance of driver containers.
 */
public class DummyDriverContainerSelectionStrategy implements DriverContainerSelectionStrategy {

  @Override
  public List<DriverContainerCandidate> select(Collection<Instance> instances,
      List<DriverMetadata> drivers) {
    // to do later
    // select driver container based on driver container balance
    List<DriverContainerCandidate> candidates = new ArrayList<DriverContainerCandidate>();
    int flag = 0;
    for (Instance instance : instances) {
      if (flag > 2) {
        break;
      }
      DriverContainerCandidate candidate = new DriverContainerCandidate();
      EndPoint ep = instance.getEndPointByServiceName(PortType.CONTROL);
      candidate.setHostName(ep.getHostName());
      candidate.setPort(ep.getPort());
      candidates.add(candidate);
      flag++;
    }
    return candidates;
  }

}
