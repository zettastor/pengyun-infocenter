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
