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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import py.icshare.InstanceMetadata;

/**
 * This strategy removes instances that don't have available space first and then sort the instances
 * based on available space and instance id. Frugal means it tries to use up as few instances as
 * possible. This strategy will be implemented by SQL if instances are persistent in a relational
 * DB.
 */
public class FrugalInstanceSelectionStrategy implements
    InstanceSelectionStrategy {

  @Override
  public List<InstanceMetadata> select(Collection<InstanceMetadata> instances) {
    List<InstanceMetadata> instancesHaveAvailSpace = new ArrayList<InstanceMetadata>(
        instances.size());
    for (InstanceMetadata instance : instances) {
      if (instance.getCurrentFreeSpace() > 0) {
        instancesHaveAvailSpace.add(instance);
      }
    }
    Collections.sort(instancesHaveAvailSpace, new Comparator<InstanceMetadata>() {

      @Override
      public int compare(InstanceMetadata o1, InstanceMetadata o2) {
        if (o1.getCurrentFreeSpace() > o2.getCurrentFreeSpace()) {
          return 1;
        } else if (o1.getCurrentFreeSpace() < o2.getCurrentFreeSpace()) {
          return -1;
        } else {
          // This could give us a consistent view of the instances
          if (o1.getInstanceId().getId() > o2.getInstanceId().getId()) {
            return 1;
          } else if (o1.getInstanceId().getId() < o2.getInstanceId().getId()) {
            return -1;
          } else {
            // This should not happen
            return 0;
          }
        }
      }
    });
    return instancesHaveAvailSpace;
  }

}
