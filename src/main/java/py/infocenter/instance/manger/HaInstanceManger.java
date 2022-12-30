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

package py.infocenter.instance.manger;

import java.util.List;
import java.util.Map;
import py.instance.Instance;

public interface HaInstanceManger {

  public void updateInstance();

  public Instance getMaster();

  public Instance getFollower(long instanceId);

  public List<Instance> getAllInstance();

  //just for test
  public void setMaterInstance(Instance materInstance);

  public void setFollowerInstanceMap(Map<Long, Instance> followerInstanceMap);

}
