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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.Utils;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;

public class HaInstanceMangerWithZk implements HaInstanceManger {

  private static final Logger logger = LoggerFactory.getLogger(HaInstanceMangerWithZk.class);
  private InstanceStore instanceStore;

  private Instance materInstance = null;
  private Map<Long, Instance> followerInstanceMap;


  public HaInstanceMangerWithZk(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
    this.followerInstanceMap = new ConcurrentHashMap<>();
    updateInstance();
  }

  @Override
  public void updateInstance() {
    followerInstanceMap.clear();
    materInstance = null;

    Set<Instance> instanceFlowers = Utils.getAllSuspendInfoCenter(instanceStore);

    Set<Instance> instanceMaster = instanceStore
        .getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY);
    Validate.isTrue(instanceMaster.isEmpty() || instanceMaster.size() == 1, "the master only one");

    for (Instance instance : instanceMaster) {
      materInstance = instance;
    }

    for (Instance instance : instanceFlowers) {
      followerInstanceMap.put(instance.getId().getId(), instance);
    }

    logger.warn("get all the HA instance, the master :{}, the follower :{}", materInstance,
        followerInstanceMap);
  }

  @Override
  public Instance getMaster() {
    if (materInstance == null) {
      updateInstance();
    }
    return materInstance;
  }

  @Override
  public Instance getFollower(long instanceId) {
    if (followerInstanceMap.containsKey(instanceId)) {
      return followerInstanceMap.get(instanceId);
    } else {
      updateInstance();
      return followerInstanceMap.get(instanceId);
    }
  }

  @Override
  public List<Instance> getAllInstance() {
    List<Instance> allInstances = new ArrayList<>();

    if (!followerInstanceMap.isEmpty()) {
      allInstances.addAll(followerInstanceMap.values());
    }

    if (materInstance != null) {
      allInstances.add(materInstance);
    }

    return allInstances;
  }

  //just for test
  public void setMaterInstance(Instance materInstance) {
    this.materInstance = materInstance;
  }

  public void setFollowerInstanceMap(Map<Long, Instance> followerInstanceMap) {
    this.followerInstanceMap = followerInstanceMap;
  }
}
