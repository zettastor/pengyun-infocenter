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
