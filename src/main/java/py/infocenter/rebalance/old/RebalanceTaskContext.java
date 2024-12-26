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

package py.infocenter.rebalance.old;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import py.app.context.AppContext;
import py.instance.InstanceStatus;


public abstract class RebalanceTaskContext implements Delayed {

  // we need the appContext because we don't want to do anything while control center is not in 
  // OK status.
  private final AppContext appContext;
  protected int failureTimes = 0;
  private long delay;
  private long timeSettingDelay;

  public RebalanceTaskContext(long delay, AppContext appContext) {
    updateDelay(delay);
    this.appContext = appContext;
  }

  public boolean statusHealthy() {
    return (appContext.getStatus() == InstanceStatus.HEALTHY);
  }

  public AppContext getAppContext() {
    return this.appContext;
  }

  public void updateDelay(long newDelay) {
    delay = newDelay;
    timeSettingDelay = System.currentTimeMillis();
  }

  public long getExpireTime() {
    return delay + timeSettingDelay;
  }

  @Override
  public int compareTo(Delayed delayed) {
    if (delayed == null) {
      return 1;
    }

    if (delayed == this) {
      return 0;
    }

    long d = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
    return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(getExpireTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  public void incFailureTimes() {
    failureTimes++;
  }

  public abstract boolean tooManyFailures();
}
