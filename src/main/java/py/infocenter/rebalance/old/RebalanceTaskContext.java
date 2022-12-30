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
