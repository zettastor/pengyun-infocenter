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

package py.infocenter.rebalance.old.processor;

import py.app.context.AppContext;
import py.infocenter.rebalance.old.RebalanceTaskContext;
import py.rebalance.RebalanceTask;


public class BasicRebalanceTaskContext extends RebalanceTaskContext {

  private static final int MAX_FAILURES = 60;
  protected RebalanceTask rebalanceTask;
  private RebalancePhase phase;

  public BasicRebalanceTaskContext(long delay, AppContext appContext) {
    super(delay, appContext);
    this.phase = RebalancePhase.INIT;
  }

  public BasicRebalanceTaskContext(long delay, AppContext appContext, RebalancePhase phase) {
    super(delay, appContext);
    this.phase = phase;
  }

  public RebalancePhase getPhase() {
    return this.phase;
  }

  public RebalanceTask getRebalanceTask() {
    return rebalanceTask;
  }

  public void setRebalanceTask(RebalanceTask rebalanceTask) {
    this.rebalanceTask = rebalanceTask;
  }

  public boolean tooManyFailures() {
    return failureTimes > MAX_FAILURES;
  }

  @Override
  public String toString() {
    return getClass().getName() + " : [phase=" + phase + ", failureTimes=" + failureTimes
        + ", rebalanceTask="
        + rebalanceTask + "]";
  }

  
  public enum RebalancePhase {

    INIT(0), // no re-balance tasks got.
    TASK_GOTTEN(1), // task gotten, wait to be executed.
    REQUEST_SENT(2), // request has been sent to data node.
    TASK_DONE(3); // task has been done

    private int step;

    private RebalancePhase(int step) {
      this.step = step;
    }

    public int step() {
      return step;
    }

  }

}
