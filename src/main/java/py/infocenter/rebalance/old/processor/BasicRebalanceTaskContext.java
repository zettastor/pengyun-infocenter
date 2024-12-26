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
