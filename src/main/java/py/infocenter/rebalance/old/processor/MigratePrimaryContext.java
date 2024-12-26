

package py.infocenter.rebalance.old.processor;

import py.app.context.AppContext;
import py.rebalance.RebalanceTask;


public class MigratePrimaryContext extends BasicRebalanceTaskContext {

  public MigratePrimaryContext(long delay, RebalancePhase phase, RebalanceTask rebalanceTask,
      AppContext appContext) {
    super(delay, appContext, phase);
    this.rebalanceTask = rebalanceTask;
  }

  @Override
  public String toString() {
    return getClass().getName() + "[ super=" + super.toString() + "]";
  }
}
