

package py.infocenter.rebalance.old.processor;

import py.app.context.AppContext;
import py.rebalance.RebalanceTask;


public class InnerMigrateSegmentUnitContext extends BasicRebalanceTaskContext {

  private boolean startedMigrating;

  public InnerMigrateSegmentUnitContext(long delay, RebalancePhase phase,
      RebalanceTask rebalanceTask,
      AppContext appContext) {
    super(delay, appContext, phase);
    this.rebalanceTask = rebalanceTask;
  }

  @Override
  public String toString() {
    return getClass().getName() + "[ super=" + super.toString() + "]";
  }

  public boolean isStartedMigrating() {
    return startedMigrating;
  }

  public void setStartedMigrating(boolean startedMigrating) {
    this.startedMigrating = startedMigrating;
  }
}
