
package py.infocenter.rebalance.old;


import py.infocenter.rebalance.old.processor.BasicRebalanceTaskContext;


public interface RebalanceTaskContextFactory {

  public BasicRebalanceTaskContext generateContext(RebalanceTaskExecutionResult executionResult);
}
