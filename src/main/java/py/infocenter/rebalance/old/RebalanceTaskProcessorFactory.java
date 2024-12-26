
package py.infocenter.rebalance.old;


import py.infocenter.rebalance.old.processor.BasicRebalanceTaskContext;


public interface RebalanceTaskProcessorFactory {

  public RebalanceTaskProcessor generateProcessor(BasicRebalanceTaskContext context);
}
