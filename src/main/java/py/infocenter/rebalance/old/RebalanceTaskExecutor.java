
package py.infocenter.rebalance.old;


public interface RebalanceTaskExecutor {

  public void start();

  public void pause();

  public boolean started();

  public void shutdown();

}
