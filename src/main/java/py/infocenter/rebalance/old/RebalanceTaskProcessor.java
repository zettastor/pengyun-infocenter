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

import java.util.concurrent.Callable;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.rebalance.old.processor.BasicRebalanceTaskContext;
import py.infocenter.service.InformationCenterImpl;
import py.rebalance.RebalanceTask;
import py.thrift.share.ServiceHavingBeenShutdownThrift;


public abstract class RebalanceTaskProcessor implements Callable<RebalanceTaskExecutionResult> {

  private static final Logger logger = LoggerFactory.getLogger(RebalanceTaskProcessor.class);
  protected final InformationCenterImpl informationCenter;
  private final BasicRebalanceTaskContext context;

  public RebalanceTaskProcessor(BasicRebalanceTaskContext context,
      InformationCenterImpl informationCenter) {
    this.context = context;
    this.informationCenter = informationCenter;
  }

  @Override
  public abstract RebalanceTaskExecutionResult call(); // no exceptions should be thrown here.

  public RebalanceTaskContext getContext() {
    return this.context;
  }

  protected RebalanceTaskExecutionResult stayHere(boolean success) {
    RebalanceTaskExecutionResult result = new RebalanceTaskExecutionResult(context);
    result.setDone(false);
    if (!success) {
      context.incFailureTimes();
    }
    return result;
  }

  protected RebalanceTaskExecutionResult nextPhase() {
    RebalanceTaskExecutionResult result = new RebalanceTaskExecutionResult(context);
    result.setDone(true);
    return result;
  }

  protected RebalanceTaskExecutionResult discardMyself() {

    RebalanceTask task = context.getRebalanceTask();
    if (task != null) {
      try {
        informationCenter.discardRebalanceTask(task.getTaskId());
      } catch (ServiceHavingBeenShutdownThrift serviceHavingBeenShutdownThrift) {
        logger.warn("infocenter is being shutdown");
      } catch (TException e) {
        logger.warn("caught an mystery exception", e);
        return stayHere(false);
      }
    }

    RebalanceTaskExecutionResult result = new RebalanceTaskExecutionResult(context);
    result.setDiscard(true);
    return result;
  }

}
