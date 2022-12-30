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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.rebalance.old.RebalanceTaskContextFactory;
import py.infocenter.rebalance.old.RebalanceTaskExecutionResult;


public class RebalanceTaskContextFactoryImpl implements RebalanceTaskContextFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(RebalanceTaskContextFactoryImpl.class);

  private static final int DEFAULT_DELAY = 5000;

  @Override
  public BasicRebalanceTaskContext generateContext(RebalanceTaskExecutionResult executionResult) {
    BasicRebalanceTaskContext oldContext = executionResult.getContext();
    if (executionResult.isDiscard() || oldContext.tooManyFailures()) {
      return new BasicRebalanceTaskContext(DEFAULT_DELAY, oldContext.getAppContext());
    }

    if (oldContext instanceof CreateSecondaryCandidateContext
        || oldContext instanceof MigratePrimaryContext
        || oldContext instanceof InnerMigrateSegmentUnitContext) {
      if (executionResult.done()) {
        if (oldContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN) {
          if (oldContext instanceof CreateSecondaryCandidateContext) {
            return new CreateSecondaryCandidateContext(0,
                BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT,
                oldContext.getRebalanceTask(), oldContext.getAppContext());
          } else if (oldContext instanceof MigratePrimaryContext) {
            return new MigratePrimaryContext(0,
                BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT,
                oldContext.getRebalanceTask(), oldContext.getAppContext());
          } else { // if (basicContext instanceof InnerMigrateSegmentUnitContext) // always true
            return new InnerMigrateSegmentUnitContext(0,
                BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT,
                oldContext.getRebalanceTask(), oldContext.getAppContext());
          }
        } else if (oldContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.REQUEST_SENT) {
          logger.warn("task done ! {}", oldContext.getRebalanceTask());
          return new BasicRebalanceTaskContext(DEFAULT_DELAY,
              oldContext.getAppContext());
         
         
         
        }
      } else {
        logger.debug("not done, still {}", oldContext.getPhase());
        oldContext.updateDelay(DEFAULT_DELAY);
        return oldContext;
      }
    }

    BasicRebalanceTaskContext basicContext = (BasicRebalanceTaskContext) oldContext;
    if (executionResult.done()) {
      if (basicContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.INIT) {
        if (basicContext.getRebalanceTask() != null) {
          switch (basicContext.getRebalanceTask().getTaskType()) {
            case NormalRebalance:
              return new CreateSecondaryCandidateContext(0,
                  BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN,
                  basicContext.getRebalanceTask(), basicContext.getAppContext());
            case InsideRebalance:
              return new InnerMigrateSegmentUnitContext(0,
                  BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN,
                  basicContext.getRebalanceTask(), basicContext.getAppContext());
            case PrimaryRebalance:
              return new MigratePrimaryContext(0,
                  BasicRebalanceTaskContext.RebalancePhase.TASK_GOTTEN,
                  basicContext.getRebalanceTask(),
                  basicContext.getAppContext());
            default:
          }
        }
        return new BasicRebalanceTaskContext(DEFAULT_DELAY, oldContext.getAppContext());
      } else {
        return new BasicRebalanceTaskContext(DEFAULT_DELAY, oldContext.getAppContext());
      }
    } else {
      oldContext.updateDelay(DEFAULT_DELAY);
      return oldContext;
    }
  }

}
