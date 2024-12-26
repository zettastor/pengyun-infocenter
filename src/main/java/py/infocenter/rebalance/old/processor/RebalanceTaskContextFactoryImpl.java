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
