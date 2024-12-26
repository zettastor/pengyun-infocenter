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
