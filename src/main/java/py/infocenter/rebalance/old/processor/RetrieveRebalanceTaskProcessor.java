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

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.RequestIdBuilder;
import py.infocenter.rebalance.old.RebalanceTaskContext;
import py.infocenter.rebalance.old.RebalanceTaskExecutionResult;
import py.infocenter.rebalance.old.RebalanceTaskProcessor;
import py.infocenter.service.InformationCenterImpl;
import py.rebalance.RebalanceTask;
import py.thrift.infocenter.service.NoNeedToRebalanceThrift;
import py.thrift.infocenter.service.RetrieveOneRebalanceTaskRequest;
import py.thrift.infocenter.service.RetrieveOneRebalanceTaskResponse;


public class RetrieveRebalanceTaskProcessor extends RebalanceTaskProcessor {

  private static final Logger logger = LoggerFactory
      .getLogger(RetrieveRebalanceTaskProcessor.class);

  public RetrieveRebalanceTaskProcessor(BasicRebalanceTaskContext context,
      InformationCenterImpl informationCenter) {
    super(context, informationCenter);
  }

  @Override
  public RebalanceTaskExecutionResult call() {
    try {
      RebalanceTaskContext context = getContext();
      BasicRebalanceTaskContext myContext = (BasicRebalanceTaskContext) context;
      Validate.isTrue(myContext.getPhase() == BasicRebalanceTaskContext.RebalancePhase.INIT,
          "phase must be INIT");

      RebalanceTaskExecutionResult result = internalProcess(myContext);
      return result;

    } catch (Throwable t) {
      logger.warn("caught an mystery throwable", t);
      return stayHere(true);
    }
  }

  private RebalanceTaskExecutionResult internalProcess(BasicRebalanceTaskContext myContext) {

    if (!myContext.statusHealthy()) {
      return stayHere(true);
    }

    try {
      RetrieveOneRebalanceTaskRequest request = new RetrieveOneRebalanceTaskRequest(
          RequestIdBuilder.get(), true);
      RetrieveOneRebalanceTaskResponse response =
          informationCenter.retrieveOneRebalanceTask(request);
      RebalanceTask rebalanceTask = RequestResponseHelper
          .buildRebalanceTask(response.getRebalanceTask());
      logger.warn("got a rebalance task {}", rebalanceTask);
      myContext.setRebalanceTask(rebalanceTask);
      return nextPhase();
    } catch (NoNeedToRebalanceThrift e) {
      logger.info("infocenter told me that there is no need to rebalance");
      return stayHere(true);
    } catch (Exception e) {
      logger.warn("caught an mystery exception", e);
      return stayHere(false);
    }
  }

}
