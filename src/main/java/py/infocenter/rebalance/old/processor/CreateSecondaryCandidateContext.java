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

import py.app.context.AppContext;
import py.rebalance.RebalanceTask;


public class CreateSecondaryCandidateContext extends BasicRebalanceTaskContext {

  public CreateSecondaryCandidateContext(long delay, RebalancePhase phase,
      RebalanceTask rebalanceTask,
      AppContext appContext) {
    super(delay, appContext, phase);
    this.rebalanceTask = rebalanceTask;
  }

  @Override
  public String toString() {
    return getClass().getName() + "[ super=" + super.toString() + "]";
  }
}
