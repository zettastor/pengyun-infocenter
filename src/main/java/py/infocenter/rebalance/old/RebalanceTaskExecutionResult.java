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


import py.infocenter.rebalance.old.processor.BasicRebalanceTaskContext;


public class RebalanceTaskExecutionResult {

  private final BasicRebalanceTaskContext context;

  private boolean done;

  private boolean discard;


  
  public RebalanceTaskExecutionResult(BasicRebalanceTaskContext context) {
    this.context = context;
    this.done = false;
    this.discard = false;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  public boolean done() {
    return done;
  }

  public boolean isDiscard() {
    return discard;
  }

  public void setDiscard(boolean discard) {
    this.discard = discard;
  }

  public BasicRebalanceTaskContext getContext() {
    return this.context;
  }

}
