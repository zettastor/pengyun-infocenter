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

package py.infocenter.create.volume;

/**
 * Factory to create controller type of {@link SegmentCreatorController}.
 *
 */
public class SegmentCreatorControllerFactory {

  public SegmentCreatorController create(long timeoutMillis) {
    return new SegmentCreatorController(timeoutMillis);
  }

  
  public static class SegmentCreatorController {

    private long timeoutMillis;

    public SegmentCreatorController(long timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
    }

    /**
     * Block thread of {@link SegmentCreatorChange} for a while.
     */
    public synchronized void block() throws InterruptedException {
      this.wait(timeoutMillis);
    }

    /**
     * Release the blocked {@link SegmentCreatorChange} thread.
     */
    public synchronized void release() {
      this.notify();
    }
  }
}
