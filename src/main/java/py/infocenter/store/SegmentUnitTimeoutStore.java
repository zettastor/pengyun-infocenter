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

package py.infocenter.store;

import java.util.Collection;
import py.archive.segment.SegmentUnitMetadata;


public interface SegmentUnitTimeoutStore {

  /*Add the segment unit for checking its report is timeout */
  public void addSegmentUnit(SegmentUnitMetadata segUnit);

  /*Get the volume ids whose segment unit is timeout */
  public int drainTo(Collection<Long> volumes);

  /*clear all segment unit which need to check status*/
  public void clear();
}
