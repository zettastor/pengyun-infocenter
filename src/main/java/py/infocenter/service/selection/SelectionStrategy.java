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

package py.infocenter.service.selection;

import java.util.Collection;
import java.util.List;

/**
 * An interface which defines a method to select specified number of elements from a given
 * collection. * {@link RandomSelectionStrategy} implements this interface as a strategy to random
 * select elements from collection.
 *
 */
public interface SelectionStrategy {

  public <T> List<T> select(Collection<T> objs, int n);
}
