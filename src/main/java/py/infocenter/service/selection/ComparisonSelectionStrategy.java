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

import edu.emory.mathcs.backport.java.util.Collections;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * A class implements the selection strategy interface {@link SelectionStrategy}. The strategy
 * compares each elements in the collection to select expected number of elements.
 *
 */
public class ComparisonSelectionStrategy<T> implements SelectionStrategy {

  private Comparator<T> comparator;

  public ComparisonSelectionStrategy(Comparator<T> comparable) {
    this.comparator = comparable;
  }

  @Override
  public <T> List<T> select(Collection<T> objs, int n) {
    List<T> objList = new ArrayList<T>();
    for (T obj : objs) {
      objList.add(obj);
    }

    Collections.sort(objList, comparator);

    List<T> selectedObjList = new ArrayList<T>();
    for (int i = 0; i < n; i++) {
      selectedObjList.add(objList.get(i));
    }

    return selectedObjList;
  }
}
