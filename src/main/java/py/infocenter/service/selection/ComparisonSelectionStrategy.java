

package py.infocenter.service.selection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A class implements the selection strategy interface {@link SelectionStrategy}. The strategy
 * compares each elements in the collection to select expected number of elements.
 *
 */
public class ComparisonSelectionStrategy<T> implements SelectionStrategy<T> {

  private Comparator<T> comparator;

  public ComparisonSelectionStrategy(Comparator<T> comparable) {
    this.comparator = comparable;
  }

  @Override
  public List<T> select(Collection<T> objs, int n) {
    List<T> objList = new ArrayList<T>(objs);
    Collections.sort(objList, comparator);

    List<T> selectedObjList = new ArrayList<T>();
    for (int i = 0; i < n; i++) {
      selectedObjList.add(objList.get(i));
    }

    return selectedObjList;
  }
}
