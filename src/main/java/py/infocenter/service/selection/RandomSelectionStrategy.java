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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class of algorithm to random select specified number of elements from a collection. This
 * algorithm uses two steps to finish random selection: * 1: random pick n*factor+remainder elements
 * from the collection.(n is the number of elements expected finally) * 2: random pick n elements
 * from the n*factor+remainder elements.
 *
 */
public class RandomSelectionStrategy implements SelectionStrategy {

  private static final Logger logger = LoggerFactory.getLogger(RandomSelectionStrategy.class);

  /*
   * used in step 1 to pick elements
   */
  private int factor = 2;

  /*
   * used in step 1 to pick elements
   */
  private int remainder = 0;

  /**
   * Random select specified n elements from the given collection.
   */
  @Override
  public <T> List<T> select(Collection<T> objs, int n) {
    if (n > objs.size()) {
      logger.warn("Unable to select {} elements from collection with size {}", n, objs.size());
      return null;
    }

    List<T> objList = new ArrayList<T>();
    for (T obj : objs) {
      objList.add(obj);
    }

    int numOfCandidates = Math.min(factor * n + remainder, objList.size());

    List<T> candidateList = new ArrayList<T>(numOfCandidates);

    Collections.shuffle(objList);
    for (int i = 0; i < numOfCandidates; i++) {
      candidateList.add(objList.get(i));
    }

    Collections.shuffle(candidateList);
    List<T> selectedItemList = new ArrayList<T>(n);
    for (int i = 0; i < n; i++) {
      selectedItemList.add(candidateList.get(i));
    }

    return selectedItemList;
  }

  public int getFactor() {
    return factor;
  }

  public void setFactor(int factor) {
    this.factor = factor;
  }

  public int getRemainder() {
    return remainder;
  }

  public void setRemainder(int remainder) {
    this.remainder = remainder;
  }
}
