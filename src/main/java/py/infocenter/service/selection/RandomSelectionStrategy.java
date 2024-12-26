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

package py.infocenter.service.selection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
public class RandomSelectionStrategy<T> implements SelectionStrategy<T> {

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
  public List<T> select(Collection<T> objs, int n) {
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
