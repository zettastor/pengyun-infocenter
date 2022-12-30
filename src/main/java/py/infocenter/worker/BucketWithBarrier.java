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

package py.infocenter.worker;


public class BucketWithBarrier {

  private final int numberOfContainer;
  private final long[] containers;
  private final long[] barrierHeights;
  private int fillCount;
  private int overwhelmBarrierIndex;



  public BucketWithBarrier(int numberOfContainer) {
    this.numberOfContainer = numberOfContainer;
    containers = new long[numberOfContainer];
    barrierHeights = new long[numberOfContainer];
    fillCount = 0;
    overwhelmBarrierIndex = numberOfContainer - 1;
  }



  public void fill(long volume) {
    long volumeLeft = volume;
    if (fillCount < numberOfContainer) {
      setOneBarrierHeight(fillCount, volume);
      containers[fillCount] = volume;
    } else {
      while (volumeLeft > 0) {
        int numberOfContainerToFillCurrently = numberOfContainer - overwhelmBarrierIndex;
        if (numberOfContainerToFillCurrently == numberOfContainer) {
          for (int i = 0; i < numberOfContainer; i++) {
            containers[i] += volumeLeft / numberOfContainer;
          }
          volumeLeft = 0;
        } else {
          long heightToOverwhelmHigherBarrier = barrierHeights[overwhelmBarrierIndex - 1]
              - containers[overwhelmBarrierIndex];
          if (numberOfContainerToFillCurrently * heightToOverwhelmHigherBarrier
              <= volumeLeft) {
            for (int i = overwhelmBarrierIndex; i < numberOfContainer; i++) {
              containers[i] = barrierHeights[overwhelmBarrierIndex - 1];
            }
            volumeLeft -= numberOfContainerToFillCurrently * heightToOverwhelmHigherBarrier;
            overwhelmBarrierIndex--;
          } else {
            for (int i = overwhelmBarrierIndex; i < numberOfContainer; i++) {
              containers[i] += volumeLeft / numberOfContainerToFillCurrently;
            }
            volumeLeft = 0;
          }
        }
      }
    }
    fillCount++;
  }

  public long getLowest() {
    return containers[numberOfContainer - 1];
  }

  private void setOneBarrierHeight(int index, long height) {
    barrierHeights[index] = height;
  }
}
