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

package py.infocenter.rebalance;


public class RebalanceConfiguration {

  private static int maxRebalanceTaskCountPerVolumeOfDatanode = 15;   // the max rebalance task 
  // count in volume per datanode
  private static int maxRebalanceVolumeCountPerPool = 1; // the max volume count in pool for 
  // rebalance
  private static RebalanceConfiguration instance = new RebalanceConfiguration();
  private int pressureAddend = 0;
  private double pressureThreshold = 0.05;
  private long pressureThresholdAccuracy = 3;
  private int minOfSegmentCountCanDoRebalance = 20;
  private int rebalanceTaskExpireTimeSeconds = 1800;
  private int segmentWrapSize = 10;
  private long rebalanceTriggerPeriod = 60;

  public static RebalanceConfiguration getInstance() {
    return instance;
  }

  public int getPressureAddend() {
    return pressureAddend;
  }

  public void setPressureAddend(int pressureAddend) {
    this.pressureAddend = pressureAddend;
  }

  public double getPressureThreshold() {
    return pressureThreshold;
  }

  public void setPressureThreshold(double pressureThreshold) {
    this.pressureThreshold = pressureThreshold;
  }

  public long getPressureThresholdAccuracy() {
    return pressureThresholdAccuracy;
  }

  public void setPressureThresholdAccuracy(long pressureThresholdAccuracy) {
    this.pressureThresholdAccuracy = pressureThresholdAccuracy;
  }

  public int getRebalanceTaskExpireTimeSeconds() {
    return rebalanceTaskExpireTimeSeconds;
  }

  public void setRebalanceTaskExpireTimeSeconds(int rebalanceTaskExpireTimeSeconds) {
    this.rebalanceTaskExpireTimeSeconds = rebalanceTaskExpireTimeSeconds;
  }

  public int getSegmentWrapSize() {
    return segmentWrapSize;
  }

  public void setSegmentWrapSize(int segmentWrapSize) {
    this.segmentWrapSize = segmentWrapSize;
  }

  public int getMaxRebalanceTaskCountPerVolumeOfDatanode() {
    return maxRebalanceTaskCountPerVolumeOfDatanode;
  }

  public void setMaxRebalanceTaskCountPerVolumeOfDatanode(
      int maxRebalanceTaskCountPerVolumeOfDatanode) {
    RebalanceConfiguration.maxRebalanceTaskCountPerVolumeOfDatanode =
        maxRebalanceTaskCountPerVolumeOfDatanode;
  }

  public int getMaxRebalanceVolumeCountPerPool() {
    return maxRebalanceVolumeCountPerPool;
  }

  public void setMaxRebalanceVolumeCountPerPool(int maxRebalanceVolumeCountPerPool) {
    RebalanceConfiguration.maxRebalanceVolumeCountPerPool = maxRebalanceVolumeCountPerPool;
  }

  public long getRebalanceTriggerPeriod() {
    return rebalanceTriggerPeriod;
  }

  public void setRebalanceTriggerPeriod(long rebalanceTriggerPeriod) {
    this.rebalanceTriggerPeriod = rebalanceTriggerPeriod;
  }

  public int getMinOfSegmentCountCanDoRebalance() {
    return minOfSegmentCountCanDoRebalance;
  }

  public void setMinOfSegmentCountCanDoRebalance(int minOfSegmentCountCanDoRebalance) {
    this.minOfSegmentCountCanDoRebalance = minOfSegmentCountCanDoRebalance;
  }
}
