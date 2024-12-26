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

package py.infocenter.instance.manger;


/**
 * just for Equilibrium volume, the Unavailable volume can not Equilibrium.
 */
public class VolumesForEquilibrium implements Cloneable {

  boolean isAvailable;
  private long volumeId;
  private long volumeSize;

  public VolumesForEquilibrium() {
  }


  public VolumesForEquilibrium(long volumeId, long volumeSize, boolean isAvailable) {
    this.volumeId = volumeId;
    this.volumeSize = volumeSize;
    this.isAvailable = isAvailable;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getVolumeSize() {
    return volumeSize;
  }

  public void setVolumeSize(long volumeSize) {
    this.volumeSize = volumeSize;
  }

  public boolean isAvailable() {
    return isAvailable;
  }

  public void setAvailable(boolean available) {
    isAvailable = available;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public String toString() {
    return "VolumeInfoForEquilibrium{"

        + "volumeId=" + volumeId

        + ", volumeSize=" + volumeSize

        + ", isAvailable=" + isAvailable

        + '}';
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
