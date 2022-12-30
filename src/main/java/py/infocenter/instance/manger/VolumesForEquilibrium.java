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
