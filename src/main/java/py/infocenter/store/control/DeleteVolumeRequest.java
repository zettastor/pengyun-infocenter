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

package py.infocenter.store.control;


public class DeleteVolumeRequest {

  private long volumeId;
  private String volumeName;
  private long accountId;
  private long newVolumeId;
  private String newVolumeName;

  private long createdAt;

  public DeleteVolumeRequest() {

  }


  
  public DeleteVolumeRequest(long volumeId, String volumeName, long accountId, long newVolumeId,
      String newVolumeName) {
    super();
    this.volumeId = volumeId;
    this.volumeName = volumeName;
    this.accountId = accountId;
    this.newVolumeId = newVolumeId;
    this.newVolumeName = newVolumeName;
    this.createdAt = System.currentTimeMillis();
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public long getNewVolumeId() {
    return newVolumeId;
  }

  public void setNewVolumeId(long newVolumeId) {
    this.newVolumeId = newVolumeId;
  }

  public String getVolumeName() {
    return newVolumeName;
  }

  public void setVolumeName(String volumeName) {
    this.newVolumeName = volumeName;
  }

  public String getNewVolumeName() {
    return newVolumeName;
  }

  public void setNewVolumeName(String newVolumeName) {
    this.newVolumeName = newVolumeName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (accountId ^ (accountId >>> 32));
    result = prime * result + (int) (createdAt ^ (createdAt >>> 32));
    result = prime * result + (int) (newVolumeId ^ (newVolumeId >>> 32));
    result = prime * result + ((newVolumeName == null) ? 0 : newVolumeName.hashCode());
    result = prime * result + (int) (volumeId ^ (volumeId >>> 32));
    result = prime * result + ((volumeName == null) ? 0 : volumeName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DeleteVolumeRequest other = (DeleteVolumeRequest) obj;
    if (accountId != other.accountId) {
      return false;
    }
    if (createdAt != other.createdAt) {
      return false;
    }
    if (newVolumeId != other.newVolumeId) {
      return false;
    }
    if (newVolumeName == null) {
      if (other.newVolumeName != null) {
        return false;
      }
    } else if (!newVolumeName.equals(other.newVolumeName)) {
      return false;
    }
    if (volumeId != other.volumeId) {
      return false;
    }
    if (volumeName == null) {
      if (other.volumeName != null) {
        return false;
      }
    } else if (!volumeName.equals(other.volumeName)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "DeleteVolumeRequest [volumeId=" + volumeId + ", volumeName=" + volumeName
        + ", accountId=" + accountId
        + ", newVolumeId=" + newVolumeId + ", newVolumeName=" + newVolumeName + ", createdAt="
        + createdAt
        + "]";
  }

}
