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

package py.infocenter.store;

import java.util.List;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;


public interface VolumeStore {

  // load volume in db when infocenter starts
  public void loadVolumeInDb();

  // delete a record from volume table
  public void deleteVolume(VolumeMetadata volumeMetadata);

  // update or insert a record(volumeMetadata)
  public void saveVolume(VolumeMetadata volumeMetadata);

  // List all volumes belonging to an account, if account Id is null, then return all volumes
  public List<VolumeMetadata> listVolumes();

  public VolumeMetadata getVolume(Long volumeId);

  public void loadVolumeFromDbToMemory();

  public void clearData();

  public VolumeMetadata getVolumeNotDeadByName(String name);

  public int updateStatusAndVolumeInAction(long volumeId, String status, String volumeInAction);

  public long updateRebalanceVersion(long volumeId, long version);

  public long incrementRebalanceVersion(long volumeId);

  public long getRebalanceVersion(long volumeId);

  public void updateRebalanceInfo(long volumeId, VolumeRebalanceInfo rebalanceInfo);

  public void updateStableTime(long volumeId, long timeMs);

  public String updateVolumeLayout(long volumeId, int startSegmentIndex, int newSegmentsCount,
      String volumeLayout);

  public void updateDescription(long volumeId, String description);

  public void updateClientLastConnectTime(long volumeId, long time);

  //just get VolumeMetadata in db
  public VolumeMetadata followerGetVolume(Long volumeId);

  /* just for report to master **/
  public void deleteVolumeForReport(VolumeMetadata volumeMetadata);

  public VolumeMetadata getVolumeForReport(Long volumeId);

  public String updateVolumeLayoutForReport(long volumeId, int startSegmentIndex,
      int newSegmentsCount, String volumeLayout);

  public void clearDataForReport();

  public VolumeMetadata listVolumesByIdForReport(long rootId);

  public int updateDeadTimeForReport(long volumeId, long deadTime);

  public int updateStatusAndVolumeInActionForReport(long volumeId, String status,
      String volumeInAction);

  public int updateVolumeExtendStatusForReport(long volumeId, String status);

  public int updateExtendingSizeForReport(long volumeId, long extendingSize,
      long thisTimeExtendSize);

  public void saveVolumeForReport(VolumeMetadata volumeMetadata);

  public List<VolumeMetadata> listVolumesForReport();

  public void updateVolumeForReport(VolumeMetadata volumeMetadata);

  public List<VolumeMetadata> listVolumesWhichToDeleteForReport();

  public void saveVolumesWhichToDeleteForReport(VolumeMetadata volumeMetadata);

  public void deleteVolumesWhichToDeleteForReport(Long volumeId);

  public void markVolumeDelete(Long volumeId);
}

