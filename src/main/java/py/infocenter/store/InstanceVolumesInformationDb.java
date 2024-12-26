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

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import javax.persistence.Lob;
import org.apache.commons.lang.Validate;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.informationcenter.Utils;


public class InstanceVolumesInformationDb {

  private static final Logger logger = LoggerFactory.getLogger(InstanceVolumesInformationDb.class);
  private long instanceId;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob volumeIds;

  public InstanceVolumesInformationDb() {
  }



  public InstanceVolumesInformation toInstanceVolumesInformation() {
    InstanceVolumesInformation instanceVolumesInformation = new InstanceVolumesInformation();
    instanceVolumesInformation.setInstanceId(instanceId);

    if (volumeIds != null) {
      String volumeIdsStr = null;
      try {
        volumeIdsStr = new String(py.license.Utils.readFrom(volumeIds));
        Set<Long> volumeList = new HashSet<>();
        volumeList.addAll(Utils.parseObjecLongFromJsonStr(volumeIdsStr));
        Validate.notNull(volumeList);
        instanceVolumesInformation.setVolumeIds(volumeList);

      } catch (SQLException | IOException e) {
        logger.error("caught exception when parseObjec volume segment info, ", e);
      }
    }

    return instanceVolumesInformation;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public Blob getVolumeIds() {
    return volumeIds;
  }

  public void setVolumeIds(Blob volumeIds) {
    this.volumeIds = volumeIds;
  }

  @Override
  public String toString() {
    return "InstanceVolumesInformationDB{"

        + "instanceId=" + instanceId

        + ", volumeInfo=" + volumeIds

        + '}';
  }

}
