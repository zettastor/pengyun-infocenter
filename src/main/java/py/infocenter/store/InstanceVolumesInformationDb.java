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
