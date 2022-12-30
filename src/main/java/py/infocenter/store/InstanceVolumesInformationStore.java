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
import java.util.List;


public interface InstanceVolumesInformationStore {

  public void saveInstanceVolumesInformationToDb(
      InstanceVolumesInformation instanceVolumesInformation);

  public int deleteInstanceVolumesInformationFromDb(Long instanceId);

  public InstanceVolumesInformation getInstanceVolumesInformationFromDb(Long instanceId)
      throws SQLException, IOException;

  public List<InstanceVolumesInformation> reloadAllInstanceVolumesInformationFromDb()
      throws SQLException, IOException;

  public Blob createBlob(byte[] bytes);

}
