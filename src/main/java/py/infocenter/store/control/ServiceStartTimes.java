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

package py.infocenter.store.control;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Json class will be used to save the control centre service start times.
 *
 */
public class ServiceStartTimes {

  @JsonIgnore
  private static final Logger logger = LoggerFactory.getLogger(ServiceStartTimes.class);
  @JsonIgnore
  private static File file = new File(System.getProperty("user.dir") + "/config/ServiceStartTimes");
  private int startTimes = 0;
  @JsonIgnore
  private File externalFile = null;


  
  @JsonIgnore
  public boolean save() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      File tmpFile = (externalFile != null) ? externalFile : file;
      logger.debug("Going to save data to save the data into file : {}", tmpFile.getPath());
      objectMapper.writeValue(tmpFile, this);
      return true;
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
  }


  
  @JsonIgnore
  public boolean load() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      ServiceStartTimes tmp = null;
      File tmpFile = (externalFile != null) ? externalFile : file;
      logger.debug("Going to load data from file : {}", tmpFile.getPath());
      if (!tmpFile.exists()) {
        logger.debug("File not existed,Going to create a File");
        if (!save()) {
          logger.error("Create new file failed");
          throw new Exception();
        }
      }

      tmp = objectMapper.readValue(tmpFile, ServiceStartTimes.class);
      this.setStartTimes(tmp.getStartTimes());
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
    return true;
  }

  public int getStartTimes() {
    return startTimes;
  }

  public void setStartTimes(int startTimes) {
    this.startTimes = startTimes;
  }

  @JsonIgnore
  public File getExternalFile() {
    return externalFile;
  }

  @JsonIgnore
  public void setExternalFile(File externalFile) {
    this.externalFile = externalFile;
  }

  @Override
  public String toString() {
    return "ServiceStartTimes [startTimes=" + startTimes + "]";
  }
}
