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
