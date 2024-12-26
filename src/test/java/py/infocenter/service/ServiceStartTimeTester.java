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

package py.infocenter.service;

import java.io.File;
import java.util.UUID;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.store.control.ServiceStartTimes;
import py.test.TestBase;


public class ServiceStartTimeTester extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ServiceStartTimeTester.class);

  @Test
  public void testSave() throws Exception {
    ServiceStartTimes startTimes = new ServiceStartTimes();
    String path = "/tmp/" + UUID.randomUUID().toString() + "_test";
    startTimes.setExternalFile(new File(path));
    startTimes.setStartTimes(0);
    startTimes.save();

    startTimes.load();


  }

  @Test
  public void testSaveTwice() throws Exception {
    ServiceStartTimes startTimes = new ServiceStartTimes();
    String path = "/tmp/" + UUID.randomUUID().toString() + "_test";
    startTimes.setExternalFile(new File(path));
    startTimes.setStartTimes(0);
    startTimes.save();

    int firstSaveStartTimes = save(startTimes, path);
    ServiceStartTimes startTimes1 = new ServiceStartTimes();
    int secondSaveStartTimes = save(startTimes1, path);

    logger.debug("first save is {},second save is {}", firstSaveStartTimes, secondSaveStartTimes);
    Assert.assertEquals(firstSaveStartTimes + 1, secondSaveStartTimes);
  }

  @Test
  public void testLoad() throws Exception {
    ServiceStartTimes startTimes = new ServiceStartTimes();
    String path = "/tmp/" + UUID.randomUUID().toString() + "_test";
    File file = new File(path);
    if (file.exists()) {
      file.delete();
    }
    startTimes.setExternalFile(new File(path));
    startTimes.load();

    Assert.assertTrue(true);
  }

  private int save(ServiceStartTimes serviceStartTimes, String path) throws Exception {
    serviceStartTimes = new ServiceStartTimes();
    serviceStartTimes.setExternalFile(new File(path));
    serviceStartTimes.load();
    if (serviceStartTimes.getStartTimes() == 0) {
      logger.debug("This is the first start, going to install default license");
    } else {
      // do nothing
      logger.debug(
          "This is not the first start, the default license had been installed in the past.");
    }
    serviceStartTimes.setStartTimes(serviceStartTimes.getStartTimes() + 1);
    serviceStartTimes.save();
    return serviceStartTimes.getStartTimes();
  }
}
