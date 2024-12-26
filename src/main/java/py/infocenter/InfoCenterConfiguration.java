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

package py.infocenter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource("classpath:config/infocenter.properties")
public class InfoCenterConfiguration {

  @Value("${segment.creator.enabled:false}")
  public boolean segmentCreatorEnabled = false;

  @Value("${segment.creator.blocking.timeout.ms:10000}")
  public long segmentCreatorBlockingTimeoutMillis = 10000;

  @Value("${segment.num.to.create.each.time.for.simple.configured.volume:5}")
  public int segmentNumToCreateEachTimeForSimpleConfiguredVolume = 5;

  public boolean isSegmentCreatorEnabled() {
    return segmentCreatorEnabled;
  }

  public void setSegmentCreatorEnabled(boolean segmentCreatorEnabled) {
    this.segmentCreatorEnabled = segmentCreatorEnabled;
  }

  public long getSegmentCreatorBlockingTimeoutMillis() {
    return segmentCreatorBlockingTimeoutMillis;
  }

  public void setSegmentCreatorBlockingTimeoutMillis(long segmentCreatorBlockingTimeoutMillis) {
    this.segmentCreatorBlockingTimeoutMillis = segmentCreatorBlockingTimeoutMillis;
  }

  public int getSegmentNumToCreateEachTimeForSimpleConfiguredVolume() {
    return segmentNumToCreateEachTimeForSimpleConfiguredVolume;
  }

  public void setSegmentNumToCreateEachTimeForSimpleConfiguredVolume(
      int segmentNumToCreateEachTimeForSimpleConfiguredVolume) {
    this.segmentNumToCreateEachTimeForSimpleConfiguredVolume =
        segmentNumToCreateEachTimeForSimpleConfiguredVolume;
  }
}
