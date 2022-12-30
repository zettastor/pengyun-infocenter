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
