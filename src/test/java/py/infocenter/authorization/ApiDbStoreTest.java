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

package py.infocenter.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import py.icshare.authorization.ApiStore;
import py.icshare.authorization.ApiToAuthorize;
import py.volume.ClosedRangeSetParser;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {InformationCenterDbConfigTest.class})
public class ApiDbStoreTest extends AuthorizationTestBase {

  private static final Logger logger = LoggerFactory.getLogger(RoleDbStoreTest.class);

  @Autowired
  ApiStore apiStore;

  @Before
  public void init() {
    apiStore.cleanApis();
  }

  /**
   * Test save api and update api. * Test step: 1. create two apis save them, then list all apis
   * hope get these two apis; 2. update api1's category and save it, check update successfully.
   */
  @Test
  public void testSaveApiAndListApi() {
    // step 1

    for (ApiToAuthorize api : prepareApis()) {
      apiStore.saveApi(api);
    }

    List<ApiToAuthorize> apis = apiStore.listApis();
    assertEquals(2L, apis.size());

    // step 2
    for (ApiToAuthorize api : apis) {
      if (api.getApiName().equals("api1")) {
        api.setCategory(ApiToAuthorize.ApiCategory.Other.name());
        apiStore.saveApi(api);
      }
    }
    apis = apiStore.listApis();
    for (ApiToAuthorize api : apis) {
      if (api.getApiName().equals("api1")) {
        assertEquals(ApiToAuthorize.ApiCategory.Other.name(), api.getCategory());
      }
    }
  }

  /**
   * Test save api and delete this api. * Test step: 1. create an api save it, then list all apis
   * hope get one api; 2. delete this api, list all apis again and hope get nothing.
   */
  @Test
  public void testDeleteApi() {
    // step 1
    apiStore.saveApi(prepareApi());

    List<ApiToAuthorize> apis = apiStore.listApis();
    assertEquals(1L, apis.size());

    ApiToAuthorize api = apis.get(0);

    // step 2
    apiStore.deleteApi(api);

    apis = apiStore.listApis();
    assertTrue(apis.isEmpty());
  }

  /**
   * Test save api twice and second save is actually update. * Test step: 1. create an api save it
   * twice, then list all apis hope get one api;
   */
  @Test
  public void testSaveApiTwice() {
    apiStore.saveApi(prepareApi());
    apiStore.saveApi(prepareApi());

    List<ApiToAuthorize> apis = apiStore.listApis();
    assertEquals(1L, apis.size());
  }

  /**
   * Test before test start, database is clear. * Test step: 1. list api directly, hope get
   * nothing.
   */
  @Test
  public void testCleanApi() {
    List<ApiToAuthorize> apis = apiStore.listApis();
    assertTrue(apis.isEmpty());
  }

  @Test
  public void testCheck() {

    RangeSet<Integer> volumeLayoutRange = TreeRangeSet.create();

    volumeLayoutRange.add(Range.closed(0, 1));

    List<Range<Integer>> rangesToRemove = new ArrayList<Range<Integer>>();
    for (Range<Integer> range : volumeLayoutRange.asRanges()) {
      if (range.lowerBoundType() == BoundType.OPEN) {
        rangesToRemove.add(Range.open(range.lowerEndpoint(), range.lowerEndpoint() + 1));
      }
      if (range.upperBoundType() == BoundType.OPEN) {
        rangesToRemove.add(Range.open(range.upperEndpoint() - 1, range.upperEndpoint()));
      }
    }

    for (Range<Integer> range : rangesToRemove) {
      volumeLayoutRange.remove(range);
    }
    String volumeLayoutString = volumeLayoutRange.toString();

    volumeLayoutRange = ClosedRangeSetParser.parseRange(volumeLayoutString);
    logger.warn("get the :{}", volumeLayoutRange);
  }

  @After
  public void clean() {
    apiStore.cleanApis();
  }
}
