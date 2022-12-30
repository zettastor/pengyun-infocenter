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

package py.infocenter.qos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import net.sf.json.JSONArray;
import org.junit.Test;
import py.RequestResponseHelper;
import py.informationcenter.Utils;
import py.instance.InstanceId;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationEntry;
import py.test.TestBase;


public class ParseJsonIoLimitationEntryTest extends TestBase {

  @Test
  public void testParseJsonStrFromInstanceIdList() {
    List<InstanceId> datanodeIdList = new ArrayList<>();
    InstanceId id1 = new InstanceId(10001);
    InstanceId id2 = new InstanceId(10002);
    InstanceId id3 = new InstanceId(10003);
    datanodeIdList.add(id1);
    datanodeIdList.add(id2);
    datanodeIdList.add(id3);

    String trans = Utils.bulidJsonStrFromObject(datanodeIdList);

    Set<InstanceId> transList = new HashSet<>();
    transList.addAll(Utils.parseObjecFromJsonStr(trans));
    assertTrue(transList.size() == datanodeIdList.size());
    for (InstanceId datanodeId : datanodeIdList) {
      assertTrue(transList.contains(datanodeId));
    }
  }

  @Test
  public void testParseJsonIoLimitationEntry() throws Exception {
    List<IoLimitationEntry> entryList = new ArrayList<>();
    IoLimitationEntry entry1 = new IoLimitationEntry(1, 2, 1, 3, 2, LocalTime.parse("11:11:11"),
        LocalTime.parse("11:11:11"));
    entryList.add(entry1);

    JSONArray jsonArray = new JSONArray();
    for (IoLimitationEntry entry : entryList) {
      jsonArray.add(entry.toJsonString());
    }

    List<IoLimitationEntry> ioLimitationEntries = new ArrayList<>();
    logger.debug("parseIoLimitationEntries {}", jsonArray);
    Iterator<Object> iterator = jsonArray.iterator();
    while (iterator.hasNext()) {
      String json = iterator.next().toString();
      logger.warn("parseIoLimitationEntries json {}", json);
      ioLimitationEntries.add(IoLimitationEntry.fromJson(json));
      logger.warn("parseIoLimitationEntries after {}", ioLimitationEntries);
    }

    assertEquals(1, ioLimitationEntries.size());
  }


  @Test
  public void testJudgeDynamicIoLimitationTimeInterleaving() {
    IoLimitation updateIoLimitation = new IoLimitation();
    updateIoLimitation.setName("test");

    LocalTime startTime0 = LocalTime.now().plusSeconds(0);
    LocalTime endTime0 = LocalTime.now().plusSeconds(4);

    LocalTime startTime1 = LocalTime.now().plusSeconds(5);
    LocalTime endTime1 = LocalTime.now().plusSeconds(10);

    LocalTime startTime2 = LocalTime.now().plusSeconds(9);
    LocalTime endTime2 = LocalTime.now().plusSeconds(15);

    LocalTime startTime3 = LocalTime.now().plusSeconds(20);
    LocalTime endTime3 = LocalTime.now().plusSeconds(25);

    boolean existTimeInterleaving = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(updateIoLimitation);
    assertFalse(existTimeInterleaving);

    List<IoLimitationEntry> entries = new ArrayList<>();
    IoLimitationEntry entry1 = new IoLimitationEntry(0, 100, 10, 1000, 100, startTime0, endTime0);
    IoLimitationEntry entry2 = new IoLimitationEntry(1, 100, 10, 1000, 100, startTime1, endTime1);
    final IoLimitationEntry entry3 = new IoLimitationEntry(2, 200, 20, 2000, 200, startTime2,
        endTime2);
    final IoLimitationEntry entry4 = new IoLimitationEntry(3, 300, 30, 3000, 300, startTime3,
        endTime3);
    entries.add(entry1);
    entries.add(entry2);

    updateIoLimitation.setLimitType(IoLimitation.LimitType.Dynamic);
    updateIoLimitation.setEntries(entries);

    logger.warn("testJudgeIoLimitationTimeInterleaving {} ", updateIoLimitation);
    existTimeInterleaving = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(updateIoLimitation);
    assertFalse(existTimeInterleaving);

    updateIoLimitation.setLimitType(IoLimitation.LimitType.Dynamic);
    List<IoLimitationEntry> entrys2 = new ArrayList<>();
    entrys2.add(entry1);
    entrys2.add(entry2);
    entrys2.add(entry3);
    entrys2.add(entry4);
    updateIoLimitation.setEntries(entrys2);
    logger.warn("testJudgeIoLimitationTimeInterleaving {} ", updateIoLimitation);
    existTimeInterleaving = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(updateIoLimitation);
    assertTrue(existTimeInterleaving);

    IoLimitation updateIoLimitation2 = null;
    logger.warn("testJudgeIoLimitationTimeInterleaving {} ", updateIoLimitation);
    existTimeInterleaving = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(updateIoLimitation2);
    assertFalse(existTimeInterleaving);

    updateIoLimitation2 = new IoLimitation();
    logger.warn("testJudgeIoLimitationTimeInterleaving {} ", updateIoLimitation);
    existTimeInterleaving = RequestResponseHelper
        .judgeDynamicIoLimitationTimeInterleaving(updateIoLimitation2);
    assertFalse(existTimeInterleaving);
  }

  private boolean inTimeSpan(LocalTime startTime, LocalTime endTime) {
    LocalTime now = LocalTime.now();
    if (startTime.isAfter(endTime)) {
      return false;
    }
    if (now.isBefore(startTime) || now.isAfter(endTime)) {
      return false;
    }
    return true;
  }


}
