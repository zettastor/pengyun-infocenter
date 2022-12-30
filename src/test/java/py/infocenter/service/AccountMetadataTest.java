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

package py.infocenter.service;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import org.junit.Ignore;
import org.junit.Test;
import py.icshare.AccountMetadata;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.test.TestBase;


public class AccountMetadataTest extends TestBase {

  @Test
  public void testSerializeDeSerialize() throws Exception {

    AccountMetadata account = new AccountMetadata();
    Long accountId = 1L;
    String accountType = "Admin";
    String hashedPassword = "1111";
    String accountName = "aksdfa";

    account.setAccountId(accountId);
    account.setAccountName(accountName);
    account.setAccountType(accountType);
    account.setHashedPassword(hashedPassword);

    ObjectMapper mapper = new ObjectMapper();
    String accountJson = mapper.writeValueAsString(account);

    logger.debug(accountJson);
    AccountMetadata parsedVolume = mapper.readValue(accountJson, AccountMetadata.class);

    assertEquals(accountId, parsedVolume.getAccountId());
    assertEquals(accountType, parsedVolume.getAccountType());
    assertEquals(accountName, parsedVolume.getAccountName());
    assertEquals(hashedPassword, parsedVolume.getHashedPassword());
  }

  @Test
  public void test11() {
    Multimap<Long, Long> volumeOperationMap = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    List<Long> list = new ArrayList<>(volumeOperationMap.get(11L));
    logger.warn("get the list:{}, time:{}", list, System.currentTimeMillis());

    LocalTime lt = LocalTime.parse("20:09");
    lt.toSecondOfDay();
    logger.warn("the time:{}", lt.toSecondOfDay());

    final long nowTimestamp = System.currentTimeMillis();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date());
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    long zeroTimestamp = calendar.getTimeInMillis();
    ;

    long relativeTimeS = nowTimestamp / 1000 - zeroTimestamp / 1000;
    logger.warn("the time22:{}", relativeTimeS);

    String test1 = null;
    Set<String> stringList = new HashSet<>();
    stringList.add("wen");
    stringList.remove(test1);

  }

  @Test
  public void justTest() {

    Map<Long, String> conMap = new ConcurrentHashMap<Long, String>();
    for (long i = 0; i < 15; i++) {
      conMap.put(i, i + "");
    }
    logger.warn("the value :{}", conMap);

    for (Map.Entry<Long, String> entry : conMap.entrySet()) {
      long key = entry.getKey();
      if (key < 10) {
        conMap.remove(key);
      }
    }

    for (Map.Entry<Long, String> entry : conMap.entrySet()) {
      System.out.println(entry.getKey() + " " + entry.getValue());
    }

    final Map<Long, Set<Long>> whichHaThisVolumeToReportMap = new ConcurrentHashMap<>();
    Set<Long> set1 = new HashSet<>();
    set1.add(10000L);
    set1.add(10001L);

    Set<Long> set2 = new HashSet<>();
    set2.add(20000L);
    set2.add(20001L);

    whichHaThisVolumeToReportMap.put(1L, set1);
    whichHaThisVolumeToReportMap.put(2L, set2);

    logger.warn("the value :{}", whichHaThisVolumeToReportMap);

    Map<Long, Long> instanceIdListMap = new HashMap<>();
    instanceIdListMap.put(1L, 10000L);
    instanceIdListMap.put(1L, 10001L);

    Map<Long, Set<Long>> volumeIdToInstanceIds = new HashMap<>();
    for (Map.Entry<Long, Set<Long>> entry : whichHaThisVolumeToReportMap.entrySet()) {
      Long infoCenterId = entry.getKey();
      if (instanceIdListMap.get(infoCenterId) == null) {
        //the instance may be dead
        whichHaThisVolumeToReportMap.remove(infoCenterId);
        continue;
      }

      for (Long volumeId : entry.getValue()) {
        Set<Long> availableHa = new HashSet<>();
        availableHa.add(infoCenterId);

        Iterator<Long> iterator = availableHa.iterator();
        while (iterator.hasNext()) {
          Long instanceId = iterator.next();
          if (instanceIdListMap.get(instanceId) == null) {
            //the instance may be dead
            iterator.remove();
          }
        }

        //if can not get the report HA, report to master
        if (availableHa.size() == 0) {
          logger.warn("i come in");
        }
      }
    }

    logger.warn("the end value :{}", whichHaThisVolumeToReportMap);
  }

  /**
   * just test for check some.
   */
  @Ignore
  @Test
  public void test2() throws Exception {
    PeriodicWorkExecutorImpl periodicWorkExecutor = volumeSweeperExecutor();
    periodicWorkExecutor.start();

    Thread.sleep(20000);
  }


  public PeriodicWorkExecutorImpl volumeSweeperExecutor() throws Exception {
    PeriodicWorkExecutorImpl sweeperExecutor = new PeriodicWorkExecutorImpl(
        volumeSweeperExecutionOptionsReader(),
        new TestFact(), "Volume-Sweeper");
    return sweeperExecutor;
  }

  public ExecutionOptionsReader volumeSweeperExecutionOptionsReader() {
    return new ExecutionOptionsReader(1, 1, 100, null);
  }

  @Ignore
  @Test
  public void test() {
    long date = 12;
    long updateVolumesStatusTrigger = 60;
    long updateVolumesStatusForNotReport = 40;
    logger.warn("get the result:{}", TimeUnit.DAYS.toSeconds(date));
    logger.warn("get the result:{}", TimeUnit.SECONDS.toMillis(10));

    long step = TimeUnit.MILLISECONDS.toSeconds(1500);
    logger.warn("in VolumeSweeper,get the step:{}", step);
    if (step > 1) {
      updateVolumesStatusTrigger = updateVolumesStatusTrigger / step;
      updateVolumesStatusForNotReport = updateVolumesStatusForNotReport / step;
    }

    logger.warn("the updateVolumesStatusForNotReport:{}, updateVolumesStatusTrigger:{}",
        updateVolumesStatusForNotReport, updateVolumesStatusTrigger);
  }

  class TestFact implements WorkerFactory {

    private TestFactSweeper worker;

    @Override
    public Worker createWorker() {
      if (worker == null) {
        worker = new TestFactSweeper();
      }

      return worker;

    }
  }

  class TestFactSweeper implements Worker {

    AtomicLong atomicLong = new AtomicLong(0);

    @Override
    public void doWork() throws Exception {
      int i = 2;

      Validate.isTrue(false,
          "invalid segment form. the value is empty" + " , at:" + atomicLong.incrementAndGet());


    }
  }
}
