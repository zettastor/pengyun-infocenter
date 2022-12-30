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

package py.infocenter.volumemanager;


import java.util.ArrayList;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.test.TestBase;
import py.thrift.share.VolumeInReportExceptionThrift;
import py.thrift.share.VolumeInUpdateActionExceptionThrift;


public class LockForSaveVolumeInfoTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(LockForSaveVolumeInfoTest.class);
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;
  private long volumeId = 111111L;
  private ArrayList<TException> exceptions;

  public LockForSaveVolumeInfoTest() {
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();
    exceptions = new ArrayList<>();
  }


  @Test
  public void testlockvolumeInSnapshotCreatingNowVolumeInReportInfo()
      throws InterruptedException {

    /*1. volume in VOLUME_UPDATE_ACTION **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_UPDATE_ACTION);
        } catch (TException e) {
          logger.warn("------ thread 1 find error:{}", e);
        }
      }

    }.start();

    Thread.sleep(1000);

    /*2. volume in VOLUME_IN_REPORT_INFO, but lock by VOLUME_UPDATE_ACTION, throw exception **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
        } catch (TException e) {
          logger.warn("------ thread 2 find error:{}", e);
          exceptions.add(e);
        }
      }

    }.start();

    Thread.sleep(2000);
    Assert.assertTrue(exceptions.size() == 1);
    Assert.assertTrue(exceptions.get(0) instanceof VolumeInUpdateActionExceptionThrift);
  }

  @Test
  public void testlockvolumeInVolumeInReportInfoNowSnapshotCreatingCountLarge()
      throws InterruptedException {

    /*1. volume in VOLUME_IN_REPORT_INFO **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
        } catch (TException e) {
          logger.warn("------ thread 1 find error:{}", e);
        }
      }

    }.start();

    Thread.sleep(1000);

    /*2. volume in VOLUME_UPDATE_ACTION, but lock by VOLUME_IN_REPORT_INFO, try to get,the count > 3
     * unlock to get it  **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_UPDATE_ACTION);
        } catch (TException e) {
          logger.warn("------ thread 2 find error:{}", e);
          exceptions.add(e);
        }
      }

    }.start();

    Thread.sleep(2000);
    Assert.assertTrue(exceptions.size() == 1);
  }

  @Test
  public void testlockvolumeInVolumeInReportInfoNowSnapshotCreatingCoutNormal()
      throws InterruptedException {

    /*1. volume in VOLUME_IN_REPORT_INFO **/
    final LockForSaveVolumeInfo.VolumeLockInfo[] lock = new LockForSaveVolumeInfo.VolumeLockInfo[1];
    new Thread() {
      public void run() {
        try {
          lock[0] = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
          Thread.sleep(1300);

          //VOLUME_IN_REPORT_INFO ok, unlock lock, the SNAPSHOT_CREATING get lock
          lock[0].setVolumeOperation(LockForSaveVolumeInfo.BusySource.LOCK_WAITING);
          lock[0].unlock();

        } catch (TException e) {
          logger.warn("------ thread 1 find error:{}", e);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }.start();

    Thread.sleep(1000);

    /*2. volume in VOLUME_UPDATE_ACTION, but lock by VOLUME_IN_REPORT_INFO, try to get,the count > 3
     * get it  **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_UPDATE_ACTION);
        } catch (TException e) {
          logger.warn("------ thread 2 find error:{}", e);
          exceptions.add(e);
        }
      }

    }.start();

    //VOLUME_IN_REPORT_INFO ok, unlock lock, the SNAPSHOT_CREATING get lock

    Thread.sleep(2000);
    Assert.assertTrue(exceptions.size() == 0);
  }

  @Test
  public void testlockvolumeInVolumeInReportInfoNowVolumeUpdateAction()
      throws InterruptedException {
    /*1. volume in VOLUME_IN_REPORT_INFO **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
        } catch (TException e) {
          logger.warn("------ thread 1 find error:{}", e);
        }
      }

    }.start();

    Thread.sleep(1000);

    /*2. volume in VOLUME_UPDATE_ACTION, but lock by VOLUME_IN_REPORT_INFO, when try 3 count,
     * can not get it
     *  throw exception **/
    new Thread() {
      public void run() {
        LockForSaveVolumeInfo.VolumeLockInfo lock;
        try {
          lock = lockForSaveVolumeInfo
              .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_UPDATE_ACTION);
        } catch (TException e) {
          logger.warn("------ thread 2 find error:{}", e);
          exceptions.add(e);
        }
      }

    }.start();

    Thread.sleep(2000);
    Assert.assertTrue(exceptions.size() == 1);
    Assert.assertTrue(exceptions.get(0) instanceof VolumeInReportExceptionThrift);
  }

  @After
  public void clean() {
    exceptions.clear();
    lockForSaveVolumeInfo.removeLockByVolumeId(volumeId);
  }

  class MyThread extends Thread {

    @Override
    public void run() {
      LockForSaveVolumeInfo.VolumeLockInfo lock;
      try {
        lock = lockForSaveVolumeInfo
            .getVolumeLockInfo(volumeId, LockForSaveVolumeInfo.BusySource.VOLUME_IN_REPORT_INFO);
      } catch (TException e) {
        logger.warn("------ thread 1 find error:{}", e);
      }
    }
  }
}
