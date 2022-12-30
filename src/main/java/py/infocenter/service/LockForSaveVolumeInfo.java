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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.thrift.share.DriverLaunchingExceptionThrift;
import py.thrift.share.DriverUnmountingExceptionThrift;
import py.thrift.share.VolumeCyclingExceptionThrift;
import py.thrift.share.VolumeDeletingExceptionThrift;
import py.thrift.share.VolumeExtendExceptionThrift;
import py.thrift.share.VolumeFixingOperationExceptionThrift;
import py.thrift.share.VolumeInReportExceptionThrift;
import py.thrift.share.VolumeInUpdateActionExceptionThrift;
import py.thrift.share.VolumeMarkReadWriteExceptionThrift;
import py.thrift.share.VolumeUnderOperationExceptionThrift;


public class LockForSaveVolumeInfo {

  private static final Logger logger = LoggerFactory.getLogger(LockForSaveVolumeInfo.class);
 
  private Map<Long, VolumeLockInfo> volumeLockInfoMap;

  public LockForSaveVolumeInfo() {
    volumeLockInfoMap = new HashMap<>();
  }

  public synchronized VolumeLockInfo createLockForVolume(long volumeId) {
    VolumeLockInfo lock = volumeLockInfoMap.get(volumeId);
    if (lock == null) {
      lock = new VolumeLockInfo();
      volumeLockInfoMap.put(volumeId, lock);
    }
    return lock;
  }

  public VolumeLockInfo getLockByVolumeId(long volumeId) {
    VolumeLockInfo lock = volumeLockInfoMap.get(volumeId);
    if (lock == null) {
      lock = createLockForVolume(volumeId);
    }
    return lock;
  }

  public synchronized void removeLockByVolumeId(long volumeId) {
    VolumeLockInfo lock = volumeLockInfoMap.get(volumeId);
    if (lock == null) {
      return;
    }
    volumeLockInfoMap.remove(volumeId);
  }

  
  public VolumeLockInfo getVolumeLockInfo(long volumeId,
      LockForSaveVolumeInfo.BusySource busySource)
      throws TException {
    LockForSaveVolumeInfo.VolumeLockInfo lock = getLockByVolumeId(volumeId);
    AtomicInteger count = new AtomicInteger(0);
    while (count.get() < 3) {
      if (!lock.tryLock()) {
        //current volume in report a
        TException exception = lock.getOperation().getException();
        if (exception instanceof VolumeInReportExceptionThrift
            || exception instanceof VolumeInUpdateActionExceptionThrift) {
          try {
            count.incrementAndGet();
            Thread.sleep(300);
            logger.warn(
                "when Volume LockInfo for :{},the current volume {} is in :{}, and count is :{}",
                busySource, volumeId, exception, count.get());
          } catch (InterruptedException e) {
            logger.warn("when Volume LockInfo for :{},the volume {} sleep error", busySource,
                volumeId);
          }
        } else {
          //print log
          if (busySource.equals(BusySource.VOLUME_IN_REPORT_INFO) || busySource
              .equals(BusySource.VOLUME_UPDATE_ACTION)) {
            logger.warn("when Volume LockInfo for :{}, the volume: {} Locked by :{} !", busySource,
                volumeId, exception);
          } else {
            logger.error("when Volume LockInfo for :{}, the volume: {} Locked by :{} !", busySource,
                volumeId, exception);
          }

          throw exception;
        }
      } else {
        break;
      }
    }

    /* if try 3 time and can not get the lock, if lock by (UPDATE_ACTION or REPORT_INFO)
     * and want to get BusySource is not VOLUME_UPDATE_ACTION or VOLUME_IN_REPORT_INFO
     * unlock the lock for BusySource
     */
    if (count.get() == 3) {
      if (!busySource.equals(BusySource.VOLUME_UPDATE_ACTION) && !busySource
          .equals(BusySource.VOLUME_IN_REPORT_INFO)) {
        logger.warn(
            "when Volume LockInfo for :{},the current volume {} is in :{} more the 1 second, "
                + "unlock it",
            busySource, volumeId, lock.getOperation().getException());
        lock.unlock();

        //try again
        if (!lock.tryLock()) {
          logger.error("the end,when Volume LockInfo for :{}, the volume: {} Locked by :{} !",
              busySource, volumeId, lock.getOperation().getException());
          throw lock.getOperation().getException();
        }
      } else {
        // all in VOLUME_UPDATE_ACTION or VOLUME_IN_REPORT_INFO,
        throw lock.getOperation().getException();
      }
    }

    //
    lock.setVolumeOperation(busySource);
    return lock;
  }

  
  public enum BusySource {
    DRIVER_LAUNCHING {
      @Override
      public TException getException() {
        return new DriverLaunchingExceptionThrift();
      }
    }, DRIVER_UMOUNTING {
      @Override
      public TException getException() {
        return new DriverUnmountingExceptionThrift();
      }
    }, VOLUME_DELETING {
      @Override
      public TException getException() {
        return new VolumeDeletingExceptionThrift();
      }
    }, VOLUME_CYCLING {
      @Override
      public TException getException() {
        return new VolumeCyclingExceptionThrift();
      }
    }, LOCK_WAITING {
      @Override
      public TException getException() {
        return new VolumeUnderOperationExceptionThrift();
      }
    }, VOLUME_FIXTING {
      @Override
      public TException getException() {
        return new VolumeFixingOperationExceptionThrift();
      }
    }, VOLUME_MARKING_READ_WRITE {
      @Override
      public TException getException() {
        return new VolumeMarkReadWriteExceptionThrift();
      }
    }, VOLUME_EXTEND {
      @Override
      public TException getException() {
        return new VolumeExtendExceptionThrift();
      }
    }, VOLUME_UPDATE_ACTION {
      @Override
      public TException getException() {
        return new VolumeInUpdateActionExceptionThrift();
      }
    }, VOLUME_CREATE_SEGMENTS {

    }, VOLUME_IN_REPORT_INFO {
      @Override
      public TException getException() {
        return new VolumeInReportExceptionThrift();
      }
    };

    public TException getException() {
      return new VolumeUnderOperationExceptionThrift();
    }
  }

  
  public class VolumeLockInfo extends ReentrantLock {

    private BusySource operation = BusySource.LOCK_WAITING;

    public BusySource getOperation() {
      return operation;
    }

    public void setVolumeOperation(BusySource operation) {
      this.operation = operation;
    }

    public String toString() {
      return super.toString() + "operation: " + operation.name();
    }
  }

}
