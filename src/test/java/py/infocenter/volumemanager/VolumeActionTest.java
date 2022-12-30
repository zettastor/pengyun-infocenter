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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static py.volume.VolumeInAction.CREATING;
import static py.volume.VolumeInAction.NULL;
import static py.volume.VolumeMetadata.VolumeSourceType.CREATE_VOLUME;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import py.icshare.DomainStore;
import py.icshare.Operation;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.instance.manger.VolumeInformationManger;
import py.infocenter.job.UpdateOperationProcessorImpl;
import py.infocenter.rebalance.SegmentUnitsDistributionManagerImpl;
import py.infocenter.service.LockForSaveVolumeInfo;
import py.infocenter.store.DbVolumeStoreImpl;
import py.infocenter.store.MemoryVolumeStoreImpl;
import py.infocenter.store.StorageStore;
import py.infocenter.store.TwoLevelVolumeStoreImpl;
import py.infocenter.store.control.OperationStore;
import py.infocenter.test.utils.TestBeans;
import py.infocenter.worker.VolumeActionSweeper;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.InstanceStatus;
import py.test.TestBase;
import py.volume.CacheType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeRebalanceInfo;
import py.volume.VolumeStatus;
import py.volume.VolumeType;


public class VolumeActionTest extends TestBase {

  @Mock
  SegmentUnitsDistributionManagerImpl segmentUnitsDistributionManager;
  @Autowired
  private SessionFactory sessionFactory;
  private VolumeActionSweeper volumeActionSweeper;
  @Mock
  private DbVolumeStoreImpl dbVolumeStore;
  @Mock
  private StorageStore storageStore;
  @Mock
  private StoragePoolStore storagePoolStore;
  @Mock
  private DomainStore domainStore;

  private TwoLevelVolumeStoreImpl volumeStore;

  private long segmentSize = 100;

  private UpdateOperationProcessorImpl updateOperationProcessor;

  private VolumeInformationManger volumeInformationManger;
  private LockForSaveVolumeInfo lockForSaveVolumeInfo;

  @Mock
  private Operation operation;

  @Mock
  private OperationStore operationStore;

  
  @Before
  public void setup() throws IOException, SQLException {
    InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);
    final ApplicationContext ctx = new AnnotationConfigApplicationContext(TestBeans.class);

    volumeInformationManger = new VolumeInformationManger();
    volumeActionSweeper = new VolumeActionSweeper();
    volumeStore = new TwoLevelVolumeStoreImpl(new MemoryVolumeStoreImpl(), dbVolumeStore);

    //not about
    volumeInformationManger.setVolumeStore(dbVolumeStore);

    volumeActionSweeper.setVolumeStore(volumeStore);
    volumeActionSweeper.setVolumeInformationManger(volumeInformationManger);

    //lockForSaveVolumeInfo
    lockForSaveVolumeInfo = new LockForSaveVolumeInfo();

    //action
    volumeActionSweeper.setAppContext(appContext);
    volumeActionSweeper.setStoragePoolStore(storagePoolStore);
    volumeActionSweeper.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    //volumeInformationManger
    volumeInformationManger = new VolumeInformationManger();
    volumeInformationManger.setVolumeStore(volumeStore);

    //updateOperationProcessor
    updateOperationProcessor = new UpdateOperationProcessorImpl();
    updateOperationProcessor.setVolumeInformationManger(volumeInformationManger);
    updateOperationProcessor.setOperationStore(operationStore);
    updateOperationProcessor.setLockForSaveVolumeInfo(lockForSaveVolumeInfo);

    StoragePool storagePool = new StoragePool();
    when(storagePoolStore.getStoragePool(anyLong())).thenReturn(storagePool);

    VolumeRebalanceInfo rebalanceRatioInfo = new VolumeRebalanceInfo();
    when(segmentUnitsDistributionManager.getRebalanceProgressInfo(anyLong()))
        .thenReturn(rebalanceRatioInfo);
  }

  @Test
  public void testVolumeNormalAction() throws Exception {
    long volumeId = 12341;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1, CREATING);
    volume.setVolumeStatus(VolumeStatus.Creating);
    volumeStore.saveVolume(volume);

    // VolumeStatus.Creating
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), CREATING);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Unavailable
    volume.setVolumeStatus(VolumeStatus.Unavailable);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Deleting
    volume.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.DELETING);

    // VolumeStatus.Deleted
    volume.setVolumeStatus(VolumeStatus.Deleted);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Dead
    volume.setVolumeStatus(VolumeStatus.Dead);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

  }

  @Test
  public void testVolumeNormalActionStill() throws Exception {
    long volumeId = 12341;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1, CREATING);
    volume.setVolumeStatus(VolumeStatus.Creating);
    volumeStore.saveVolume(volume);

    // VolumeStatus.Creating
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), CREATING);

    // VolumeStatus.Creating still
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), CREATING);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Available still
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Unavailable
    volume.setVolumeStatus(VolumeStatus.Unavailable);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Unavailable still
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Available still
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Deleting
    volume.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.DELETING);

    // VolumeStatus.Deleting still
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.DELETING);

    // VolumeStatus.Deleted
    volume.setVolumeStatus(VolumeStatus.Deleted);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Deleted still
    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Dead
    volume.setVolumeStatus(VolumeStatus.Dead);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

  }

  @Test
  public void testVolumeRecycleOkAction() throws Exception {
    long volumeId = 12341;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1, CREATING);
    volume.setVolumeStatus(VolumeStatus.Creating);
    volumeStore.saveVolume(volume);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Deleting
    volume.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.DELETING);

    // VolumeStatus.Recycling
    volume.setVolumeStatus(VolumeStatus.Recycling);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.RECYCLING);

    // VolumeStatus.Recycling still
    volume.setVolumeStatus(VolumeStatus.Recycling);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.RECYCLING);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

  }

  @Test
  public void testVolumeRecycleFailedAction() throws Exception {
    long volumeId = 12341;
    VolumeMetadata volume = generateVolumeMetadata(volumeId, null, 6, 3, 1, CREATING);
    volume.setVolumeStatus(VolumeStatus.Creating);
    volumeStore.saveVolume(volume);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Available);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);

    // VolumeStatus.Deleting
    volume.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.DELETING);

    // VolumeStatus.Recycling
    volume.setVolumeStatus(VolumeStatus.Recycling);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.RECYCLING);

    // VolumeStatus.Recycling still
    volume.setVolumeStatus(VolumeStatus.Recycling);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.RECYCLING);

    // VolumeStatus.Available
    volume.setVolumeStatus(VolumeStatus.Dead);
    volumeStore.saveVolume(volume);

    volumeActionSweeper.doWork();
    volume = volumeStore.getVolume(volumeId);
    Assert.assertEquals(volume.getInAction(), VolumeInAction.NULL);
  }

  //extend volume
  @Test
  public void testExtendVolumeOkAction() throws Exception {
    long extendVolumeId = 12341;
    //extendVolumeId
    VolumeMetadata extendVolume = generateVolumeMetadata(extendVolumeId, null, 6, 3, 1, NULL);
    extendVolume.setVolumeStatus(VolumeStatus.Available);
    extendVolume.setExtendingSize(1);
    extendVolume.setInAction(VolumeInAction.EXTENDING);
    volumeStore.saveVolume(extendVolume);

    //update action
    volumeActionSweeper.doWork();
    Assert.assertEquals(extendVolume.getInAction(), VolumeInAction.EXTENDING);

    //the extend ok
    extendVolume.setExtendingSize(0);
    volumeStore.saveVolume(extendVolume);

    //update action
    volumeActionSweeper.doWork();
    extendVolume = volumeStore.getVolume(extendVolumeId);
    Assert.assertEquals(extendVolume.getInAction(), VolumeInAction.NULL);
  }

  @Test
  public void testExtendVolumeDeleteByUser() throws Exception {
    long extendVolumeId = 12341;
    //extendVolumeId
    VolumeMetadata extendVolume = generateVolumeMetadata(extendVolumeId, null, 6, 3, 1, NULL);
    extendVolume.setVolumeStatus(VolumeStatus.Available);
    extendVolume.setExtendingSize(1);
    extendVolume.setInAction(VolumeInAction.EXTENDING);
    volumeStore.saveVolume(extendVolume);

    //update action
    volumeActionSweeper.doWork();
    Assert.assertEquals(extendVolume.getInAction(), VolumeInAction.EXTENDING);

    //the volume Delete By User
    extendVolume.setVolumeStatus(VolumeStatus.Deleting);
    volumeStore.saveVolume(extendVolume);

    //update action
    volumeActionSweeper.doWork();
    extendVolume = volumeStore.getVolume(extendVolumeId);
    Assert.assertEquals(extendVolume.getInAction(), VolumeInAction.DELETING);
  }

  @Test
  public void testJava() {
    long extendVolumeId = 12341;
    long extendVolumeId2 = 12342;
    VolumeMetadata volumeMetadata1 = generateVolumeMetadata(extendVolumeId, null, 6, 3, 1, NULL);
    VolumeMetadata volumeMetadata2 = generateVolumeMetadata(extendVolumeId2, null, 6, 3, 1, NULL);

    List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
    volumeMetadataList.add(volumeMetadata1);
    volumeMetadataList.add(volumeMetadata2);

    volumeMetadataList.forEach((v) -> logger
        .warn("the value :{} {} {}", v.getVolumeId(), v.getName(), v.getVolumeStatus(),
            v.getInAction()));
    volumeMetadataList.stream().map(v -> v.getVolumeId());

    List<PrintLog> longList = new ArrayList<>();
    volumeMetadataList.forEach((v) -> longList
        .add(new PrintLog(v.getVolumeId(), v.getVolumeSize(), v.getExtendingSize(), v.getName(),
            v.getVolumeStatus(), v.getInAction(), v.getReadWrite())));
    logger.warn("--- :{}", longList);
  }


  @Test
  public void testException() {
    Exception exception = new Exception();
    logger.warn("--- ", exception);
  }

  private VolumeMetadata generateVolumeMetadata(long volumeId, Long childVolumeId, long volumeSize,
      long segmentSize,
      int version, VolumeInAction action) {
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId, volumeSize, segmentSize,
        VolumeType.REGULAR, 0L, 0L);
    volumeMetadata.setChildVolumeId(childVolumeId);
    volumeMetadata.setPositionOfFirstSegmentInLogicVolume(0);
    volumeMetadata.setName("testvolume");
    volumeMetadata.setVolumeSource(CREATE_VOLUME);
    volumeMetadata.setInAction(action);
    volumeMetadata.setVolumeLayout("test");
    while (version-- > 0) {
      volumeMetadata.incVersion();
    }

    return volumeMetadata;
  }

  class PrintLog {

    private String name;
    private long volumeId;
    private VolumeStatus volumeStatus;
    private VolumeInAction inAction;
    private VolumeMetadata.ReadWriteType readWrite;
    private long volumeSize;
    private long extendingSize;

    public PrintLog(long volumeId, long volumeSize, long extendingSize, String name,
        VolumeStatus volumeStatus, VolumeInAction inAction,
        VolumeMetadata.ReadWriteType readWrite) {
      this.volumeId = volumeId;
      this.volumeSize = volumeSize;
      this.extendingSize = extendingSize;
      this.name = name;
      this.volumeStatus = volumeStatus;
      this.inAction = inAction;
      this.readWrite = readWrite;
    }

    @Override
    public String toString() {
      return "PrintLog{"
          + "name='" + name + '\''
          + ", volumeId=" + volumeId
          + ", volumeStatus=" + volumeStatus
          + ", inAction=" + inAction
          + ", readWrite=" + readWrite
          + ", volumeSize=" + volumeSize
          + ", extendingSize=" + extendingSize
          + '}';
    }
  }

}