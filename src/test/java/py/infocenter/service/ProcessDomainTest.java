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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.ArchiveStatus;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.icshare.Domain;
import py.icshare.DomainStore;
import py.icshare.InstanceMetadata;
import py.icshare.authorization.PyResource;
import py.infocenter.InfoCenterAppContext;
import py.infocenter.authorization.PySecurityManager;
import py.infocenter.store.StorageStore;
import py.infocenter.store.VolumeStore;
import py.infocenter.store.control.OperationStore;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.test.TestBase;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.CreateDomainRequest;
import py.thrift.share.DatanodeNotFoundExceptionThrift;
import py.thrift.share.DatanodeNotFreeToUseExceptionThrift;
import py.thrift.share.DeleteDomainRequest;
import py.thrift.share.DomainExistedExceptionThrift;
import py.thrift.share.DomainNameExistedExceptionThrift;
import py.thrift.share.DomainNotExistedExceptionThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.ListDomainRequest;
import py.thrift.share.ListDomainResponse;
import py.thrift.share.RemoveDatanodeFromDomainRequest;
import py.thrift.share.StatusThrift;
import py.thrift.share.StillHaveStoragePoolExceptionThrift;
import py.thrift.share.UpdateDomainRequest;


public class ProcessDomainTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ProcessDomainTest.class);

  @Mock
  private StorageStore storageStore;
  @Mock
  private VolumeStore volumeStore;
  @Mock
  private DomainStore domainStore;
  @Mock
  private StoragePoolStore storagePoolStore;

  @Mock
  private PySecurityManager securityManager;

  @Mock
  private InstanceStore instanceStore;
  @Mock
  private Instance instance;

  @Mock
  private OperationStore operationStore;

  private InformationCenterImpl informationCenter;

  @Mock
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory;


  @Before
  public void init() throws Exception {
    super.init();
    InfoCenterAppContext appContext = mock(InfoCenterAppContext.class);
    when(appContext.getStatus()).thenReturn(InstanceStatus.HEALTHY);

    informationCenter = new InformationCenterImpl();
    informationCenter.setStorageStore(storageStore);
    informationCenter.setVolumeStore(volumeStore);
    informationCenter.setDomainStore(domainStore);
    informationCenter.setStoragePoolStore(storagePoolStore);
    informationCenter.setAppContext(appContext);
    informationCenter.setSecurityManager(securityManager);
    informationCenter.setInstanceStore(instanceStore);
    informationCenter.setOperationStore(operationStore);
    informationCenter.setDataNodeClientFactory(dataNodeClientFactory);

    when(securityManager.hasPermission(anyLong(), anyString())).thenReturn(true);
  }

  @Test
  public void testCreateDomainSucess() throws TException {
    CreateDomainRequest request = new CreateDomainRequest();
    request.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    domainThrift.setDomainId(RequestIdBuilder.get());
    domainThrift.setDomainName(TestBase.getRandomString(10));
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    request.setDomain(domainThrift);

    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(true);
    informationCenter.createDomain(request);

    Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));
  }

  @Test
  public void testCreateDomainThrowException_SameNameAndId() throws TException, Exception {
    CreateDomainRequest requestSucess = new CreateDomainRequest();
    requestSucess.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    Long sucessDomainId = RequestIdBuilder.get();
    String successDomainName = TestBase.getRandomString(10);
    domainThrift.setDomainId(sucessDomainId);
    domainThrift.setDomainName(successDomainName);
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);
    requestSucess.setDomain(domainThrift);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(true);
    informationCenter.createDomain(requestSucess);

    Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

    // domain name too long
    CreateDomainRequest requestNameTooLong = new CreateDomainRequest();
    requestNameTooLong.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift1 = new DomainThrift();
    domainThrift1.setDomainId(RequestIdBuilder.get());
    domainThrift1.setDomainName(TestBase.getRandomString(101));
    domainThrift1.setDomainDescription(TestBase.getRandomString(20));
    domainThrift1.setStatus(StatusThrift.Available);
    domainThrift1.setLastUpdateTime(System.currentTimeMillis());
    domainThrift1.setDatanodes(datanodes);
    requestNameTooLong.setDomain(domainThrift1);
    boolean caughtNameException = false;
    try {
      informationCenter.createDomain(requestNameTooLong);
    } catch (InvalidInputExceptionThrift e) {
      caughtNameException = true;
    }
    assertTrue(caughtNameException);

    List<Domain> allDomains = new ArrayList<Domain>();
    allDomains.add(RequestResponseHelper.buildDomainFrom(domainThrift));
    when(domainStore.listAllDomains()).thenReturn(allDomains);

    // domain name exist
    CreateDomainRequest requestNameExist = new CreateDomainRequest();
    requestNameExist.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift2 = new DomainThrift();
    domainThrift2.setDomainId(RequestIdBuilder.get());
    domainThrift2.setDomainName(successDomainName);
    domainThrift2.setDomainDescription(TestBase.getRandomString(20));
    domainThrift2.setDatanodes(datanodes);
    domainThrift2.setStatus(StatusThrift.Available);
    domainThrift2.setLastUpdateTime(System.currentTimeMillis());
    requestNameExist.setDomain(domainThrift2);
    boolean caughtNameExistException = false;
    try {
      informationCenter.createDomain(requestNameExist);
    } catch (DomainNameExistedExceptionThrift e) {
      caughtNameExistException = true;
    }
    assertTrue(caughtNameExistException);

    // domain Id exist
    CreateDomainRequest requestDomainExist = new CreateDomainRequest();
    requestDomainExist.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift3 = new DomainThrift();
    domainThrift3.setDomainId(sucessDomainId);
    domainThrift3.setDomainName(TestBase.getRandomString(99));
    domainThrift3.setDomainDescription(TestBase.getRandomString(20));
    domainThrift3.setDatanodes(datanodes);
    domainThrift3.setStatus(StatusThrift.Available);
    domainThrift3.setLastUpdateTime(System.currentTimeMillis());
    requestDomainExist.setDomain(domainThrift3);
    boolean caughtDomainExistException = false;
    try {
      informationCenter.createDomain(requestDomainExist);
    } catch (DomainExistedExceptionThrift e) {
      caughtDomainExistException = true;
    }
    assertTrue(caughtDomainExistException);
  }

  @Test
  public void testCreateDomainThrowException2() throws TException {
    CreateDomainRequest requestNotFree = new CreateDomainRequest();
    requestNotFree.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    domainThrift.setDomainId(RequestIdBuilder.get());
    domainThrift.setDomainName(TestBase.getRandomString(10));
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);
    requestNotFree.setDomain(domainThrift);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(false);
    boolean caughtNotFreeException = false;
    try {
      informationCenter.createDomain(requestNotFree);
    } catch (DatanodeNotFreeToUseExceptionThrift e) {
      caughtNotFreeException = true;
    }
    assertTrue(caughtNotFreeException);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(null);
    }
    boolean caughtNotFoundException = false;
    try {
      informationCenter.createDomain(requestNotFree);
    } catch (DatanodeNotFoundExceptionThrift e) {
      caughtNotFoundException = true;
    }
    assertTrue(caughtNotFoundException);
  }

  @Test
  public void testUpdateDomainSuccess() throws TException, Exception {
    // create first
    CreateDomainRequest requestSucess = new CreateDomainRequest();
    requestSucess.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    Long sucessDomainId = RequestIdBuilder.get();
    String successDomainName = TestBase.getRandomString(10);
    domainThrift.setDomainId(sucessDomainId);
    domainThrift.setDomainName(successDomainName);
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);
    requestSucess.setDomain(domainThrift);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(true);
    informationCenter.createDomain(requestSucess);

    Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

    when(domainStore.getDomain(any(Long.class)))
        .thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));
    // then update
    UpdateDomainRequest updateSucess = new UpdateDomainRequest();
    updateSucess.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift1 = new DomainThrift();
    domainThrift1.setDomainId(sucessDomainId);
    domainThrift1.setDomainName(TestBase.getRandomString(10));
    domainThrift1.setDomainDescription(TestBase.getRandomString(20));
    domainThrift1.setDatanodes(datanodes);
    domainThrift1.setStatus(StatusThrift.Available);
    domainThrift1.setLastUpdateTime(System.currentTimeMillis());
    updateSucess.setDomain(domainThrift1);

    informationCenter.updateDomain(updateSucess);

    Mockito.verify(domainStore, Mockito.times(2)).saveDomain(any(Domain.class));
  }

  @Test
  public void testUpdateDomainThrowException() throws TException, Exception {
    // create first
    CreateDomainRequest requestSucess = new CreateDomainRequest();
    requestSucess.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    Long sucessDomainId = RequestIdBuilder.get();
    String successDomainName = TestBase.getRandomString(10);
    domainThrift.setDomainId(sucessDomainId);
    domainThrift.setDomainName(successDomainName);
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);
    requestSucess.setDomain(domainThrift);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(true);
    informationCenter.createDomain(requestSucess);

    Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

    when(domainStore.getDomain(any(Long.class)))
        .thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));
    // then update
    UpdateDomainRequest updateNotFree = new UpdateDomainRequest();
    updateNotFree.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift1 = new DomainThrift();
    domainThrift1.setDomainId(sucessDomainId);
    domainThrift1.setDomainName(TestBase.getRandomString(10));
    domainThrift1.setDomainDescription(TestBase.getRandomString(20));
    domainThrift1.setStatus(StatusThrift.Available);
    domainThrift1.setLastUpdateTime(System.currentTimeMillis());
    Long addDatanodeId = RequestIdBuilder.get();
    datanodes.add(addDatanodeId);
    domainThrift1.setDatanodes(datanodes);
    updateNotFree.setDomain(domainThrift1);
    when(storageStore.get(addDatanodeId)).thenReturn(datanode);
    when(datanode.isFree()).thenReturn(false);

    boolean datanodeNotFreeException = false;
    try {
      informationCenter.updateDomain(updateNotFree);
    } catch (DatanodeNotFreeToUseExceptionThrift e) {
      datanodeNotFreeException = true;
    }
    assertTrue(datanodeNotFreeException);

    // then update, but datanode not found
    UpdateDomainRequest updateNotFound = new UpdateDomainRequest();
    updateNotFound.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift2 = new DomainThrift();
    domainThrift2.setDomainId(sucessDomainId);
    domainThrift2.setDomainName(TestBase.getRandomString(10));
    domainThrift2.setDomainDescription(TestBase.getRandomString(20));
    domainThrift2.setStatus(StatusThrift.Available);
    domainThrift2.setLastUpdateTime(System.currentTimeMillis());
    Long addDatanodeId2 = RequestIdBuilder.get();
    datanodes.add(addDatanodeId2);
    domainThrift2.setDatanodes(datanodes);
    updateNotFound.setDomain(domainThrift2);
    when(storageStore.get(addDatanodeId2)).thenReturn(null);
    when(datanode.isFree()).thenReturn(true);
    when(instanceStore.get(any(InstanceId.class))).thenReturn(instance);
    when(instance.getEndPointByServiceName(PortType.CONTROL))
        .thenReturn(new EndPoint("10.0.0.80", 8081));

    boolean datanodeNotFoundException = false;
    try {
      informationCenter.updateDomain(updateNotFound);
    } catch (DatanodeNotFoundExceptionThrift e) {
      datanodeNotFoundException = true;
    }
    assertTrue(datanodeNotFoundException);

    when(domainStore.getDomain(any(Long.class))).thenReturn(null);
    // then update
    UpdateDomainRequest updateNotFoundDomain = new UpdateDomainRequest();
    updateNotFoundDomain.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift3 = new DomainThrift();
    domainThrift3.setDomainId(sucessDomainId);
    domainThrift3.setDomainName(TestBase.getRandomString(10));
    domainThrift3.setDomainDescription(TestBase.getRandomString(20));
    domainThrift3.setDatanodes(datanodes);
    domainThrift3.setStatus(StatusThrift.Available);
    domainThrift3.setLastUpdateTime(System.currentTimeMillis());
    updateNotFoundDomain.setDomain(domainThrift3);
    when(storageStore.get(addDatanodeId)).thenReturn(datanode);
    when(datanode.isFree()).thenReturn(true);

    boolean domainNotFoundException = false;
    try {
      informationCenter.updateDomain(updateNotFoundDomain);
    } catch (DomainNotExistedExceptionThrift e) {
      domainNotFoundException = true;
    }
    assertTrue(domainNotFoundException);
  }

  @Test
  public void testDeleteDomainThrowException() throws TException, Exception {
    // delete domain but not exist
    DeleteDomainRequest deleteRequest = new DeleteDomainRequest();
    deleteRequest.setRequestId(RequestIdBuilder.get());
    deleteRequest.setDomainId(RequestIdBuilder.get());

    boolean domainNotFoundException = false;
    try {
      informationCenter.deleteDomain(deleteRequest);
    } catch (DomainNotExistedExceptionThrift e) {
      domainNotFoundException = true;
    }
    assertTrue(domainNotFoundException);

    // create domain
    CreateDomainRequest requestSucess = new CreateDomainRequest();
    requestSucess.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    Long sucessDomainId = RequestIdBuilder.get();
    String successDomainName = TestBase.getRandomString(10);
    domainThrift.setDomainId(sucessDomainId);
    domainThrift.setDomainName(successDomainName);
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);
    requestSucess.setDomain(domainThrift);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(true);
    informationCenter.createDomain(requestSucess);

    Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));
    Domain domain = mock(Domain.class);
    when(domainStore.getDomain(any(Long.class))).thenReturn(domain);
    when(domain.hasStoragePool()).thenReturn(true);
    // delete domain, but still has storage pool
    DeleteDomainRequest deleteRequestStillHasStoragePool = new DeleteDomainRequest();
    deleteRequestStillHasStoragePool.setRequestId(RequestIdBuilder.get());
    deleteRequestStillHasStoragePool.setDomainId(sucessDomainId);

    boolean hasStoragePoolException = false;
    try {
      informationCenter.deleteDomain(deleteRequest);
    } catch (StillHaveStoragePoolExceptionThrift e) {
      hasStoragePoolException = true;
    }
    assertTrue(hasStoragePoolException);

    // delete again
    when(domain.hasStoragePool()).thenReturn(false);
    informationCenter.deleteDomain(deleteRequest);
    Mockito.verify(domainStore, Mockito.times(2)).saveDomain(any(Domain.class));
  }

  @Test
  public void testListDomain() throws TException, Exception {
    int domainCount = 10;
    List<Domain> allDomains = new ArrayList<Domain>();
    List<Domain> partOfDomains = new ArrayList<Domain>();
    List<Long> partOfDomainIds = new ArrayList<Long>();
    List<Long> allDomainIds = new ArrayList<Long>();
    for (int i = 0; i < domainCount; i++) {
      DomainThrift domainThrift = new DomainThrift();
      domainThrift.setDomainId(Long.valueOf(i));
      domainThrift.setDomainName(TestBase.getRandomString(10));
      domainThrift.setDomainDescription(TestBase.getRandomString(20));
      domainThrift.setStatus(StatusThrift.Available);
      domainThrift.setLastUpdateTime(System.currentTimeMillis());
      Set<Long> datanodes = buildIdSet(5);
      domainThrift.setDatanodes(datanodes);
      Domain domain = RequestResponseHelper.buildDomainFrom(domainThrift);
      allDomains.add(domain);
      allDomainIds.add(Long.valueOf(i));
      if (i % 2 == 0) {
        partOfDomainIds.add(Long.valueOf(i));
        partOfDomains.add(domain);
      }
    }

    //set the access to list
    Set<Long> accessibleResource = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      accessibleResource.add((long) i);
    }

    when(
        securityManager.getAccessibleResourcesByType(anyLong(), any(PyResource.ResourceType.class)))
        .thenReturn(accessibleResource);

    when(domainStore.listDomains(allDomainIds)).thenReturn(allDomains);
    when(domainStore.listDomains(partOfDomainIds)).thenReturn(partOfDomains);

    ListDomainRequest listAllDomainRequest = new ListDomainRequest();
    listAllDomainRequest.setRequestId(RequestIdBuilder.get());
    ListDomainResponse response = informationCenter.listDomains(listAllDomainRequest);
    assertTrue(response.getDomainDisplays().size() == domainCount);

    ListDomainRequest listPartOfDomainRequest = new ListDomainRequest();
    listPartOfDomainRequest.setRequestId(RequestIdBuilder.get());
    listPartOfDomainRequest.setDomainIds(partOfDomainIds);
    ListDomainResponse response1 = informationCenter.listDomains(listPartOfDomainRequest);
    assertTrue(response1.getDomainDisplays().size() == partOfDomains.size());
  }

  @Test
  public void testRemoveDatanodeFromDomain() throws TException, Exception {
    // create first
    CreateDomainRequest requestSucess = new CreateDomainRequest();
    requestSucess.setRequestId(RequestIdBuilder.get());
    DomainThrift domainThrift = new DomainThrift();
    Long sucessDomainId = RequestIdBuilder.get();
    String successDomainName = TestBase.getRandomString(10);
    domainThrift.setDomainId(sucessDomainId);
    domainThrift.setDomainName(successDomainName);
    domainThrift.setDomainDescription(TestBase.getRandomString(20));
    domainThrift.setStatus(StatusThrift.Available);
    domainThrift.setLastUpdateTime(System.currentTimeMillis());
    Set<Long> datanodes = buildIdSet(5);
    domainThrift.setDatanodes(datanodes);
    requestSucess.setDomain(domainThrift);

    InstanceMetadata datanode = mock(InstanceMetadata.class);

    for (Long datanodeId : datanodes) {
      when(storageStore.get(datanodeId)).thenReturn(datanode);
    }
    when(datanode.isFree()).thenReturn(true);
    informationCenter.createDomain(requestSucess);

    Mockito.verify(domainStore, Mockito.times(1)).saveDomain(any(Domain.class));

    when(domainStore.getDomain(any(Long.class)))
        .thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));

    // datanode not found
    Long notExistDatanodeId = RequestIdBuilder.get();
    when(storageStore.get(notExistDatanodeId)).thenReturn(null);

    RemoveDatanodeFromDomainRequest requestRemoveDatanode = new RemoveDatanodeFromDomainRequest();
    requestRemoveDatanode.setRequestId(RequestIdBuilder.get());
    requestRemoveDatanode.setDomainId(sucessDomainId);
    requestRemoveDatanode.setDatanodeInstanceId(notExistDatanodeId);
    boolean notFoundDatanodeException = false;
    try {
      informationCenter.removeDatanodeFromDomain(requestRemoveDatanode);
    } catch (DatanodeNotFoundExceptionThrift e) {
      notFoundDatanodeException = true;
    }
    assertTrue(notFoundDatanodeException);

    // domian not exist
    when(domainStore.getDomain(any(Long.class))).thenReturn(null);
    RemoveDatanodeFromDomainRequest requestDomainNotExist = new RemoveDatanodeFromDomainRequest();
    requestDomainNotExist.setRequestId(RequestIdBuilder.get());
    requestDomainNotExist.setDomainId(sucessDomainId);
    requestDomainNotExist.setDatanodeInstanceId((Long) datanodes.toArray()[0]);
    boolean domainNotExistException = false;
    try {
      informationCenter.removeDatanodeFromDomain(requestDomainNotExist);
    } catch (DomainNotExistedExceptionThrift e) {
      domainNotExistException = true;
    }
    assertTrue(domainNotExistException);

    // sucess delete datanode from domain
    Long removeDatanodeId = (Long) datanodes.toArray()[0];
    when(domainStore.getDomain(any(Long.class)))
        .thenReturn(RequestResponseHelper.buildDomainFrom(domainThrift));
    StoragePool storagePool = mock(StoragePool.class);
    List<StoragePool> storagePoolList = new ArrayList<StoragePool>();
    storagePoolList.add(storagePool);
    when(storagePoolStore.listAllStoragePools()).thenReturn(storagePoolList);
    Multimap<Long, Long> archivesInDataNode = buildMultiMap(5);
    List<RawArchiveMetadata> archives = new ArrayList<RawArchiveMetadata>();
    for (int i = 0; i < 5; i++) {
      RawArchiveMetadata archive = new RawArchiveMetadata();
      archive.setArchiveId((long) i);
      archive.setStatus(ArchiveStatus.GOOD);
      archive.setStorageType(StorageType.SATA);
      archives.add(archive);
      archivesInDataNode.put(removeDatanodeId, Long.valueOf(i));
    }

    EndPoint endPoint = new EndPoint();
    when(storagePool.getArchivesInDataNode()).thenReturn(archivesInDataNode);
    when(datanode.getArchives()).thenReturn(archives);
    when(instanceStore.get(any(InstanceId.class))).thenReturn(instance);
    when(instance.getEndPoint()).thenReturn(endPoint);

    DataNodeService.Iface dataNodeClient = mock(DataNodeService.Iface.class);
    when(dataNodeClientFactory.generateSyncClient(any(EndPoint.class), anyLong()))
        .thenReturn(dataNodeClient);

    RemoveDatanodeFromDomainRequest requestRemoveDatanodeSuccess =
        new RemoveDatanodeFromDomainRequest();
    requestRemoveDatanodeSuccess.setRequestId(RequestIdBuilder.get());
    requestRemoveDatanodeSuccess.setDomainId(sucessDomainId);
    requestRemoveDatanodeSuccess.setDatanodeInstanceId(removeDatanodeId);

    //set the Datanode in the domain
    when(datanode.getDomainId()).thenReturn(sucessDomainId);
    informationCenter.removeDatanodeFromDomain(requestRemoveDatanodeSuccess);

    Mockito.verify(storagePoolStore, Mockito.times(5)).saveStoragePool(any(StoragePool.class));
    Mockito.verify(domainStore, Mockito.times(2)).saveDomain(any(Domain.class));
  }
}
