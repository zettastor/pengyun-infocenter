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

package py.infocenter.store.control;

import java.util.List;
import java.util.stream.Collectors;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.VolumeCreationRequest;
import py.volume.VolumeStatus;

public class VolumeJobStoreImpl implements VolumeJobStoreDb {

  private static final Logger logger = LoggerFactory.getLogger(VolumeJobStoreImpl.class);
  private SessionFactory sessionFactory;

  @Transactional
  @Override
  public long saveCreateOrExtendVolumeRequest(VolumeCreationRequest request) {
    long result = (Long) sessionFactory.getCurrentSession().save(request);
    logger.warn("saveCreateOrExtendVolumeRequest, the result:{}", result);
    return result;
  }

  @Transactional
  @Override
  public void saveDeleteVolumeRequest(DeleteVolumeRequest request) {
    if (request == null) {
      logger.error("can not save a null DeleteVolumeRequest to DB");
      return;
    }
    sessionFactory.getCurrentSession().save(request);
  }

  @Transactional
  @Override
  public List<VolumeCreationRequest> getCreateOrExtendVolumeRequest() {
    List<VolumeCreationRequest> requests = sessionFactory.getCurrentSession()
        .createQuery("from VolumeCreationRequest where status = :status order by createdAt")
        .setParameter("status", VolumeStatus.ToBeCreated.name()).setMaxResults(2).list();

    logger.warn("get the VolumeCreationRequest size :{}", requests.size());
    return requests;
  }

  @Transactional
  @Override
  public List<Long> getAllCreateOrExtendVolumeId() {
    List<VolumeCreationRequest> requests = sessionFactory.getCurrentSession()
        .createQuery("from VolumeCreationRequest where status = :status order by createdAt")
        .setParameter("status", VolumeStatus.ToBeCreated.name()).list();

    List<Long> volumeIds = requests.stream().map(request -> request.getVolumeId())
        .collect(Collectors.toList());
    logger.warn("getAllCreateOrExtendVolumeId :{}", volumeIds);
    return volumeIds;
  }

  @Transactional
  @Override
  public List<DeleteVolumeRequest> getDeleteVolumeRequest() {
    List<DeleteVolumeRequest> requests = sessionFactory.getCurrentSession()
        .createQuery("from DeleteVolumeRequest")
        .list();

    return requests;
  }

  @Transactional
  @Override
  public void deleteCreateOrExtendVolumeRequest(VolumeCreationRequest request) {
    if (request == null) {
      logger.warn("can not delete VolumeCreationRequest form DB, can not find the value ");
      return;
    }
    logger.warn("delete VolumeCreationRequest form DB, the value :{}", request);
    sessionFactory.getCurrentSession().delete(request);
  }

  @Transactional
  @Override
  public void deleteDeleteVolumeRequest(DeleteVolumeRequest request) {
    if (request == null) {
      logger.warn("can not delete DeleteVolumeRequest form DB, can not find the value ");
      return;
    }
    logger.warn("delete DeleteVolumeRequest form DB, the value :{}", request);
    sessionFactory.getCurrentSession().delete(request);
  }

  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

}