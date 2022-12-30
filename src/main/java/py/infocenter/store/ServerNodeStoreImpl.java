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

package py.infocenter.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.ServerNode;


@Transactional
public class ServerNodeStoreImpl implements ServerNodeStore {

  private static final Logger logger = LoggerFactory.getLogger(ServerNodeStoreImpl.class);
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void clearDb() {

    List<ServerNode> serverNodeList = listAllServerNodes();
    List<String> ids = new ArrayList<>();
    for (ServerNode serverNode1 : serverNodeList) {
      ids.add(serverNode1.getId());
    }
    deleteServerNodes(ids);
  }

  @Override
  public void saveOrUpdateServerNode(ServerNode serverNode) {
    sessionFactory.getCurrentSession().saveOrUpdate(serverNode);
  }

  @Override
  public void deleteServerNodes(List<String> serverNodeIds) {
    for (String serverNodeId : serverNodeIds) {
      deleteServerNodeById(serverNodeId);
    }
  }

  private void deleteServerNodeById(String serverNodeId) {
    if (serverNodeId == null) {
      logger.error("Invalid parameter, serverNodeId:{}", serverNodeId);
      return;
    }
    try {
      ServerNode serverNode = sessionFactory.getCurrentSession()
          .get(ServerNode.class, serverNodeId);
      sessionFactory.getCurrentSession().delete(serverNode);
    } catch (Exception e) {
      logger.error("caught an exception: {}", e);
      throw e;
    }
  }

  @Override
  public void updateServerNode(ServerNode serverNode) {
    sessionFactory.getCurrentSession().update(serverNode);
  }

  @Override
  public List<ServerNode> listAllServerNodes() {
    List<ServerNode> serverNodeList = sessionFactory.getCurrentSession()
        .createQuery("from ServerNode").list();
    return serverNodeList;
  }

  @SuppressWarnings("deprecation")
  @Override
  public ServerNode getServerNodeByIp(String ip) {
    Session currentSession = sessionFactory.getCurrentSession();
    Criteria criteria = currentSession.createCriteria(ServerNode.class);
    criteria.add(Restrictions.ilike("networkCardInfoName", ip, MatchMode.ANYWHERE));
    List list = criteria.list();
    if (list.size() != 0) {
      return (ServerNode) list.get(0);
    } else {
      return null;
    }
  }


  @Override
  public ServerNode listServerNodeById(String id) {
    ServerNode serverNode = sessionFactory.getCurrentSession().get(ServerNode.class, id);
    return serverNode;
  }

  /**
   * xx.
   *
   * @param sortDirection "ASC" | "DESC"
   */
  @Override
  public List<ServerNode> listServerNodes(int offset, int limit, String sortField,
      String sortDirection,
      String hostName, String modelInfo, String cpuInfo, String memoryInfo, String diskInfo,
      String networkCardInfo,
      String manageIp, String gatewayIp, String storeIp, String rackNo, String slotNo) {
    logger.debug(
        "listServerNodes parameter: limit-{}, page-{}, sortField-{}, sortDirection-{},"
            + "modelInfo-{}, cpuInfo-{}, memoryInfo-{}, diskInfo-{}, networkCardInfo-{}, "
            + "manageIp-{}, gatewayIp-{}, storeIp-{}, rackNo-{}, slotNo-{}",
        offset, limit, sortField, sortDirection, modelInfo, cpuInfo, memoryInfo, diskInfo,
        networkCardInfo,
        manageIp, gatewayIp, storeIp, rackNo, slotNo);

    StringBuilder hqlSequenceSb = new StringBuilder("from ServerNode ");
    String hqlFilterStr = getFilterStr(hostName, modelInfo, cpuInfo, memoryInfo, diskInfo,
        networkCardInfo, manageIp,
        gatewayIp, storeIp, rackNo, slotNo);
    hqlSequenceSb.append(hqlFilterStr);

    if (sortField == null) {
      sortField = "networkCardInfoName";
    }
    if (!Objects.equals(sortDirection, "ASC") && !Objects.equals(sortDirection, "DESC")) {
      sortDirection = "ASC";
    }
    hqlSequenceSb.append(String.format(" order by %s %s ", sortField, sortDirection));

    String hqlSequenceStr = hqlSequenceSb.toString();
    try {
      Query query = sessionFactory.getCurrentSession().createQuery(hqlSequenceStr);
      query.setFirstResult(offset);
      query.setMaxResults(limit);
      logger.debug("hqlSequenceStr: {}", hqlSequenceStr);
      List<ServerNode> serverNodeList = query.list();
      return serverNodeList;
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }
  }

  @Override
  public int getCountTotle() {
    String hqlString = "select count(id) from ServerNode ";
    Query query = sessionFactory.getCurrentSession()
        .createQuery(hqlString);
    return ((Number) query.uniqueResult()).intValue();
  }

  private String getFilterStr(String hostName, String modelInfo, String cpuInfo, String memoryInfo,
      String diskInfo,
      String networkCardInfo, String manageIp, String gatewayIp, String storeIp, String rackNo,
      String slotNo) {

    StringBuilder sqlSequenceSb = new StringBuilder(String.format(" where 1 = 1"));

    if (manageIp != null) {
      sqlSequenceSb.append(String.format("and lower(manageIp) = lower('%s') ", manageIp));
    }
    if (rackNo != null) {
      sqlSequenceSb.append(String.format("and lower(rackNo) = lower('%s') ", rackNo));
    }
    if (slotNo != null) {
      sqlSequenceSb.append(String.format("and lower(slotNo) = lower('%s') ", slotNo));
    }
    if (hostName != null) {
      sqlSequenceSb
          .append(String.format("and lower(hostName) like lower('%%" + "%s" + "%%') ", hostName));
    }
    if (modelInfo != null) {
      sqlSequenceSb
          .append(String.format("and lower(modelInfo) like lower('%%" + "%s" + "%%') ", modelInfo));
    }
    if (cpuInfo != null) {
      sqlSequenceSb
          .append(String.format("and lower(cpuInfo) like lower('%%" + "%s" + "%%') ", cpuInfo));
    }
    if (memoryInfo != null) {
      sqlSequenceSb.append(
          String.format("and lower(memoryInfo) like lower('%%" + "%s" + "%%') ", memoryInfo));
    }
    if (diskInfo != null) {
      sqlSequenceSb
          .append(String.format("and lower(diskInfo) like lower('%%" + "%s" + "%%') ", diskInfo));
    }
    if (networkCardInfo != null) {
      sqlSequenceSb.append(String
          .format("and lower(networkCardInfo) like lower('%%" + "%s" + "%%') ", networkCardInfo));
    }
    if (storeIp != null) {
      sqlSequenceSb
          .append(String.format("and lower(storeIp) like lower('%%" + "%s" + "%%') ", storeIp));
    }
    if (gatewayIp != null) {
      sqlSequenceSb
          .append(String.format("and lower(gatewayIp) like lower('%%" + "%s" + "%%') ", gatewayIp));
    }

    return sqlSequenceSb.toString();
  }

}
