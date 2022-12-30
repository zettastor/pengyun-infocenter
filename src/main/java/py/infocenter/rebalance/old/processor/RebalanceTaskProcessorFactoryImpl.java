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

package py.infocenter.rebalance.old.processor;

import py.client.thrift.GenericThriftClientFactory;
import py.infocenter.rebalance.old.RebalanceTaskProcessor;
import py.infocenter.rebalance.old.RebalanceTaskProcessorFactory;
import py.infocenter.service.InformationCenterImpl;
import py.instance.InstanceStore;
import py.thrift.datanode.service.DataNodeService.Iface;


public class RebalanceTaskProcessorFactoryImpl implements RebalanceTaskProcessorFactory {

  private final InstanceStore instanceStore;
  private final InformationCenterImpl informationCenter;
  private final GenericThriftClientFactory<Iface> dataNodeSyncClientFactory;


  public RebalanceTaskProcessorFactoryImpl(InstanceStore instanceStore,
      InformationCenterImpl informationCenter,
      GenericThriftClientFactory<Iface> dataNodeSyncClientFactory) {
    super();
    this.instanceStore = instanceStore;
    this.informationCenter = informationCenter;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
  }

  @Override
  public RebalanceTaskProcessor generateProcessor(BasicRebalanceTaskContext context) {
    if (context instanceof CreateSecondaryCandidateContext) {
      return new CreateSecondaryCandidateProcessor(context, instanceStore,
          dataNodeSyncClientFactory,
          informationCenter);
    } else if (context instanceof MigratePrimaryContext) {
      return new MigratePrimaryProcessor(context, instanceStore, dataNodeSyncClientFactory,
          informationCenter);
    } else if (context instanceof InnerMigrateSegmentUnitContext) {
      return new InnerMigrateSegmentUnitProcessor(context, instanceStore, dataNodeSyncClientFactory,
          informationCenter);
    } else {
      return new RetrieveRebalanceTaskProcessor(context, informationCenter);
    }
  }

}