/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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