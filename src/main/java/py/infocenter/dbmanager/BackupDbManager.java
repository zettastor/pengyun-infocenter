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

package py.infocenter.dbmanager;


import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;

/**
 * 1, pick up datanodes should consider groupId 2, find way to process round define: if set three
 * groups should save database info, first arrived three groups and first datanode in its' group
 * will save DB Info.
 *
 */
public interface BackupDbManager {

  public ReportDbResponseThrift process(ReportDbRequestThrift reportRequest);

  void backupDatabase();

  /**
   * xx.
   *
   * @return whether successfully recover database.
   */
  boolean recoverDatabase();

  public boolean needRecoverDb();

  public void loadTablesFromDb(ReportDbResponseThrift response) throws Exception;

  /**
   * xx.
   *
   * @param newestDbInfo newest database info to store
   * @return whether successfully save newest database info to database
   */
  public boolean saveTablesToDb(ReportDbRequestThrift newestDbInfo);

  public boolean passedRecoveryTime();

}
