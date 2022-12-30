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
