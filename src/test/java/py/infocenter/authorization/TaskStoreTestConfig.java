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

package py.infocenter.authorization;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import py.infocenter.store.TaskStore;
import py.infocenter.store.TaskStoreImpl;

@Configurable
@ImportResource({"classpath:spring-config/hibernate.xml"})
public class TaskStoreTestConfig {

  @Autowired
  SessionFactory sessionFactory;

  @Bean
  public TaskStore taskStore() {
    TaskStoreImpl taskStore = new TaskStoreImpl();
    taskStore.setSessionFactory(sessionFactory);
    return taskStore;
  }

}
