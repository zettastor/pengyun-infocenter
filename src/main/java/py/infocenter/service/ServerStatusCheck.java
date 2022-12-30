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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.NamedThreadFactory;
import py.icshare.CheckStatus;


public class ServerStatusCheck {

  private static final Logger logger = LoggerFactory.getLogger(ServerStatusCheck.class);
  private ExecutorService checkExecutor;
  //true is ok
  private Map<String, String> zookeeperServiceStatus = new ConcurrentHashMap<>();
  private String zookeeperConnectionString;


  public ServerStatusCheck(String zookeeperConnectionString) {
    this.zookeeperConnectionString = zookeeperConnectionString;
    checkExecutor = createCheckExecutor();
    logger.warn("in ServerStatusCheck the zk string:{}", zookeeperConnectionString);
  }

  public Map<String, String> getZookeeperServiceStatus() {
    return zookeeperServiceStatus;
  }


  public void doCheck() {
    try {
      //zk
      String[] endPointZks = zookeeperConnectionString.split(",");
      CountDownLatch countDownLatch = new CountDownLatch(endPointZks.length);

      for (String endPoint : endPointZks) {
        CheckStatusTask checkStatusTask = new CheckStatusTask(endPoint, zookeeperServiceStatus);
        CompletableFuture
            .runAsync(checkStatusTask, checkExecutor)
            .thenRun(
                () -> {
                  countDownLatch.countDown();
                }
            )
            .exceptionally(
                throwable -> {
                  countDownLatch.countDown();
                  return null;
                }
            );
      }
      //50s
      try {
        countDownLatch.await(50000, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("check zk status, wait the thread time out", e);
      }

    } catch (Exception e) {
      logger.warn("check ServiceStatus is error, cause exception:", e);
    }
  }

  private ExecutorService createCheckExecutor() {
    return new ThreadPoolExecutor(1, 5,
        2, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
        new NamedThreadFactory("check-zk-status"), new ThreadPoolExecutor.CallerRunsPolicy());
  }


  public void stop() {
    if (checkExecutor != null) {
      checkExecutor.shutdown();
    }
  }


  public class CheckStatusTask implements Runnable {

    private String url;
    private Map<String, String> zookeeperServiceStatus;


    public CheckStatusTask(String url, Map<String, String> zookeeperServiceStatus) {
      this.url = url;
      this.zookeeperServiceStatus = zookeeperServiceStatus;
    }

    @Override
    public void run() {
      //for zk
      String host = "";
      int port = 0;
      String cmd = "stat";
      BufferedReader reader = null;
      Socket sock = null;
      try {
        String[] split = url.split(":");
        host = split[0];
        port = Integer.parseInt(split[1]);
        logger.info("check zk, get the host:{}, port:{}", host, port);

        sock = new Socket();
        SocketAddress endpoint = new InetSocketAddress(host, port);
        sock.connect(endpoint, 2000);

        OutputStream outstream = sock.getOutputStream();
        //
        outstream.write(cmd.getBytes());
        outstream.flush();
        sock.shutdownOutput();
        reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));

        String line;
        boolean getIt = false;
        //if have value, is good
        while ((line = reader.readLine()) != null) {
          if (line.indexOf("Mode: ") != -1) {
            zookeeperServiceStatus.put(url, CheckStatus.OK.name());
            getIt = true;
            break;
          }
        }

        //if not have value, is error
        if (!getIt) {
          zookeeperServiceStatus.put(url, CheckStatus.ERROR.name());
        }
      } catch (Exception e) {
        logger.warn("check ZookeeperServiceStatus is read data {}, cause exception: {}", host, e);
        zookeeperServiceStatus.put(url, CheckStatus.ERROR.name());
      } finally {
        try {
          if (sock != null) {
            sock.close();
          }

          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          logger.warn("check ZookeeperServiceStatus is close io, cause exception:", e);
        }
      }
    }
  }
}
