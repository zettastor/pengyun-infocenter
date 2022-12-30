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
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.CmdResult;
import py.common.cmd.ProcessRun;
import py.icshare.CheckStatus;
import py.test.TestBase;
import py.thrift.share.ServiceIpStatusThrift;

public class NormalTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(NormalTest.class);

  @Test
  public void test() {
    List<Integer> addSegmentOrder = new ArrayList<>();
    addSegmentOrder.add(2);
    addSegmentOrder.add(2);
    addSegmentOrder.remove(new Integer(1));
    logger.warn("get the value:{}", addSegmentOrder);

    long time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
        .parse("1970-01-01 08:00:00", new ParsePosition(0)).getTime() / 1000;

    long now1 = System.currentTimeMillis();
    long now2 = new Date().getTime();

    logger.warn("get the value:{}, {}", time, new Date(0));

    for (int i = 0; i < 10; i++) {
      if (i == 5) {
        break;
      }
      logger.warn("get the i:{}", i);
    }

    String s = "http://10.0.0.81:2380";
    String endUrl = "";
    if (s.startsWith("http://")) {
      endUrl = s.substring(7);
    }

    if (!endUrl.isEmpty()) {
      String[] strings = endUrl.split(":");
      if (strings.length != 0) {
        String sub = strings[0];
        logger.warn("get the sub: {}", sub);
      }
    }
  }

  @Ignore
  @Test
  public void serverStatus() throws IOException {
    String host = "10.0.0.82";
    int port = 2181;
    String cmd = "stat";

    Socket sock = new Socket();
    SocketAddress endpoint = new InetSocketAddress(host, port);
    sock.connect(endpoint, 2000);
    BufferedReader reader = null;
    try {
      OutputStream outstream = sock.getOutputStream();

      outstream.write(cmd.getBytes());
      outstream.flush();
      sock.shutdownOutput();

      reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.indexOf("Mode: ") != -1) {
          System.out.println(line.replaceAll("Mode: ", "").trim());
        }
      }
    } finally {
      sock.close();
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Ignore
  @Test
  public void wen() {
    ServiceIpStatusThrift thrift = null;
    String host = "";
    int port = 0;
    String cmd = "stat";
    BufferedReader reader = null;
    Socket sock = null;

    String ipUrl = "10.0.0.80:2181,10.0.0.81:2181,10.0.0.82:2181";
    String[] endPoints = ipUrl.split(",");
    for (String endPoint : endPoints) {
      try {
        thrift = new ServiceIpStatusThrift();
        String[] split = endPoint.split(":");
        host = split[0];
        port = Integer.parseInt(split[1]);
        sock = new Socket(host, port);
        OutputStream outstream = sock.getOutputStream();

        outstream.write(cmd.getBytes());
        outstream.flush();
        sock.shutdownOutput();
        reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));

        String line;
        thrift.setHostname(host);
        while ((line = reader.readLine()) != null) {
          if (line.indexOf("Mode: ") != -1) {
            thrift.setStatus(CheckStatus.OK.name());
          }
        }

        if (thrift.getStatus().isEmpty()) {
          thrift.setStatus(CheckStatus.ERROR.name());
        }
      } catch (Exception e) {
        logger.warn("listZookeeperServiceStatus is read data {}, cause exception: {}", host, e);
        thrift.setHostname(host);
        thrift.setStatus(CheckStatus.ERROR.name());
      } finally {
        try {
          if (sock != null) {
            sock.close();
          }
          if (reader != null) {
            reader.close();
          }
        } catch (IOException e) {
          logger.warn("listZookeeperServiceStatus is close io, cause exception: {}", e);
        }
      }
      logger.warn("get the thrift:{}", thrift);
    }

  }
}
