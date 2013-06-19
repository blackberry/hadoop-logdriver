/** Copyright 2013 BlackBerry, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. 
 */

package com.rim.logdriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.fs.PathInfo;
import com.rim.logdriver.locks.LockUtil;

public class LockedFsShell {
  private static final Logger LOG = LoggerFactory
      .getLogger(LockedFsShell.class);

  public static void main(String[] args) {
    // The required args are zkConnectString, dcNumber, service, date, hour,
    // component and an arbitrary number of commands.
    if (args.length < 7) {
      printUsage();
      System.exit(1);
    }

    String zkConnectString = args[0];
    String dcNumber = args[1];
    String service = args[2];
    String date = args[3];
    String hour = args[4];
    String component = args[5];

    // Set the configuration correctly, so we can reach zookeeper
    Configuration conf = new Configuration();
    conf.set("zk.connect.string", zkConnectString);

    try {
      LockUtil lockUtil = new LockUtil(conf);

      PathInfo pathInfo = new PathInfo();
      pathInfo.setDcNumber(dcNumber);
      pathInfo.setService(service);
      pathInfo.setDate(date);
      pathInfo.setHour(hour);
      pathInfo.setComponent(component);

      String lockPath = lockUtil.getLockPath(pathInfo);

      // Get the write lock
      while (true) {
        try {
          lockUtil.acquireWriteLock(lockPath);
          break;
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
        }
      }

      // Run the commands
      int res = 0;

      for (int i = 6; i < args.length; i++) {
        String[] fsShellArgs = args[i].split("\\s+");
        LOG.info("Calling FsShell with args {}", args[i]);
        FsShell shell = new FsShell();
        try {
          res = ToolRunner.run(shell, fsShellArgs);
        } finally {
          shell.close();
        }

        if (res != 0) {
          break;
        }
      }

      // Release the write lock
      while (true) {
        try {
          lockUtil.releaseWriteLock(lockPath);
          break;
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
        }
      }

      if (res != 0) {
        LOG.error("Bad return value ({}) from FsShell", res);
        System.exit(res);
      }

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.out.println("Usage: " + LockedFsShell.class.getSimpleName()
        + " <zkConnectString> <dcNumber> <service> "
        + "<yyyymmdd> <hh> <component> <command> [<command> ...]");
  }

}
