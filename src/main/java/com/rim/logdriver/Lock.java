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

/**
 * Simple command line tools for locking and unlocking service/date.
 * <p>
 * Usage: Lock [-confFiles=FILE[,FILE ...]] [-zkConnectString=CONNECT_STRING] [READ|WRITE] [LOCK|UNLOCK] SERVICE_PATH TIMESTAMP
 */

package com.rim.logdriver;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.fs.PathInfo;
import com.rim.logdriver.locks.LockInfo;
import com.rim.logdriver.locks.LockUtil;

public class Lock {
  private static final Logger LOG = LoggerFactory.getLogger(Lock.class);

  public static void main(String[] args) {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }

    // We'll be using a Configuration object
    Configuration conf = new Configuration();

    // Check args for conf files or specific configs
    int i = 0;
    while (args[i].startsWith("-")) {
      String arg = args[i];
      if (arg.startsWith("-confFiles=")) {
        String fileNames = arg.substring("-confFiles=".length());
        String[] files = fileNames.split(",");
        for (String file : files) {
          conf.addResource(new Path(file));
        }
      } else if (arg.startsWith("-zkConnectString=")) {
        String connectString = arg.substring("-zkConnectString=".length());
        conf.set("zk.connect.string", connectString);
      }

      i++;
    }

    LockUtil lockUtil = null;
    try {
      lockUtil = new LockUtil(conf);
    } catch (Exception e) {
      LOG.error("Error getting ZooKeeper client.", e);
      System.exit(1);
    }

    // Look for the SCAN command. It's a little different.
    if (args.length > i && args[i].toUpperCase().equals("SCAN")) {
      String root = "";
      if (args.length > 1 + 1) {
        root = args[i + 1];
      }
      try {
        scan(lockUtil, root);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return;
    }

    // Look for the SCAN command. It's a little different.
    if (args.length > i + 1 && args[i].toUpperCase().equals("PURGE")) {
      String root = args[i + 1];

      try {
        purge(lockUtil, root);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return;
    }

    // Now, just grab the required args
    String readWrite = null;
    String lockUnlock = null;

    String dcNumber = null;
    String service = null;
    String date = null;
    String hour = null;
    String component = null;

    if (args.length < i + 6) {
      printUsage();
      System.exit(1);
    }

    readWrite = args[i].toUpperCase();
    if (!readWrite.equals("READ") && !readWrite.equals("WRITE")) {
      printUsage();
      System.exit(1);
    }
    i++;

    lockUnlock = args[i].toUpperCase();
    if (!lockUnlock.equals("RESET") && !lockUnlock.equals("STATUS")) {
      printUsage();
      System.exit(1);
    }
    i++;

    dcNumber = args[i];
    i++;

    service = args[i];
    i++;

    date = args[i];
    i++;

    hour = args[i];
    i++;

    component = args[i];

    PathInfo pathInfo = new PathInfo();
    try {
      pathInfo.setDcNumber(dcNumber);
      pathInfo.setService(service);
      pathInfo.setDate(date);
      pathInfo.setHour(hour);
      pathInfo.setComponent(component);
    } catch (Exception e) {
      LOG.error("Exception configuring path info.", e);
      System.exit(1);
    }

    String lockPath = null;
    try {
      lockPath = lockUtil.getLockPath(pathInfo);
    } catch (Exception e) {
      LOG.error("Error getting lock path", e);
      System.exit(1);
    }

    if (readWrite.equals("READ") && lockUnlock.equals("RESET")) {
      while (true) {
        try {
          lockUtil.resetReadLock(lockPath);
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
          continue;
        } catch (Exception e) {
          LOG.error("Unexpected error", e);
          System.exit(1);
        }
        break;
      }
    } else if (readWrite.equals("READ") && lockUnlock.equals("STATUS")) {
      long numLocks = 0;
      while (true) {
        try {
          numLocks = lockUtil.getReadLockCount(lockPath);
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
          continue;
        } catch (Exception e) {
          LOG.error("Unexpected error", e);
          System.exit(1);
        }

        System.out.println("Read lock count is " + numLocks);
        break;
      }
    } else if (readWrite.equals("WRITE") && lockUnlock.equals("RESET")) {
      while (true) {
        try {
          lockUtil.resetWriteLock(lockPath);
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
          continue;
        } catch (Exception e) {
          LOG.error("Unexpected error", e);
          System.exit(1);
        }
        break;
      }
    } else if (readWrite.equals("WRITE") && lockUnlock.equals("STATUS")) {
      long numLocks = 0;
      while (true) {
        try {
          numLocks = lockUtil.getWriteLockCount(lockPath);
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
          continue;
        } catch (Exception e) {
          LOG.error("Unexpected error", e);
          System.exit(1);
        }

        System.out.println("Write lock count is " + numLocks);
        break;
      }
    }
  }

  private static void scan(LockUtil lockUtil, String scanPath) throws Exception {
    for (LockInfo li : lockUtil.scan(scanPath)) {
      System.out.println(li);
    }
  }

  private static void purge(LockUtil lockUtil, String path)
      throws KeeperException, InterruptedException {
    ZooKeeper client = lockUtil.getZkClient();
    String p = path;

    // Delete all immediate children
    List<String> children = client.getChildren(p, false);
    for (String child : children) {
      client.delete(p + "/" + child, -1);
    }

    // Walk up the tree, deleting all directories that have no children
    // (stopping at /logdriver/locks)
    while (true) {
      if ("/logdriver/locks".equals(p) || "/".equals(p)) {
        break;
      }

      children = client.getChildren(p, false);
      if (children.size() == 0) {
        client.delete(p, -1);
        p = p.substring(0, p.lastIndexOf("/"));
        continue;
      }

      break;
    }
  }

  private static void printUsage() {
    System.out
        .println("Usage: Lock [-confFiles=FILE[,FILE ...]|-zkConnectString=ZK_CONNECT_STRING] cmd opts");
    System.out.println("  Commands and args");
    System.out
        .println("    READ STATUS [SERVICE_PATH] [TIMESTAMP] [COMPONENT]");
    System.out.println("    READ RESET [SERVICE_PATH] [TIMESTAMP] [COMPONENT]");
    System.out
        .println("    WRITE STATUS [SERVICE_PATH] [TIMESTAMP] [COMPONENT]");
    System.out
        .println("    WRITE RESET [SERVICE_PATH] [TIMESTAMP] [COMPONENT]");
    System.out.println("    SCAN [FULL_PATH]");
    System.out.println("    PURGE [FULL_PATH]");
  }

}
