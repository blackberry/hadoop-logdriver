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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.fs.PathInfo;
import com.rim.logdriver.locks.LockUtil;

public class LockedFs extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(LockedFs.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LockedFs(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    // The required args are zkConnectString, dcNumber, service, date, hour,
    // component, from, to
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
    String[] commands = new String[args.length - 6];

    String logDir = getConf().get("logdriver.logdir.name", "logs");

    for (int i = 6; i < args.length; i++) {
      commands[i - 6] = args[i];
    }

    // Set the configuration correctly, so we can reach zookeeper
    Configuration conf = getConf();
    conf.set("zk.connect.string", zkConnectString);

    LockUtil lockUtil = null;
    String lockPath = null;
    try {
      lockUtil = new LockUtil(conf);

      PathInfo pathInfo = new PathInfo();
      pathInfo.setDcNumber(dcNumber);
      pathInfo.setService(service);
      pathInfo.setLogdir(logDir);
      pathInfo.setDate(date);
      pathInfo.setHour(hour);
      pathInfo.setComponent(component);

      lockPath = lockUtil.getLockPath(pathInfo);

      // Get the write lock
      while (true) {
        try {
          lockUtil.acquireWriteLock(lockPath);
          break;
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
        }
      }

      for (String command : commands) {
        LOG.info("Running {}", command);

        String[] parts = command.split("\\s+");
        if ("move".equals(parts[0].toLowerCase())) {
          if (parts.length < 3) {
            LOG.error("Move required at least 2 arguements");
            return 1;
          }

          String[] from = new String[parts.length - 2];
          for (int i = 1; i < parts.length - 1; i++) {
            from[i - 1] = parts[i];
          }
          String to = parts[parts.length - 1];

          move(conf, from, to);

        } else if ("delete".equals(parts[0].toLowerCase())) {
          for (int i = 1; i < parts.length; i++) {
            delete(conf, parts[i]);
          }

        } else if ("touch".equals(parts[0].toLowerCase())) {
          for (int i = 1; i < parts.length; i++) {
            touch(conf, parts[i]);
          }

        }
      }
    } catch (Exception e) {
      LOG.error("Caught exception.", e);
    } finally {
      // Release the write lock
      while (true) {
        try {
          lockUtil.releaseWriteLock(lockPath);
          lockUtil.close();
          break;
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
        }
      }
    }

    return 0;
  }

  public void move(Configuration conf, String[] from, String to)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);

    List<FileStatus> fromList = new ArrayList<FileStatus>();
    for (String s : from) {
      FileStatus[] statuses = fs.globStatus(new Path(s));
      if (statuses == null) {
        continue;
      }
      for (FileStatus status : statuses) {
        fromList.add(status);
      }
    }

    Path toPath = new Path(to);
    Boolean toExists = fs.exists(toPath);
    FileStatus toFileStatus = null;
    if (toExists) {
      toFileStatus = fs.getFileStatus(toPath);
    }

    // If there is no from, that's a problem.
    if (fromList.isEmpty()) {
      throw new IOException("No input files found");
    }

    // If the to exists, and is a file, that's a problem too.
    if (toExists && !toFileStatus.isDir()) {
      throw new IOException("Destination file exists:" + to);
    }

    // If the destination exists, and is a directory, then ensure that none of
    // the from list names will clash with existing contents of the directory.
    if (toExists && toFileStatus.isDir()) {
      for (FileStatus fromStatus : fromList) {
        String name = fromStatus.getPath().getName();
        if (fs.exists(new Path(toPath, name))) {
          throw new IOException("Destination file exists:" + to + "/" + name);
        }
      }
    }

    // If the destination doesn't exist, but it ends with a slash, then create
    // it as a directory.
    if (!toExists && to.endsWith("/")) {
      fs.mkdirs(toPath);
      toFileStatus = fs.getFileStatus(toPath);
      toExists = true;
    }

    // If the destination doesn't exist, and there is more than one 'from', then
    // create a directory.
    if (!toExists && fromList.size() > 1) {
      fs.mkdirs(toPath);
      toFileStatus = fs.getFileStatus(toPath);
    }

    // If there was only one from, then just rename it to to
    if (fromList.size() == 1) {
      fs.mkdirs(toPath.getParent());
      fs.rename(fromList.get(0).getPath(), toPath);
    }

    // If there was more than one from, then for each file in the from list,
    // move it to the to directory.
    if (fromList.size() > 1) {
      for (FileStatus fromStatus : fromList) {
        String name = fromStatus.getPath().getName();
        fs.rename(fromStatus.getPath(), new Path(toPath, name));
      }
    }
  }

  public void delete(Configuration conf, String toDelete) throws IOException {
    FileSystem fs = FileSystem.get(conf);

    Path path = new Path(toDelete);
    if (fs.exists(path)) {
      fs.delete(path, true);
    } else {
      LOG.warn("File to delete not found:" + toDelete);
    }
  }

  public void touch(Configuration conf, String file) throws IOException {
    FileSystem fs = FileSystem.get(conf);

    long now = System.currentTimeMillis();

    Path path = new Path(file);
    fs.createNewFile(path);
    fs.setTimes(path, now, now);
  }

  private void printUsage() {
    System.out.println("Usage: " + this.getClass().getSimpleName()
        + " <zkConnectString> <dcNumber> <service> "
        + "<yyyymmdd> <hh> <component> <command> [<command> ...]");
  }
}
