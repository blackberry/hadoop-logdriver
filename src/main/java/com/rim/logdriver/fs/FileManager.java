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

package com.rim.logdriver.fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.locks.LockUtil;

public class FileManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileManager.class);

  private static final String[] branches = new String[] { "/incoming/*/*",
      "/data/*", "/archive/*", "/working/*/incoming/*" };

  private static final PathFilter pathFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      if (name.startsWith("_") || name.endsWith(".tmp")) {
        return false;
      }
      return true;
    }
  };

  private LockUtil lockUtil;
  private FileSystem fs;

  private String logdir;

  public FileManager(Configuration conf) throws Exception {
    this.lockUtil = new LockUtil(conf);
    this.fs = FileSystem.get(conf);

    // the logdir can be set by a java property.
    logdir = conf.get("logdriver.logdir.name", "logs");
  }

  public List<String> getHoursForTimeRange(long start, long end) {
    List<String> hours = new ArrayList<String>();

    // Bad range means return empty list
    if (end < start) {
      return hours;
    }

    // If the numbers are the same, then we just want the end. This will ensure
    // that we get the start, but it will not return the next hour.
    if (end == start) {
      end++;
    }

    Calendar endCal = Calendar.getInstance();
    
    // Round the end time up to the last millisecond of the hour
    endCal.setTimeInMillis(end - 1);
    endCal.set(Calendar.MINUTE, 59);
    endCal.set(Calendar.SECOND, 59);
    endCal.set(Calendar.MILLISECOND, 999);
    

    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(start);
    while (c.before(endCal)) {
      hours.add(String.format("/%04d%02d%02d/%02d", c.get(Calendar.YEAR),
          c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH),
          c.get(Calendar.HOUR_OF_DAY)));

      c.add(Calendar.HOUR_OF_DAY, 1);
    }

    return hours;
  }

  public List<PathInfo> getPathInfo(String dcNumber, String service,
      String component, long startTime, long endTime) throws Exception {
    List<String> hours = getHoursForTimeRange(startTime, endTime);
    List<PathInfo> paths = new ArrayList<PathInfo>();
    String glob;
    FileStatus[] fileStatuses;
    for (String hour : hours) {
      glob = "/service/" + dcNumber + "/" + service + "/" + logdir + hour + "/"
          + component;
      fileStatuses = fs.globStatus(new Path(glob), pathFilter);
      if (fileStatuses == null) {
        continue;
      }
      for (FileStatus f : fileStatuses) {
        if (f.isDir()) {
          paths.add(new PathInfo(logdir, f.getPath().toUri().getPath()));
        }
      }
    }
    return paths;
  }

  public List<String> getInputPaths(PathInfo pi) throws IOException {
    List<String> list = new ArrayList<String>();
    FileStatus[] fileStatuses;
    for (String branch : branches) {
      fileStatuses = fs.globStatus(new Path(pi.getFullPath() + branch),
          pathFilter);
      if (fileStatuses == null) {
        continue;
      }
      for (FileStatus f : fileStatuses) {
        list.add(f.getPath().toUri().getPath());
      }
    }
    return list;
  }

  public void acquireReadLocks(List<PathInfo> paths) throws Exception {
    // First, ensure we have all the paths in order
    SortedSet<String> sortedPaths = new TreeSet<String>();

    for (PathInfo pi : paths) {
      sortedPaths.add(lockUtil.getLockPath(pi));
    }

    // For each path, grab the lock
    for (String p : sortedPaths) {
      while (true) {
        try {
          lockUtil.acquireReadLock(p);
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
          continue;
        }
        break;
      }
    }
  }

  public void releaseReadLocks(List<PathInfo> paths) throws Exception {
    // First, ensure we have all the paths in order
    SortedSet<String> sortedPaths = new TreeSet<String>();

    for (PathInfo pi : paths) {
      sortedPaths.add(lockUtil.getLockPath(pi));
    }

    // For each path, release the lock
    for (String p : sortedPaths) {
      while (true) {
        try {
          lockUtil.releaseReadLock(p);
        } catch (KeeperException.ConnectionLossException e) {
          LOG.warn("Lost connection to ZooKeeper.  Retrying.", e);
          continue;
        }
        break;
      }
    }
  }
}
