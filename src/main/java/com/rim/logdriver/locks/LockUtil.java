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
 * This class handles locking and unlocking of services within zookeeper.
 * <p>
 * It consists of a few utility classes that handle the locking.
 * <p>
 * Important! <code>zk.connect.string</code> must be set, or this will fail.
 */
package com.rim.logdriver.locks;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.fs.PathInfo;

public class LockUtil {
  private static final Logger LOG = LoggerFactory.getLogger(LockUtil.class);

  private static final String ZK_CONNECT_STRING_PROPERTY = "zk.connect.string";
  public static final String ROOT = "/logdriver/locks";

  private static final Pattern readPattern = Pattern.compile("read-.*-\\d{10}");
  private static final Pattern writePattern = Pattern
      .compile("write-.*-\\d{10}");

  private static final int SESSION_TIMEOUT = 15000;

  private ZooKeeper zk;

  public LockUtil(ZooKeeper zk) {
    this.zk = zk;
  }

  public LockUtil(String zkConnectString) throws IOException {
    this.zk = getClient(zkConnectString);
  }

  public LockUtil(Configuration conf) throws Exception {
    String zkConnectString = conf.get(ZK_CONNECT_STRING_PROPERTY);
    if (zkConnectString == null) {
      throw new Exception("Configuration item missing: "
          + ZK_CONNECT_STRING_PROPERTY);
    }
    this.zk = getClient(zkConnectString);
  }

  private static ZooKeeper getClient(String zkConnectString) throws IOException {
    ZooKeeper zk = new ZooKeeper(zkConnectString, SESSION_TIMEOUT,
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
          }
        });
    return zk;
  }

  public String getLockPath(PathInfo pathInfo) throws Exception {
    return ROOT + pathInfo.getFullPath();
  }

  protected void ensureNodeExists(String path) throws KeeperException,
      InterruptedException {
    if (zk.exists(path, false) == null) {
      String[] parts = path.split("/");

      String p = "";
      for (int i = 0; i < parts.length; i++) {
        if ("".equals(parts[i])) {
          continue;
        }
        p += "/" + parts[i];

        try {
          zk.create(p, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
          // That's fine.
        }
      }
    }
  }

  public ZooKeeper getZkClient() {
    return zk;
  }

  public List<LockInfo> scan(String scanPath) throws Exception {

    String basePath = ROOT + scanPath;
    List<LockInfo> lockInfo = new ArrayList<LockInfo>();

    Stat stat = new Stat();

    // Go down the tree, looking for nodes called 'read-' or 'write-'
    Queue<String> pathQueue = new ArrayDeque<String>();
    pathQueue.add(basePath);

    while (pathQueue.size() > 0) {
      String path = pathQueue.remove();

      List<String> children;
      try {
        children = zk.getChildren(path, false);
      } catch (KeeperException.NoNodeException e) {
        continue;
      }

      if (children.size() == 0) {
        zk.getData(path, false, stat);
        LockInfo li = new LockInfo();
        li.setPath(path);
        li.setReadLockCount(0);
        li.setWriteLockCount(0);
        li.setLastModified(stat.getMtime());

        lockInfo.add(li);
        continue;
      }

      int read = 0;
      int write = 0;
      for (String child : children) {
        if (readPattern.matcher(child).matches()) {
          read++;
        } else if (writePattern.matcher(child).matches()) {
          write++;
        } else {
          pathQueue.add(path + "/" + child);
        }
      }

      if (read > 0 || write > 0) {
        zk.getData(path, false, stat);
        LockInfo li = new LockInfo();
        li.setPath(path);
        li.setReadLockCount(read);
        li.setWriteLockCount(write);
        li.setLastModified(stat.getMtime());

        lockInfo.add(li);
        continue;
      }
    }

    Collections.sort(lockInfo);

    return lockInfo;
  }

  public void acquireReadLock(String lockPath) throws Exception {
    final SynchronousQueue<Byte> gate = new SynchronousQueue<Byte>();

    LOG.info("Getting read lock on {}", lockPath);

    // There is a possibility here that the node we're working on will be
    // deleting while we try all this. So catch any NoNode exceptions and retry
    // if that happens.
    while (true) {
      try {
        // Ensure the parent exists
        ensureNodeExists(lockPath);

        String node = null;

        // Do we already have a node?
        LOG.debug("Checking for an existing lock");
        Stat stat = new Stat();
        for (String child : zk.getChildren(lockPath, false)) {
          if (!child.startsWith("read-")) {
            LOG.debug("  {} does not start with read-", child);
            continue;
          }

          // Sometimes someone else will delete their node while I'm searching
          // through them for mine. That's okay!
          try {
            zk.getData(lockPath + "/" + child, false, stat);
          } catch (KeeperException.NoNodeException e) {
            LOG.debug("Node was deleted before I could check if I own it: {}",
                child, e);
            continue;
          }

          if (zk.getSessionId() == stat.getEphemeralOwner()) {
            LOG.debug("  {} is owned by me!", child);
            node = lockPath + "/" + child;
            break;
          }
        }

        // Create a sequential node under the parent
        if (node == null) {
          LOG.debug("Creating a new node");
          node = zk.create(lockPath + "/read-", new byte[0],
              Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        List<String> children;

        // The node number is 10 digits at the end of the node name
        String nodeNumber = node.substring(node.length() - 10);
        String previousNode = null;
        String previousNodeNumber = null;
        String childNodeNumber = null;

        while (true) {
          previousNode = null;
          children = zk.getChildren(lockPath, false);
          LOG.debug("Children = {}", children);

          for (String child : children) {
            // Skip anything that is not a write lock.
            if (!child.startsWith("write-")) {
              continue;
            }

            // So get the number, and if it's less, then wait on it. Otherwise,
            // we
            // have the lock.
            childNodeNumber = child.substring(child.length() - 10);
            if (nodeNumber.compareTo(childNodeNumber) > 0) {
              // This child comes before me.
              if (previousNode == null) {
                previousNode = child;
                previousNodeNumber = childNodeNumber;
              } else if (previousNodeNumber.compareTo(childNodeNumber) < 0) {
                previousNode = child;
                previousNodeNumber = childNodeNumber;
              }
            }
            LOG.debug("Previous node={}", previousNode);
          }

          Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
              try {
                gate.put((byte) 0);
              } catch (InterruptedException e) {
                // If this happens, this method will never return.
              }
            }
          };

          if (previousNode == null) {
            // No previous node? We have the lock!
            LOG.debug("No previous node - got lock");
            break;
          }
          stat = zk.exists(lockPath + "/" + previousNode, watcher);
          if (stat == null) {
            continue;
          }

          // wait for the watcher to get news
          gate.take();
        }
      } catch (KeeperException.NoNodeException e) {
        LOG.warn("Node was deleted while trying to acquire lock.  Retrying.");
        continue;
      }
      break;
    }

    LOG.info("Got read lock on {}", lockPath);
  }

  public long getReadLockCount(String lockPath) throws Exception {

    int count = 0;

    try {
      for (String child : zk.getChildren(lockPath, false)) {
        if (child.startsWith("read-")) {
          count++;
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // Nothing there? That's a zero.
      return 0;
    }

    return count;
  }

  public boolean releaseReadLock(String lockPath) throws Exception {

    LOG.info("Releasing read lock on {}", lockPath);

    // Ensure the parent exists
    ensureNodeExists(lockPath);

    // Do we have a node?
    LOG.debug("Checking for an existing lock");
    Stat stat = new Stat();
    for (String child : zk.getChildren(lockPath, false)) {
      if (!child.startsWith("read-")) {
        LOG.debug("  {} does not start with read-", child);
        continue;
      }

      // Sometimes someone else will delete their node while I'm searching
      // through them for mine. That's okay!
      try {
        zk.getData(lockPath + "/" + child, false, stat);
      } catch (KeeperException.NoNodeException e) {
        LOG.debug("Node was deleted before I could check if I own it: {}",
            child, e);
        continue;
      }

      if (zk.getSessionId() == stat.getEphemeralOwner()) {
        LOG.debug("  {} is owned by me!", child);
        zk.delete(lockPath + "/" + child, -1);
        LOG.info("Released read lock on {}", lockPath);
        return true;
      }
    }

    LOG.info("No read lock found to release on {}", lockPath);
    return false;
  }

  public boolean resetReadLock(String lockPath) throws Exception {

    LOG.info("Resetting read lock on {}", lockPath);

    // Ensure the parent exists
    ensureNodeExists(lockPath);

    for (String child : zk.getChildren(lockPath, false)) {
      if (child.startsWith("read-")) {
        zk.delete(lockPath + "/" + child, -1);
      }
    }

    return true;
  }

  public void acquireWriteLock(String lockPath) throws Exception {
    final SynchronousQueue<Byte> gate = new SynchronousQueue<Byte>();

    LOG.info("Getting write lock on {}", lockPath);

    // There is a possibility here that the node we're working on will be
    // deleting while we try all this. So catch any NoNode exceptions and retry
    // if that happens.
    while (true) {
      try {
        // Ensure the parent exists
        ensureNodeExists(lockPath);

        String node = null;

        // Do we already have a node?
        LOG.debug("Checking for an existing lock");
        Stat stat = new Stat();
        for (String child : zk.getChildren(lockPath, false)) {
          if (!child.startsWith("write-")) {
            LOG.debug("  {} does not start with write-", child);
            continue;
          }

          // Sometimes someone else will delete their node while I'm searching
          // through them for mine. That's okay!
          try {
            zk.getData(lockPath + "/" + child, false, stat);
          } catch (KeeperException.NoNodeException e) {
            LOG.debug("Node was deleted before I could check if I own it: {}",
                child, e);
            continue;
          }

          if (zk.getSessionId() == stat.getEphemeralOwner()) {
            LOG.debug("  {} is owned by me!", child);
            node = lockPath + "/" + child;
            break;
          }
        }

        // Create a sequential node under the parent
        if (node == null) {
          LOG.debug("Creating a new node");
          node = zk.create(lockPath + "/write-", new byte[0],
              Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        List<String> children;

        // The node number is 10 digits at the end of the node name
        String nodeNumber = node.substring(node.length() - 10);
        String previousNode = null;
        String previousNodeNumber = null;
        String childNodeNumber = null;

        while (true) {
          previousNode = null;
          children = zk.getChildren(lockPath, false);
          LOG.debug("Children = {}", children);

          for (String child : children) {
            // So get the number, and if it's less, then wait on it. Otherwise,
            // we
            // have the lock.
            childNodeNumber = child.substring(child.length() - 10);
            if (nodeNumber.compareTo(childNodeNumber) > 0) {
              // This child comes before me.
              if (previousNode == null) {
                previousNode = child;
                previousNodeNumber = childNodeNumber;
              } else if (previousNodeNumber.compareTo(childNodeNumber) < 0) {
                previousNode = child;
                previousNodeNumber = childNodeNumber;
              }
            }
            LOG.debug("Previous node={}", previousNode);
          }

          Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
              try {
                gate.put((byte) 0);
              } catch (InterruptedException e) {
                // If this happens, this method will never return.
              }
            }
          };

          if (previousNode == null) {
            // No previous node? We have the lock!
            LOG.debug("No previous node - got lock");
            break;
          }
          stat = zk.exists(lockPath + "/" + previousNode, watcher);
          if (stat == null) {
            continue;
          }

          // wait for the watcher to get news
          gate.take();
        }
      } catch (KeeperException.NoNodeException e) {
        LOG.warn("Node was deleted while trying to acquire lock.  Retrying.");
        continue;
      }
      break;
    }

    LOG.info("Got write lock on {}", lockPath);
  }

  public long getWriteLockCount(String lockPath) throws Exception {

    int count = 0;

    try {
      for (String child : zk.getChildren(lockPath, false)) {
        if (child.startsWith("write-")) {
          count++;
        }
      }
    } catch (KeeperException.NoNodeException e) {
      // Nothing there? That's a zero.
      return 0;
    }

    return count;
  }

  public boolean releaseWriteLock(String lockPath) throws Exception {

    LOG.info("Releasing write lock on {}", lockPath);

    // Ensure the parent exists
    ensureNodeExists(lockPath);

    // Do we have a node?
    LOG.debug("Checking for an existing lock");
    Stat stat = new Stat();
    for (String child : zk.getChildren(lockPath, false)) {
      if (!child.startsWith("write-")) {
        LOG.debug("  {} does not start with write-", child);
        continue;
      }

      // Sometimes someone else will delete their node while I'm searching
      // through them for mine. That's okay!
      try {
        zk.getData(lockPath + "/" + child, false, stat);
      } catch (KeeperException.NoNodeException e) {
        LOG.debug("Node was deleted before I could check if I own it: {}",
            child, e);
        continue;
      }

      if (zk.getSessionId() == stat.getEphemeralOwner()) {
        LOG.debug("  {} is owned by me!", child);
        zk.delete(lockPath + "/" + child, -1);
        LOG.info("Released write lock on {}", lockPath);
        return true;
      }
    }

    LOG.info("No write lock found to release on {}", lockPath);
    return false;
  }

  public boolean resetWriteLock(String lockPath) throws Exception {

    LOG.debug("Resetting write lock on {}", lockPath);

    // Ensure the parent exists
    ensureNodeExists(lockPath);

    for (String child : zk.getChildren(lockPath, false)) {
      if (child.startsWith("write-")) {
        zk.delete(lockPath + "/" + child, -1);
      }
    }

    return true;
  }

  public void close() throws InterruptedException {
    zk.close();
  }

}
