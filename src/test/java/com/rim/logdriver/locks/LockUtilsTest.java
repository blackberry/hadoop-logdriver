package com.rim.logdriver.locks;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.fs.PathInfo;

public class LockUtilsTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(LockUtilsTest.class);

  private static final int clientPort = 21818; // non-standard
  private static final int numConnections = 5000;
  private static final int tickTime = 2000;

  private File dir;
  private Configuration conf;
  private ZooKeeperServer server;

  private Class<?> factoryClass;
  private Object standaloneServerFactory;

  public Configuration getConf() {
    return conf;
  }
  
  /**
   * Setup a local, embedded zookeeper instance, writing to temp/zookeeper.
   * 
   * @throws Exception
   */
  @Before
  public void setup() throws Exception {
    String dataDirectory = System.getProperty("java.io.tmpdir");

    dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();

    while (dir.exists()) {
      LOG.info("deleting {}", dir);
      FileUtils.deleteDirectory(dir);
    }

    server = new ZooKeeperServer(dir, dir, tickTime);

    // The class that we need changed name between CDH3 and CDH4, so let's check
    // for the right version here.
    try {
      factoryClass = Class
          .forName("org.apache.zookeeper.server.NIOServerCnxnFactory");

      standaloneServerFactory = factoryClass.newInstance();
      Method configure = factoryClass.getMethod("configure",
          InetSocketAddress.class, Integer.TYPE);
      configure.invoke(standaloneServerFactory, new InetSocketAddress(
          clientPort), numConnections);
      Method startup = factoryClass.getMethod("startup", ZooKeeperServer.class);
      startup.invoke(standaloneServerFactory, server);

    } catch (ClassNotFoundException e) {
      LOG.info("Did not find NIOServerCnxnFactory");
      try {
        factoryClass = Class
            .forName("org.apache.zookeeper.server.NIOServerCnxn$Factory");

        Constructor<?> constructor = factoryClass.getConstructor(
            InetSocketAddress.class, Integer.TYPE);
        standaloneServerFactory = constructor.newInstance(
            new InetSocketAddress(clientPort), numConnections);
        Method startup = factoryClass.getMethod("startup",
            ZooKeeperServer.class);
        startup.invoke(standaloneServerFactory, server);

      } catch (ClassNotFoundException e1) {
        LOG.info("Did not find NIOServerCnxn.Factory");
        throw new ClassNotFoundException(
            "Can't find NIOServerCnxnFactory or NIOServerCnxn.Factory");
      }
    }

    conf = new Configuration();
    conf.set("zk.connect.string", "localhost:" + clientPort);
  }

  @Test
  public void testReadLocks() throws Exception {
    String path = "/service/99/readlock/logs/20120203/04/component/";

    LockUtil lockUtil = new LockUtil(conf);
    String lockPath = lockUtil.getLockPath(new PathInfo(path));

    assertEquals(0, lockUtil.getReadLockCount(lockPath));
    assertEquals(false, lockUtil.releaseReadLock(lockPath));
    assertEquals(0, lockUtil.getReadLockCount(lockPath));
    lockUtil.acquireReadLock(lockPath);
    assertEquals(1, lockUtil.getReadLockCount(lockPath));
    lockUtil.acquireReadLock(lockPath);
    assertEquals(1, lockUtil.getReadLockCount(lockPath));
    assertEquals(true, lockUtil.releaseReadLock(lockPath));
    assertEquals(0, lockUtil.getReadLockCount(lockPath));
    assertEquals(false, lockUtil.releaseReadLock(lockPath));
    assertEquals(0, lockUtil.getReadLockCount(lockPath));
    lockUtil.acquireReadLock(lockPath);
    assertEquals(1, lockUtil.getReadLockCount(lockPath));
    assertEquals(true, lockUtil.resetReadLock(lockPath));
    assertEquals(0, lockUtil.getReadLockCount(lockPath));

    lockUtil.close();
  }

  @Test
  public void testWriteLocks() throws Exception {
    String path = "/service/99/writelock/logs/20120203/04/component/";

    LockUtil lockUtil = new LockUtil(conf);
    String lockPath = lockUtil.getLockPath(new PathInfo(path));

    assertEquals(0, lockUtil.getWriteLockCount(lockPath));
    assertEquals(false, lockUtil.releaseWriteLock(lockPath));
    assertEquals(0, lockUtil.getWriteLockCount(lockPath));
    lockUtil.acquireWriteLock(lockPath);
    assertEquals(1, lockUtil.getWriteLockCount(lockPath));
    lockUtil.acquireWriteLock(lockPath);
    assertEquals(1, lockUtil.getWriteLockCount(lockPath));
    assertEquals(true, lockUtil.releaseWriteLock(lockPath));
    assertEquals(0, lockUtil.getWriteLockCount(lockPath));
    assertEquals(false, lockUtil.releaseWriteLock(lockPath));
    assertEquals(0, lockUtil.getWriteLockCount(lockPath));
    lockUtil.acquireWriteLock(lockPath);
    assertEquals(1, lockUtil.getWriteLockCount(lockPath));
    assertEquals(true, lockUtil.resetWriteLock(lockPath));
    assertEquals(0, lockUtil.getWriteLockCount(lockPath));

    lockUtil.close();
  }

  @Test
  public void testMulitpleWriteLocks() throws Exception {
    LOG.info("========== testMulitpleWriteLocks ==========");
    String path = "/service/99/multiplewritelocks/logs/20120203/04/component/";

    final String lockPath;
    {
      LockUtil lockUtil = new LockUtil(conf);
      lockPath = lockUtil.getLockPath(new PathInfo(path));
    }

    final List<Integer> results = new ArrayList<Integer>();

    final BlockingQueue<Integer> q0 = new SynchronousQueue<Integer>();
    final BlockingQueue<Integer> q1 = new SynchronousQueue<Integer>();
    final BlockingQueue<Integer> q2 = new SynchronousQueue<Integer>();

    // Run three threads, that get write locks in a particular order. Add
    // decreasing delays, so that if the locks don't work, the numbers will come
    // out in reverse order.
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          q0.take();
          LockUtil lockUtil = new LockUtil(conf);
          lockUtil.acquireWriteLock(lockPath);
          q1.put(0);

          Thread.sleep(1000);

          results.add(1);

          lockUtil.releaseWriteLock(lockPath);

          lockUtil.close();
        } catch (Exception e) {
          LOG.error("Exception", e);
        }
      }
    });

    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          q1.take();
          LockUtil lockUtil = new LockUtil(conf);
          lockUtil.acquireWriteLock(lockPath);
          q2.put(0);

          Thread.sleep(500);

          results.add(2);

          lockUtil.releaseWriteLock(lockPath);

          lockUtil.close();
        } catch (Exception e) {
          LOG.error("Exception", e);
        }
      }
    });

    Thread t3 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          q2.take();
          LockUtil lockUtil = new LockUtil(conf);
          lockUtil.acquireWriteLock(lockPath);

          results.add(3);

          lockUtil.releaseWriteLock(lockPath);

          lockUtil.close();
        } catch (Exception e) {
          LOG.error("Exception", e);
        }
      }
    });

    t3.start();
    t2.start();
    t1.start();

    q0.put(0);

    t1.join();
    t2.join();
    t3.join();

    assertEquals(new Integer(1), results.get(0));
    assertEquals(new Integer(2), results.get(1));
    assertEquals(new Integer(3), results.get(2));
  }

  @Test
  public void testReadWriteLocks() throws Exception {
    LOG.info("========== testReadWriteLocks ==========");
    String path = "/service/99/readwritelocks/logs/20120203/04/component/";

    final String lockPath;
    {
      LockUtil lockUtil = new LockUtil(conf);
      lockPath = lockUtil.getLockPath(new PathInfo(path));
    }

    final Integer[] value = { 0 };

    final BlockingQueue<Integer> q0 = new SynchronousQueue<Integer>();
    final BlockingQueue<Integer> q1 = new SynchronousQueue<Integer>();
    final BlockingQueue<Integer> q2 = new SynchronousQueue<Integer>();

    // Run three threads, that get write locks in a particular order. Add
    // decreasing delays, so that if the locks don't work, the numbers will come
    // out in reverse order.
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          q0.take();
          LockUtil lockUtil = new LockUtil(conf);
          lockUtil.acquireReadLock(lockPath);
          q1.put(0);

          Thread.sleep(1000);

          assertEquals(new Integer(0), value[0]);

          lockUtil.releaseReadLock(lockPath);

          lockUtil.close();
        } catch (Exception e) {
          LOG.error("Exception", e);
        }
      }
    });

    Thread t2 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          q1.take();
          LockUtil lockUtil = new LockUtil(conf);
          lockUtil.acquireWriteLock(lockPath);
          q2.put(0);

          Thread.sleep(500);

          value[0] = 1;

          lockUtil.releaseWriteLock(lockPath);

          lockUtil.close();
        } catch (Exception e) {
          LOG.error("Exception", e);
        }
      }
    });

    Thread t3 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          q2.take();
          LockUtil lockUtil = new LockUtil(conf);
          lockUtil.acquireReadLock(lockPath);

          assertEquals(new Integer(1), value[0]);

          lockUtil.releaseReadLock(lockPath);

          lockUtil.close();
        } catch (Exception e) {
          LOG.error("Exception", e);
        }
      }
    });

    t3.start();
    t2.start();
    t1.start();

    q0.put(0);

    t1.join();
    t2.join();
    t3.join();

    LockUtil lockUtil = new LockUtil(conf);
    assertEquals(0, lockUtil.getReadLockCount(lockPath));
    assertEquals(0, lockUtil.getWriteLockCount(lockPath));
    lockUtil.close();
  }

  @After
  public void cleanup() throws InterruptedException, IOException,
      SecurityException, NoSuchMethodException, IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    server.shutdown();

    Method shutdown = factoryClass.getMethod("shutdown", new Class<?>[] {});
    shutdown.invoke(standaloneServerFactory, new Object[] {});

    while (dir.exists()) {
      LOG.info("deleting {}", dir);
      FileUtils.deleteDirectory(dir);
    }
  }
}
