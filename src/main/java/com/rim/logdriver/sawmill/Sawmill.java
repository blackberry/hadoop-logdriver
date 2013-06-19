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
 * Logwriter accepts logs of the format [RFC5424 date] [something]
 * and encodes them as Boom files in HDFS.
 */
package com.rim.logdriver.sawmill;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.LineDelimiter;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.util.ExceptionMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.sawmill.mina.TextLineCodecFactory;

public class Sawmill {
  private static final Logger LOG = LoggerFactory.getLogger(Sawmill.class);

  public static void main(String[] args) {
    new Sawmill().run(args);
  }

  public void run(String[] args) {
    if (args.length < 1) {
      System.out.println("Usage: " + this.getClass().getSimpleName()
          + " <config.properties>");
      System.exit(1);
    }

    LOG.info("Starting {}", Sawmill.class.getSimpleName());

    // First arg is the config
    String configFile = args[0];

    // Load configuration.
    Properties conf = new Properties();
    try {
      conf.load(new FileInputStream(configFile));
    } catch (FileNotFoundException e) {
      LOG.error("Config file not found.", e);
      System.exit(1);
    } catch (Throwable t) {
      LOG.error("Error reading config file.", t);
      System.exit(1);
    }

    // Parse the configuration.

    // Load in any Hadoop config files.
    Configuration hConf = new Configuration();
    {
      String[] hadoopConfs = Configs.hadoopConfigPaths.getArray(conf);
      for (String confPath : hadoopConfs) {
        hConf.addResource(new Path(confPath));
      }
      // Also, don't shut down my FileSystem automatically!!!
      hConf.setBoolean("fs.automatic.close", false);
      for (Entry<Object, Object> e : System.getProperties().entrySet()) {
        if (e.getValue() instanceof Integer) {
          hConf.setInt(e.getKey().toString(), (Integer) e.getValue());
        } else if (e.getValue() instanceof Long) {
          hConf.setLong(e.getKey().toString(), (Long) e.getValue());
        } else {
          hConf.set(e.getKey().toString(), e.getValue().toString());
        }
      }
    }

    // Ensure that UserGroupInformation is set up, and knows if security is
    // enabled.
    UserGroupInformation.setConfiguration(hConf);

    // Kerberos credentials. If these are not present, then it just won't try to
    // authenticate.
    String kerbConfPrincipal = Configs.kerberosPrincipal.get(conf);
    String kerbKeytab = Configs.kerberosKeytab.get(conf);
    Authenticator.getInstance().setKerbConfPrincipal(kerbConfPrincipal);
    Authenticator.getInstance().setKerbKeytab(kerbKeytab);

    // Check out the number of threads for workers, and creater the threadpools
    // for both workers and stats updates.
    int threadCount = Configs.threadpoolSize.getInteger(conf);
    final ScheduledExecutorService executor = Executors
        .newScheduledThreadPool(threadCount);

    // Get the MBean server
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    // Set up the Mina Exception Monitor
    ExceptionMonitor.setInstance(new ExceptionLoggerExceptionMonitor());

    // For each port->output mapping, create a path (listener, queue, worker).
    // List<DataPath> paths = new ArrayList<DataPath>();
    final List<IoAcceptor> acceptors = new ArrayList<IoAcceptor>();
    final List<Writer> writers = new ArrayList<Writer>();
    {
      String[] pathStrings = Configs.paths.getArray(conf);
      for (String p : pathStrings) {
        Properties pathConf = Util.subProperties(conf, "path." + p);

        String name = Configs.name.get(pathConf);
        if (name == null) {
          LOG.info("Path has no name.  Using {}", p);
          name = p;
        }
        LOG.info("[{}] Configuring path {}", name, name);

        // Check the properties for this specific instance
        Integer maxLineLength = Configs.tcpMaxLineLength.getInteger(pathConf);
        if (maxLineLength == null) {
          maxLineLength = Configs.defaultTcpMaxLineLength.getInteger(conf);
        }
        LOG.info("[{}] Maximum line length is {}", name, maxLineLength);

        InetAddress bindAddress = null;
        try {
          String address = Configs.bindAddress.get(pathConf);
          bindAddress = InetAddress.getByName(address);
        } catch (UnknownHostException e) {
          LOG.error("[{}] Error getting bindAddress from string {}",
              new Object[] { name, pathConf.getProperty("bindAddress") }, e);
        }

        Integer port = Configs.port.getInteger(pathConf);
        if (port == null) {
          LOG.error("[{}] Port not set.  Skipping this path.", name);
          continue;
        }

        int queueLength = Configs.queueCapacity.getInteger(pathConf);

        // Set up the actual processing chain
        IoAcceptor acceptor = new NioSocketAcceptor();
        SocketSessionConfig sessionConfig = (SocketSessionConfig) acceptor
            .getSessionConfig();
        sessionConfig.setReuseAddress(true);
        acceptors.add(acceptor);

        String charsetName = Configs.charset.getString(pathConf);
        Charset charset = null;
        try {
          charset = Charset.forName(charsetName);
        } catch (UnsupportedCharsetException e) {
          LOG.error(
              "[{}] Charset '{}' is not supported.  Defaulting to UTF-8.",
              name, charsetName);
          charset = Charset.forName("UTF-8");
        }
        LOG.info("[{}] Using character set {}", name, charset.displayName());
        TextLineCodecFactory textLineCodecFactory = new TextLineCodecFactory(
            charset, LineDelimiter.UNIX, LineDelimiter.AUTO);
        textLineCodecFactory.setDecoderMaxLineLength(maxLineLength);
        acceptor.getFilterChain().addLast("textLineCodec",
            new ProtocolCodecFilter(textLineCodecFactory));

        int numBuckets = Configs.outputBuckets.getInteger(pathConf);
        if (numBuckets > 1) {
          // Set up mulitple writers for one MultiEnqueueHandler
          @SuppressWarnings("unchecked")
          BlockingQueue<String>[] queues = new BlockingQueue[numBuckets];

          for (int i = 0; i < numBuckets; i++) {
            BlockingQueue<String> queue = new ArrayBlockingQueue<String>(
                queueLength);
            queues[i] = queue;

            // Set up the processor on the other end.
            Writer writer = new Writer();
            writer.setName(name);
            writer.setConfig(pathConf);
            writer.setHadoopConf(hConf);
            writer.setQueue(queue);
            writer.init();

            // Set up MBean for the Writer
            {
              ObjectName mbeanName = null;
              try {
                mbeanName = new ObjectName(Writer.class.getPackage().getName()
                    + ":type=" + Writer.class.getSimpleName() + " [" + i + "]"
                    + ",name=" + name);
              } catch (MalformedObjectNameException e) {
                LOG.error("[{}] Error creating MBean name.", name, e);
              } catch (NullPointerException e) {
                LOG.error("[{}] Error creating MBean name.", name, e);
              }
              try {
                mbs.registerMBean(writer, mbeanName);
              } catch (InstanceAlreadyExistsException e) {
                LOG.error("[{}] Error registering MBean name.", name, e);
              } catch (MBeanRegistrationException e) {
                LOG.error("[{}] Error registering MBean name.", name, e);
              } catch (NotCompliantMBeanException e) {
                LOG.error("[{}] Error registering MBean name.", name, e);
              }
            }

            executor.scheduleWithFixedDelay(writer, 0, 100,
                TimeUnit.MILLISECONDS);
            writers.add(writer);
          }

          MultiEnqueueHandler handler = new MultiEnqueueHandler(queues);
          acceptor.setHandler(handler);

          // Set up MBean for the MultiEnqueueHandler
          {
            ObjectName mbeanName = null;
            try {
              mbeanName = new ObjectName(MultiEnqueueHandler.class.getPackage()
                  .getName()
                  + ":type="
                  + MultiEnqueueHandler.class.getSimpleName() + ",name=" + name);
            } catch (MalformedObjectNameException e) {
              LOG.error("[{}] Error creating MBean name.", name, e);
            } catch (NullPointerException e) {
              LOG.error("[{}] Error creating MBean name.", name, e);
            }
            try {
              mbs.registerMBean(handler, mbeanName);
            } catch (InstanceAlreadyExistsException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            } catch (MBeanRegistrationException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            } catch (NotCompliantMBeanException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            }
          }
        } else {
          BlockingQueue<String> queue = new ArrayBlockingQueue<String>(
              queueLength);

          // Set up the processor on the other end.
          Writer writer = new Writer();
          writer.setName(name);
          writer.setConfig(pathConf);
          writer.setHadoopConf(hConf);
          writer.setQueue(queue);
          writer.init();

          // Set up MBean for the Writer
          {
            ObjectName mbeanName = null;
            try {
              mbeanName = new ObjectName(Writer.class.getPackage().getName()
                  + ":type=" + Writer.class.getSimpleName() + ",name=" + name);
            } catch (MalformedObjectNameException e) {
              LOG.error("[{}] Error creating MBean name.", name, e);
            } catch (NullPointerException e) {
              LOG.error("[{}] Error creating MBean name.", name, e);
            }
            try {
              mbs.registerMBean(writer, mbeanName);
            } catch (InstanceAlreadyExistsException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            } catch (MBeanRegistrationException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            } catch (NotCompliantMBeanException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            }
          }

          executor
              .scheduleWithFixedDelay(writer, 0, 100, TimeUnit.MILLISECONDS);
          writers.add(writer);

          EnqueueHandler handler = new EnqueueHandler(queue);
          acceptor.setHandler(handler);

          // Set up MBean for the EnqueueHandler
          {
            ObjectName mbeanName = null;
            try {
              mbeanName = new ObjectName(EnqueueHandler.class.getPackage()
                  .getName()
                  + ":type="
                  + EnqueueHandler.class.getSimpleName()
                  + ",name=" + name);
            } catch (MalformedObjectNameException e) {
              LOG.error("[{}] Error creating MBean name.", name, e);
            } catch (NullPointerException e) {
              LOG.error("[{}] Error creating MBean name.", name, e);
            }
            try {
              mbs.registerMBean(handler, mbeanName);
            } catch (InstanceAlreadyExistsException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            } catch (MBeanRegistrationException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            } catch (NotCompliantMBeanException e) {
              LOG.error("[{}] Error registering MBean name.", name, e);
            }
          }
        }

        acceptor.getSessionConfig().setReadBufferSize(
            Configs.tcpReadBufferSize.getInteger(pathConf));
        acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 5);

        while (true) {
          try {
            acceptor.bind(new InetSocketAddress(bindAddress, port));
          } catch (IOException e) {
            LOG.error("Error binding to {}:{}.  Retrying...", bindAddress, port);

            try {
              Thread.sleep(2000);
            } catch (InterruptedException e1) {
              // nothing
            }

            continue;
          }

          break;
        }

      }
    }

    // Register a shutdown hook..
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        LOG.info("Shutting down");

        LOG.info("Unbinding and disposing of all IoAcceptors");
        for (IoAcceptor acceptor : acceptors) {
          acceptor.unbind();
          acceptor.dispose(true);
        }

        LOG.info("Shutting down worker threadpools.  This could take a little while.");
        executor.shutdown();
        try {
          executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
          LOG.error("Interrupted waiting for writer threadpool termination.", e);
        }
        if (!executor.isTerminated()) {
          LOG.error("Threadpool did not terminate cleanly.");
        }

        LOG.info("Cleaning out any remaining messages from the queues.");
        List<Thread> threads = new ArrayList<Thread>();
        for (final Writer writer : writers) {
          Runnable r = new Runnable() {
            @Override
            public void run() {
              try {
                writer.runAndClose();
              } catch (Throwable t) {
                LOG.error("Error shutting down writer [{}]", writer.getName(),
                    t);
              }
            }
          };
          Thread t = new Thread(r);
          t.setDaemon(false);
          t.start();
          threads.add(t);
        }

        for (Thread t : threads) {
          try {
            t.join();
          } catch (InterruptedException e) {
            LOG.error("Interrupted waiting for thread to finish.");
          }
        }

        LOG.info("Closing filesystems.");
        try {
          FileSystem.closeAll();
        } catch (Throwable t) {
          LOG.error("Error closing filesystems.", t);
        }

        LOG.info("Finished shutting down cleanly.");
      }
    });
  }
}
