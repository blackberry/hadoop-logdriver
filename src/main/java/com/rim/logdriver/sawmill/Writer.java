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

package com.rim.logdriver.sawmill;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.timestamp.Rfc5424TimestampParser;
import com.rim.logdriver.timestamp.TimestampParser;

public class Writer implements Runnable, WriterMBean {
  private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

  private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

  private final String uuid = UUID.randomUUID().toString();
  private final TimestampParser timestampParser = new Rfc5424TimestampParser();
  private final FsPermission permissions = new FsPermission(
      FsAction.READ_WRITE, FsAction.READ, FsAction.NONE);
  private Long blocksize;
  private Short replicas;
  private Integer bufferSize;

  private String name;

  private String hostname;

  private Properties conf;
  private Configuration hConf;
  private BlockingQueue<String> queue;

  private static final Object fsLock = new Object();
  private FileSystem fs;
  private String fileTemplate;
  private long rotateInterval;

  private Map<String, FileInfo> fileMap = new HashMap<String, FileInfo>();
  private int index = 0;

  private String proxyUserName;

  // Metrics!
  private long linesRead = 0;
  private long linesWritten = 0;
  private long boomBlocksWritten = 0;
  private long averageLinesPerBoomBlock;
  private long errors = 0;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setConfig(Properties conf) {
    this.conf = conf;
  }

  public void setHadoopConf(Configuration hConf) {
    this.hConf = hConf;
  }

  public void setQueue(BlockingQueue<String> queue) {
    this.queue = queue;
  }

  @Override
  public long getLinesRead() {
    return linesRead;
  }

  @Override
  public long getLinesWritten() {
    return linesWritten;
  }

  @Override
  public long getBoomBlocksWritten() {
    return boomBlocksWritten;
  }

  @Override
  public long getAverageLinesPerBoomBlock() {
    return averageLinesPerBoomBlock;
  }

  @Override
  public long getErrors() {
    return errors;
  }

  @Override
  public int getQueueSize() {
    return queue.size();
  }

  @Override
  public long getReadNotWritten() {
    return linesRead - linesWritten;
  }

  public void init() {
    blocksize = Configs.hdfsBlockSize.getLong(conf);
    replicas = Configs.hdfsReplicas.getShort(conf);
    bufferSize = Configs.hdfsBufferSize.getInteger(conf);

    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      LOG.error("[{}] Can't determine local hostname", name);
      ++errors;
      hostname = "unknown.host";
    }

    rotateInterval = Configs.fileRotateInterval.getLong(conf) * 1000;

    fileTemplate = Configs.filePathTemplate.get(conf);

    proxyUserName = Configs.hdfsProxyUser.get(conf);
    if (proxyUserName == null) {
      proxyUserName = "";
    }
  }

  public void runAndClose() throws IOException, InterruptedException {
    // First, run to clear the queue.
    LOG.info("[{}] Running the queue one final time.", name);
    run();

    for (Entry<String, FileInfo> e : new HashSet<Entry<String, FileInfo>>(
        fileMap.entrySet())) {
      String key = e.getKey();
      final FileInfo fi = e.getValue();

      LOG.info("[{}] Closing {}", name, fi.tmpName);
      try {
        Authenticator.getInstance().runPrivileged(proxyUserName,
            new PrivilegedExceptionAction<Void>() {
              @Override
              public Void run() throws Exception {
                try {
                  fi.serializer.flush();
                  fi.out.flush();
                  fi.out.sync();
                  fi.out.close();
                  fs.rename(new Path(fi.tmpName), new Path(fi.finalName));

                  boomBlocksWritten += fi.serializer.getBoomBlocksWritten();
                  linesWritten += fi.serializer.getLinesWritten();
                } catch (IOException ex) {
                  LOG.error("[{}] Error closing file {}", new Object[] { name,
                      fi.tmpName }, ex);
                  ++errors;
                }
                return null;
              }
            });
      } catch (Exception ex) {
        LOG.error("Error closing file {}.  Trying to reauthenticate.",
            fi.tmpName, ex);
        ++errors;
      }
      fileMap.remove(key);
    }
  }

  @Override
  public void run() {
    try {
      // First, check if any files need closing.
      long cutoffTime = System.currentTimeMillis() - rotateInterval;
      for (Entry<String, FileInfo> e : new HashSet<Entry<String, FileInfo>>(
          fileMap.entrySet())) {
        String key = e.getKey();
        final FileInfo fi = e.getValue();
        if (fi.createTime < cutoffTime) {
          LOG.debug("[{}] Closing {}", name, fi.tmpName);
          try {
            Authenticator.getInstance().runPrivileged(proxyUserName,
                new PrivilegedExceptionAction<Void>() {
                  @Override
                  public Void run() throws Exception {
                    try {
                      fi.serializer.flush();
                      fi.out.flush();
                      fi.out.sync();
                      fi.out.close();
                      fs.rename(new Path(fi.tmpName), new Path(fi.finalName));

                      boomBlocksWritten += fi.serializer.getBoomBlocksWritten();
                      linesWritten += fi.serializer.getLinesWritten();
                    } catch (IOException ex) {
                      LOG.error("[{}] Error closing file {}", new Object[] {
                          name, fi.tmpName }, ex);
                      ++errors;
                    }
                    return null;
                  }
                });
          } catch (Exception ex) {
            LOG.error("Error closing file {}.  Trying to reauthenticate.",
                fi.tmpName, ex);
            ++errors;
          }
          fileMap.remove(key);
        }
      }

      int linesProcessed = 0;
      String priorityString = null;
      int priority;
      long timestamp;
      String message;
      String[] tsAndMsg;
      while (true) {
        // Get the line
        String line = queue.poll();
        if (line == null) {
          LOG.debug("[{}] Processed {} lines.", name, linesProcessed);
          return;
        }
        ++linesProcessed;
        ++linesRead;
        LOG.trace("[{}] LINE:{}", name, line);

        // Strip leading priorities.
        if (line.charAt(0) == '<') {
          int closingBracket = line.indexOf('>');
          if (closingBracket > 1 && closingBracket <= 4) {
            priorityString = line.substring(1, closingBracket);
            priority = Integer.parseInt(priorityString, 10);
            if (priority >= 0 && priority <= 191
                && priorityString.equals(Integer.toString(priority, 10))) {
              line = line.substring(closingBracket + 1);
            }
          }
        }

        // First, extract the timestamp

        LOG.trace("[{}] Splitting line", name);
        tsAndMsg = timestampParser.splitLine(line);
        if (tsAndMsg[0] == null) {
          LOG.error("[{}] Error extracting timestamp from:{}", name, line);
          ++errors;
          timestamp = System.currentTimeMillis();
          message = line;
        } else {
          message = tsAndMsg[1];

          LOG.trace("[{}] Getting timestamp", name);
          try {
            timestamp = timestampParser.parseTimestatmp(tsAndMsg[0]);
          } catch (ParseException e) {
            LOG.info("Error parsing timestamp from line. Error:{}, Line:{}",
                e.toString(), line);
            ++errors;
            timestamp = System.currentTimeMillis();
            message = line;
          }
        }

        BoomSerializer serializer = getSerializerForTimestamp(timestamp);

        serializer.write(timestamp, message);
      }
    } catch (Throwable t) {
      LOG.error("[{}] Something bad happened.", name, t);
      ++errors;

      // Sleep for a couple of seconds. This prevents us from going into a tight
      // loop when
      // HDFS is unavailable.
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        // do nothing
      }
    }
  }

  private BoomSerializer getSerializerForTimestamp(long timestamp) {
    String fileName = fillInTemplate(fileTemplate, timestamp);

    FileInfo fi = fileMap.get(fileName);

    if (fi == null) {
      final FileInfo newFi = new FileInfo();

      StringBuilder sb = new StringBuilder().append(fileName).append('.')
          .append(uuid).append('.').append(index).append(".bm");
      newFi.finalName = sb.toString();
      newFi.tmpName = sb.append(".tmp").toString();

      newFi.createTime = System.currentTimeMillis();
      LOG.debug("[{}] Creating {}", name, newFi.tmpName);
      try {
        Authenticator.getInstance().runPrivileged(proxyUserName,
            new PrivilegedExceptionAction<Void>() {

              @Override
              public Void run() throws Exception {
                // Apparently getting a FileSystem from a path is not thread
                // safe
                synchronized (fsLock) {
                  try {
                    fs = new Path(fileTemplate).getFileSystem(hConf);
                  } catch (IOException e) {
                    LOG.error("[{}] Error getting File System.", name, e);
                    ++errors;
                  }
                }
                newFi.out = fs.create(new Path(newFi.tmpName), permissions,
                    false, bufferSize, replicas, blocksize, null);
                return null;
              }
            });
      } catch (Exception e) {
        LOG.error("[{}] Error creating file.", name, e);
        ++errors;
      }
      ++index;
      newFi.serializer = new BoomSerializer(newFi.out, conf);
      try {
        newFi.serializer.afterCreate();
      } catch (IOException e) {
        LOG.error("[{}] Error with serializer", name, e);
        ++errors;
      }
      fileMap.put(fileName, newFi);
      fi = newFi;
    }

    return fi.serializer;
  }

  private String fillInTemplate(String template, long timestamp) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(UTC);
    cal.setTimeInMillis(timestamp);

    long templateLength = template.length();

    StringBuilder sb = new StringBuilder();
    int i = 0;
    int p = 0;
    char c;
    while (true) {
      p = template.indexOf('%', i);
      if (p == -1) {
        sb.append(template.substring(i));
        break;
      }
      sb.append(template.substring(i, p));

      if (p + 1 < templateLength) {
        c = template.charAt(p + 1);
        switch (c) {
        case 'y':
          sb.append(String.format("%04d", cal.get(Calendar.YEAR)));
          break;
        case 'M':
          sb.append(String.format("%02d", cal.get(Calendar.MONTH) + 1));
          break;
        case 'd':
          sb.append(String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
          break;
        case 'H':
          sb.append(String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
          break;
        case 'm':
          sb.append(String.format("%02d", cal.get(Calendar.MINUTE)));
          break;
        case 's':
          sb.append(String.format("%02d", cal.get(Calendar.SECOND)));
          break;
        case 'l':
          sb.append(hostname);
          break;
        default:
          sb.append('%').append(c);
        }
      } else {
        sb.append('%');
        break;
      }

      i = p + 2;

      if (i >= templateLength) {
        break;
      }
    }

    return sb.toString();
  }

  private static class FileInfo {
    private String tmpName;
    private String finalName;
    private FSDataOutputStream out;
    private BoomSerializer serializer;
    private long createTime;
  }

}
