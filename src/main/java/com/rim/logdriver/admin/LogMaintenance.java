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

package com.rim.logdriver.admin;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.LockedFs;
import com.rim.logdriver.fs.PathInfo;
import com.rim.logdriver.locks.LockInfo;
import com.rim.logdriver.locks.LockUtil;
import com.rim.logdriver.mapreduce.boom.BoomFilterMapper;
import com.rim.logdriver.oozie.SynchronizedOozieClient;

public class LogMaintenance extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(LogMaintenance.class);

  private static final Pattern VALID_FILE = Pattern.compile(".*([0-9]|\\.bm)$");
  private static final String READY_MARKER = "_READY";

  // How long to wait after a directory stops being written to, before we start
  // processing.
  private static final long WAIT_TIME = 10 * 60 * 1000l;

  private String oozieUrl;
  private OozieClient oozieClient = null;

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    // If run by Oozie, then load the Oozie conf too
    if (System.getProperty("oozie.action.conf.xml") != null) {
      conf.addResource(new URL("file://"
          + System.getProperty("oozie.action.conf.xml")));
    }

    // For some reason, Oozie needs some options to be set in system instead of
    // in the confiuration. So copy the configs over.
    {
      Iterator<Entry<String, String>> i = conf.iterator();
      while (i.hasNext()) {
        Entry<String, String> next = i.next();
        System.setProperty(next.getKey(), next.getValue());
      }
    }

    if (args.length < 3) {
      printUsage();
      return 1;
    }

    String userName = args[0];
    String dcNumber = args[1];
    String service = args[2];
    String date = null;
    String hour = null;
    if (args.length >= 4) {
      date = args[3];
    }
    if (args.length >= 5) {
      hour = args[4];
    }

    // Set from environment variables
    oozieUrl = getConfOrEnv(conf, "OOZIE_URL");
    String mergeJobPropertiesFile = getConfOrEnv(conf, "MERGEJOB_CONF");
    String filterJobPropertiesFile = getConfOrEnv(conf, "FILTERJOB_CONF");
    String daysBeforeArchive = getConfOrEnv(conf, "DAYS_BEFORE_ARCHIVE");
    String daysBeforeDelete = getConfOrEnv(conf, "DAYS_BEFORE_DELETE");
    String maxConcurrentMergeJobs = getConfOrEnv(conf,
        "MAX_CONCURRENT_MERGE_JOBS");
    String maxConcurrentFilterJobs = getConfOrEnv(conf,
        "MAX_CONCURRENT_FILTER_JOBS");
    String zkConnectString = getConfOrEnv(conf, "ZK_CONNECT_STRING");
    String logdir = getConfOrEnv(conf, "logdriver.logdir.name");
    boolean resetOrphanedJobs = Boolean.parseBoolean(getConfOrEnv(conf,
        "reset.orphaned.jobs"));
    String rootDir = getConfOrEnv(conf, "service.root.dir");

    boolean doMerge = true;
    boolean doArchive = true;
    boolean doDelete = true;

    if (oozieUrl == null) {
      LOG.info("OOZIE_URL is not set.  Not merging or archiving.");
      doMerge = false;
      doArchive = false;
    }
    if (zkConnectString == null) {
      LOG.error("ZK_CONNECT_STRING is not set.  Exiting.");
      return 1;
    }
    if (mergeJobPropertiesFile == null) {
      LOG.info("MERGEJOB_CONF is not set.  Not merging.");
      doMerge = false;
    }
    if (filterJobPropertiesFile == null) {
      LOG.info("FILTERJOB_CONF is not set.  Not archiving.");
      doArchive = false;
    }
    if (daysBeforeArchive == null) {
      LOG.info("DAYS_BEFORE_ARCHIVE is not set.  Not archiving.");
      doArchive = false;
    }
    if (doArchive && Integer.parseInt(daysBeforeArchive) < 0) {
      LOG.info("DAYS_BEFORE_ARCHIVE is negative.  Not archiving.");
      doArchive = false;
    }
    if (daysBeforeDelete == null) {
      LOG.info("DAYS_BEFORE_DELETE is not set.  Not deleting.");
      doDelete = false;
    }
    if (doDelete && Integer.parseInt(daysBeforeDelete) < 0) {
      LOG.info("DAYS_BEFORE_DELETE is negative.  Not deleting.");
      doDelete = false;
    }
    if (maxConcurrentMergeJobs == null) {
      LOG.info("MAX_CONCURRENT_MERGE_JOBS is not set.  Using default value of -1.");
      maxConcurrentMergeJobs = "-1";
    }
    if (maxConcurrentFilterJobs == null) {
      LOG.info("MAX_CONCURRENT_FILTER_JOBS is not set.  Using default value of -1.");
      maxConcurrentMergeJobs = "-1";
    }
    if (logdir == null) {
      LOG.info("LOGDRIVER_LOGDIR_NAME is not set.  Using default value of 'logs'.");
      logdir = "logs";
    }
    if (rootDir == null) {
      LOG.info("SERVICE_ROOT_DIR is not set.  Using default value of 'service'.");
      rootDir = "/service";
    }

    // Now it's safe to create our Oozie Runners.
    OozieRunner mergeOozieRunner = new OozieRunner(oozieUrl,
        Integer.parseInt(maxConcurrentMergeJobs));
    Thread mergeOozieRunnerThread = new Thread(mergeOozieRunner);
    mergeOozieRunnerThread.setName("OozieRunner - Merge");
    mergeOozieRunnerThread.setDaemon(false);
    mergeOozieRunnerThread.start();

    OozieRunner filterOozieRunner = new OozieRunner(oozieUrl,
        Integer.parseInt(maxConcurrentFilterJobs));
    Thread filterOozieRunnerThread = new Thread(filterOozieRunner);
    filterOozieRunnerThread.setName("OozieRunner - Filter");
    filterOozieRunnerThread.setDaemon(false);
    filterOozieRunnerThread.start();

    // Figure out what date we start filters on.
    String filterCutoffDate = "";
    if (doArchive) {
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DAY_OF_MONTH, Integer.parseInt("-" + daysBeforeArchive));
      filterCutoffDate = String.format("%04d%02d%02d%02d",
          cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH) + 1),
          cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY));
      LOG.info("Archiving logs from before {}", filterCutoffDate);
    }
    String deleteCutoffDate = "";
    if (doDelete) {
      Calendar cal = Calendar.getInstance();
      cal.add(Calendar.DAY_OF_MONTH, Integer.parseInt("-" + daysBeforeDelete));
      deleteCutoffDate = String.format("%04d%02d%02d%02d",
          cal.get(Calendar.YEAR), (cal.get(Calendar.MONTH) + 1),
          cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY));
      LOG.info("Deleting logs from before {}", deleteCutoffDate);
    }

    long now = System.currentTimeMillis();

    // Various exceptions have been popping up here. So make sure I catch them
    // all.
    try {
      // We can hang if this fails. So make sure we abort if it fails.
      FileSystem fs = null;
      try {
        fs = FileSystem.get(conf);
        fs.exists(new Path("/")); // Test if it works.
      } catch (IOException e) {
        LOG.error("Error getting filesystem.", e);
        return 1;
      }
      // We'll need an Oozie client to check on orphaned directories.
      oozieClient = getOozieClient();

      // LockUtils are used in a couple of places
      LockUtil lu = new LockUtil(zkConnectString);

      // Patterns to recognize hour, day and incoming directories, so that they
      // can be processed.
      Pattern datePathPattern;
      Pattern hourPathPattern;
      Pattern incomingPathPattern;
      Pattern dataPathPattern;
      Pattern archivePathPattern;
      Pattern workingPathPattern;
      if (hour != null) {
        datePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")");
        hourPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")");
        incomingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/incoming");
        dataPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/data");
        archivePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/archive");
        workingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")/("
            + Pattern.quote(hour) + ")/([^/]+)/working/([^/]+)_(\\d+)");
      } else if (date != null) {
        datePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date) + ")");
        hourPathPattern = Pattern
            .compile(rootDir + "/" + Pattern.quote(dcNumber) + "/"
                + Pattern.quote(service) + "/" + Pattern.quote(logdir) + "/("
                + Pattern.quote(date) + ")/(\\d{2})");
        incomingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/incoming");
        dataPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/data");
        archivePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/archive");
        workingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(" + Pattern.quote(date)
            + ")/(\\d{2})/([^/]+)/working/([^/]+)_(\\d+)");
      } else {
        datePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})");
        hourPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})");
        incomingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})/([^/]+)/incoming");
        dataPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})/([^/]+)/data");
        archivePathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir) + "/(\\d{8})/(\\d{2})/([^/]+)/archive");
        workingPathPattern = Pattern.compile(rootDir + "/"
            + Pattern.quote(dcNumber) + "/" + Pattern.quote(service) + "/"
            + Pattern.quote(logdir)
            + "/(\\d{8})/(\\d{2})/([^/]+)/working/([^/]+)_(\\d+)");
      }

      // Do a depth first search of the directory, processing anything that
      // looks
      // interesting along the way
      Deque<Path> paths = new ArrayDeque<Path>();
      Path rootPath = new Path(rootDir + "/" + dcNumber + "/" + service + "/"
          + logdir + "/");
      paths.push(rootPath);

      while (paths.size() > 0) {
        Path p = paths.pop();
        LOG.debug("{}", p.toString());

        if (!fs.exists(p)) {
          continue;
        }

        FileStatus dirStatus = fs.getFileStatus(p);
        FileStatus[] children = fs.listStatus(p);
        boolean addChildren = true;

        boolean old = dirStatus.getModificationTime() < now - WAIT_TIME;
        LOG.debug("    Was last modified {}ms ago",
            now - dirStatus.getModificationTime());

        if (!old) {
          LOG.debug("    Skipping, since it's not old enough.");

        } else if ((!rootPath.equals(p))
            && (children.length == 0 || (children.length == 1 && children[0]
                .getPath().getName().equals(READY_MARKER)))) {
          // old and no children? Delete!
          LOG.info("    Deleting empty directory {}", p.toString());
          fs.delete(p, true);

        } else {
          Matcher matcher = datePathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.debug("Checking date directory");

            // If this is already done, then skip it. So only process if it
            // doesn't exist.
            if (fs.exists(new Path(p, READY_MARKER)) == false) {
              // Check each subdirectory. If they all have ready markers, then I
              // guess we're ready.
              boolean ready = true;
              for (FileStatus c : children) {
                if (c.isDir()
                    && fs.exists(new Path(c.getPath(), READY_MARKER)) == false) {
                  ready = false;
                  break;
                }
              }

              if (ready) {
                fs.createNewFile(new Path(p, READY_MARKER));
              }
            }
          }

          matcher = hourPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.debug("Checking hour directory");

            // If this is already done, then skip it. So only process if it
            // doesn't exist.
            if (fs.exists(new Path(p, READY_MARKER)) == false) {
              // Check each subdirectory. If they all have ready markers, then I
              // guess we're ready.
              boolean ready = true;
              for (FileStatus c : children) {
                if (c.isDir()
                    && fs.exists(new Path(c.getPath(), READY_MARKER)) == false) {
                  ready = false;
                  break;
                }
              }

              if (ready) {
                fs.createNewFile(new Path(p, READY_MARKER));
              }
            }
          }

          // Check to see if we have to run a merge
          matcher = incomingPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.debug("Checking incoming directory");
            String matchDate = matcher.group(1);
            String matchHour = matcher.group(2);
            String matchComponent = matcher.group(3);

            String timestamp = matchDate + matchHour;

            if (doDelete && timestamp.compareTo(deleteCutoffDate) < 0) {
              LOG.info("Deleting old directory: {}", p);
              fs.delete(p, true);
              addChildren = false;
            } else if (doMerge) {

              // old, looks right, and has children? Run it!
              boolean hasMatchingChildren = false;
              boolean subdirTooYoung = false;

              for (FileStatus child : children) {
                if (!hasMatchingChildren) {
                  FileStatus[] grandchildren = fs.listStatus(child.getPath());
                  for (FileStatus gc : grandchildren) {
                    if (VALID_FILE.matcher(gc.getPath().getName()).matches()) {
                      hasMatchingChildren = true;
                      break;
                    }
                  }
                }
                if (!subdirTooYoung) {
                  if (child.getModificationTime() >= now - WAIT_TIME) {
                    subdirTooYoung = true;
                    LOG.debug("    Subdir {} is too young.", child.getPath());
                  }
                }
              }

              if (!hasMatchingChildren) {
                LOG.debug("    No files match the expected pattern ({})",
                    VALID_FILE.pattern());
              }

              if (hasMatchingChildren && !subdirTooYoung) {
                LOG.info("    Run Merge job {} :: {} {} {} {} {}",
                    new Object[] { p.toString(), dcNumber, service, matchDate,
                        matchHour, matchComponent });

                Properties oozieJobProps = new Properties();
                oozieJobProps.load(new FileInputStream(mergeJobPropertiesFile));

                oozieJobProps.setProperty("rootDir", rootDir);
                oozieJobProps.setProperty("dcNumber", dcNumber);
                oozieJobProps.setProperty("service", service);
                oozieJobProps.setProperty("date", matchDate);
                oozieJobProps.setProperty("hour", matchHour);
                oozieJobProps.setProperty("component", matchComponent);
                oozieJobProps.setProperty("user.name", userName);
                oozieJobProps.setProperty("logdir", logdir);

                mergeOozieRunner.submit(oozieJobProps);

                addChildren = false;
              }
            }
          }

          // Check to see if we need to run a filter and archive
          matcher = dataPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            String matchDate = matcher.group(1);
            String matchHour = matcher.group(2);
            String matchComponent = matcher.group(3);

            String timestamp = matchDate + matchHour;

            if (doDelete && timestamp.compareTo(deleteCutoffDate) < 0) {
              LOG.info("Deleting old directory: {}", p);
              fs.delete(p, true);
              addChildren = false;
            } else if (doArchive && timestamp.compareTo(filterCutoffDate) < 0) {

              Properties oozieJobProps = new Properties();
              oozieJobProps.load(new FileInputStream(filterJobPropertiesFile));

              oozieJobProps.setProperty("rootDir", rootDir);
              oozieJobProps.setProperty("dcNumber", dcNumber);
              oozieJobProps.setProperty("service", service);
              oozieJobProps.setProperty("date", matchDate);
              oozieJobProps.setProperty("hour", matchHour);
              oozieJobProps.setProperty("component", matchComponent);
              oozieJobProps.setProperty("user.name", userName);
              oozieJobProps.setProperty("logdir", logdir);

              // Check to see if we should just keep all or delete all here.
              // The filter file should be here
              String appPath = oozieJobProps
                  .getProperty("oozie.wf.application.path");
              appPath = appPath.replaceFirst("\\$\\{.*?\\}", "");
              Path filterFile = new Path(appPath + "/" + service + ".yaml");
              LOG.info("Filter file is {}", filterFile);
              if (fs.exists(filterFile)) {
                List<BoomFilterMapper.Filter> filters = BoomFilterMapper
                    .loadFilters(matchComponent, fs.open(filterFile));

                if (filters == null) {
                  LOG.warn(
                      "    Got null when getting filters.  Not processing. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                } else if (filters.size() == 0) {
                  LOG.warn(
                      "    Got no filters.  Not processing. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                } else if (filters.size() == 1
                    && filters.get(0) instanceof BoomFilterMapper.KeepAllFilter) {
                  LOG.info("    Keeping everything. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                  // Move files from data to archive
                  // delete it all!
                  String destination = rootDir + "/" + dcNumber + "/" + service
                      + "/" + logdir + "/" + matchDate + "/" + matchHour + "/"
                      + matchComponent + "/archive/";

                  String[] moveArgs = { zkConnectString, dcNumber, service,
                      matchDate, matchHour, matchComponent,
                      "move " + p.toUri().getPath() + " " + destination };
                  ToolRunner.run(new Configuration(), new LockedFs(), moveArgs);
                } else if (filters.size() == 1
                    && filters.get(0) instanceof BoomFilterMapper.DropAllFilter) {
                  LOG.info("    Dropping everything. {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                  // delete it all!
                  String[] delArgs = { zkConnectString, dcNumber, service,
                      matchDate, matchHour, matchComponent,
                      "delete " + p.toUri().getPath() };
                  ToolRunner.run(new Configuration(), new LockedFs(), delArgs);
                } else {
                  LOG.info("    Run Filter/Archive job {} :: {} {} {} {} {}",
                      new Object[] { p.toString(), dcNumber, service,
                          matchDate, matchHour, matchComponent });
                  filterOozieRunner.submit(oozieJobProps);
                }
              } else {
                LOG.warn("Skipping filter job, since no filter file exists");
              }

              addChildren = false;
            }
          }

          matcher = archivePathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            String matchDate = matcher.group(1);
            String matchHour = matcher.group(2);

            String timestamp = matchDate + matchHour;

            if (doDelete && timestamp.compareTo(deleteCutoffDate) < 0) {
              LOG.info("Deleting old directory: {}", p);
              fs.delete(p, true);
              addChildren = false;
            }
          }

          matcher = workingPathPattern.matcher(p.toUri().getPath());
          if (matcher.matches()) {
            LOG.info("  Matches working pattern");
            if (resetOrphanedJobs) {
              String matchDate = matcher.group(1);
              String matchHour = matcher.group(2);
              String matchComponent = matcher.group(3);
              String matchOozieJobId = matcher.group(4);

              // Check to see what's up with the oozie job. If it's still
              // running,
              // we don't want to touch it.
              Status status = null;
              try {
                WorkflowJob jobInfo = oozieClient.getJobInfo(matchOozieJobId);
                status = jobInfo.getStatus();
              } catch (OozieClientException e) {
                if (e.getMessage() != null
                    && e.getMessage().contains("Job does not exist")) {
                  LOG.info(
                      "Oozie job not found.  Proceeding as though job was failed.",
                      e);
                  status = Status.FAILED;
                } else {
                  LOG.error("Oozie client error.  Not Proceeding.", e);
                }
              }
              LOG.info("  Oozie job status is {}", status);
              if (status != null && status != Status.RUNNING
                  && status != Status.PREP && status != Status.SUSPENDED) {
                // Move everything from working/xxx/incoming/ to incoming/
                PathInfo lockPathInfo = new PathInfo(rootDir + "/" + dcNumber
                    + "/" + service + "/" + logdir + "/" + matchDate + "/"
                    + matchHour + "/" + matchComponent);
                lu.acquireWriteLock(lu.getLockPath(lockPathInfo));

                FileStatus[] fileStatuses = fs.listStatus(new Path(p.toUri()
                    .getPath() + "/incoming/"));
                if (fileStatuses != null) {
                  for (FileStatus fileStatus : fileStatuses) {
                    Path toPath = new Path(fileStatus.getPath().getParent()
                        .getParent().getParent().getParent(), "incoming/"
                        + fileStatus.getPath().getName());

                    LOG.info("  Moving data from {} to {}",
                        fileStatus.getPath(), toPath);
                    LOG.info("    mkdir {}", toPath);
                    fs.mkdirs(toPath);

                    Path fromDir = new Path(p.toUri().getPath(), "incoming/"
                        + fileStatus.getPath().getName());
                    LOG.info("    moving from {}", fromDir);
                    FileStatus[] files = fs.listStatus(fromDir);
                    if (files == null || files.length == 0) {
                      LOG.info("    Nothing to move from  {}", fromDir);
                    } else {
                      for (FileStatus f : files) {
                        LOG.info("    rename {} {}", f.getPath(), new Path(
                            toPath, f.getPath().getName()));
                        fs.rename(f.getPath(), new Path(toPath, f.getPath()
                            .getName()));
                      }
                    }

                    LOG.info("    rm {}", fileStatus.getPath().getParent()
                        .getParent());
                    fs.delete(fileStatus.getPath().getParent().getParent(),
                        true);
                  }

                  lu.releaseWriteLock(lu.getLockPath(lockPathInfo));

                }
              }
            }

            addChildren = false;
          }
        }

        // Add any children which are directories to the stack.
        if (addChildren) {
          for (int i = children.length - 1; i >= 0; i--) {
            FileStatus child = children[i];
            if (child.isDir()) {
              paths.push(child.getPath());
            }
          }
        }
      }

      // Since we may have deleted a bunch of directories, delete any unused
      // locks
      // from ZooKeeper.
      {
        LOG.info("Checking for unused locks in ZooKeeper");
        String scanPath = rootDir + "/" + dcNumber + "/" + service + "/"
            + logdir;
        if (date != null) {
          scanPath += "/" + date;
          if (hour != null) {
            scanPath += "/" + hour;
          }
        }

        List<LockInfo> lockInfo = lu.scan(scanPath);

        for (LockInfo li : lockInfo) {
          // Check if the lock path still exists in HDFS. If it doesn't, then
          // delete it from ZooKeeper.
          String path = li.getPath();
          String hdfsPath = path.substring(LockUtil.ROOT.length());
          if (!fs.exists(new Path(hdfsPath))) {
            ZooKeeper zk = lu.getZkClient();

            while (!path.equals(LockUtil.ROOT)) {
              try {
                zk.delete(path, -1);
              } catch (KeeperException.NotEmptyException e) {
                // That's fine. just stop trying then.
                break;
              } catch (Exception e) {
                LOG.error("Caught exception trying to delete from ZooKeeper.",
                    e);
                break;
              }
              LOG.info("Deleted from ZooKeeper: {}", path);
              path = path.substring(0, path.lastIndexOf('/'));
            }

          }
        }
      }
      lu.close();

      // Now that we're done, wait for the Oozie Runner to stop, and print the
      // results.
      LOG.info("Waiting for Oozie jobs to complete.");
      mergeOozieRunner.shutdown();
      mergeOozieRunnerThread.join();
      LOG.info(
          "Oozie Job Stats : Merge  : Started={} Succeeded={} failed={} errors={}",
          new Object[] { mergeOozieRunner.getStarted(),
              mergeOozieRunner.getSucceeded(), mergeOozieRunner.getFailed(),
              mergeOozieRunner.getErrors() });

      filterOozieRunner.shutdown();
      filterOozieRunnerThread.join();
      LOG.info(
          "Oozie Job Stats : Filter : Started={} Succeeded={} failed={} errors={}",
          new Object[] { filterOozieRunner.getStarted(),
              filterOozieRunner.getSucceeded(), filterOozieRunner.getFailed(),
              filterOozieRunner.getErrors() });

    } catch (Exception e) {
      LOG.error("Unexpected exception caught.", e);
      return 1;
    }

    return 0;
  }

  private String getConfOrEnv(Configuration conf, String propertyOrEnv) {
    String property = propertyOrEnv.toLowerCase().replaceAll("_", ".");
    String env = propertyOrEnv.toUpperCase().replaceAll("\\.", "_");
    LOG.debug("Checking {}/{}", property, env);
    String result = conf.get(property, System.getenv(env));
    LOG.info("Option {}/{} = {}", new Object[] { property, env, result });
    return result;
  }

  private synchronized OozieClient getOozieClient() {
    if (oozieClient == null) {
      oozieClient = new SynchronizedOozieClient(new AuthOozieClient(oozieUrl));
    }
    return oozieClient;
  }

  private class OozieRunner implements Runnable {
    private OozieClient client;
    private int maxConcurrentJobs;
    private BlockingQueue<Properties> pendingQueue;
    private Map<String, Properties> runningJobs;

    private int started = 0;
    private int succeeded = 0;
    private int failed = 0;
    private int errors = 0;

    private boolean shutdown = false;

    private OozieRunner(String oozieUrl, int maxConcurrentJobs) {
      client = getOozieClient();
      this.maxConcurrentJobs = maxConcurrentJobs;
      if (this.maxConcurrentJobs < 1) {
        this.maxConcurrentJobs = Integer.MAX_VALUE;
      }

      pendingQueue = new LinkedBlockingQueue<Properties>();
      runningJobs = new HashMap<String, Properties>();
    }

    @Override
    public void run() {
      try {
        while (true) {
          boolean somethingChanged = false;

          // First, check the state of all running jobs
          for (String jobId : new HashSet<String>(runningJobs.keySet())) {
            WorkflowJob jobInfo = client.getJobInfo(jobId);
            switch (jobInfo.getStatus()) {
            case PREP:
            case RUNNING:
              // That's fine. Do nothing.
              break;
            case SUCCEEDED:
              LOG.info("Job {} succeeded.", jobId);
              runningJobs.remove(jobId);
              succeeded++;
              somethingChanged = true;
              break;
            default:
              // Oh oh!
              LOG.error("Job {} is in status {}.  Abandonning job.", jobId,
                  jobInfo.getStatus());
              runningJobs.remove(jobId);
              failed++;
              somethingChanged = true;
            }
          }

          // Next, start up any new jobs that are waiting.
          while (pendingQueue.size() > 0
              && runningJobs.size() < maxConcurrentJobs) {
            Properties prop = pendingQueue.poll();
            if (prop == null) {
              break;
            }

            String jobId = null;
            try {
              jobId = client.run(prop);
              runningJobs.put(jobId, prop);
              started++;
              somethingChanged = true;
            } catch (Exception e) {
              LOG.error("Error submitting Oozie job.", e);
              errors++;
            }
          }

          // Exit if we are done.
          if (pendingQueue.isEmpty() && runningJobs.isEmpty()
              && shutdown == true) {
            break;
          }

          // Finally, just wait a bit before checking again.
          if (somethingChanged == false) {
            Thread.sleep(10000);
          }
        }

      } catch (Throwable t) {
        LOG.error("Unexpected error.  Shutting down.", t);
        System.exit(1);
      }
    }

    public void submit(Properties prop) throws InterruptedException {
      pendingQueue.put(prop);
    }

    public void shutdown() {
      shutdown = true;
    }

    public int getStarted() {
      return started;
    }

    public int getSucceeded() {
      return succeeded;
    }

    public int getFailed() {
      return failed;
    }

    public int getErrors() {
      return errors;
    }

  }

  public void printUsage() {
    System.out.println("Usage: " + this.getClass().getSimpleName()
        + " <user.name> <site number> <service> [yyyymmdd [hh]]");
  }

  public static void main(String[] args) throws Exception {
    if (!System.getProperties().contains(
        AuthOozieClient.USE_AUTH_TOKEN_CACHE_SYS_PROP)) {
      System.setProperty(AuthOozieClient.USE_AUTH_TOKEN_CACHE_SYS_PROP, "true");
    }

    int res = ToolRunner.run(new Configuration(), new LogMaintenance(), args);
    System.exit(res);
  }

}
