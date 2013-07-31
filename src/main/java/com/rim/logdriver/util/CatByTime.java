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
 * Search Logs in a given time range, based on the logdriver file structure.
 * <p>
 * Usage: [genericOptions] searchString baseDir filePrefix startTime endTime output
 * <p>
 * For example: -Djob.wait=true 'ERR=12345678' /service/web/logs app 1332939045000 1332942648000 /user/me/grep
 * 
 */
package com.rim.logdriver.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.fs.FileManager;
import com.rim.logdriver.fs.PathInfo;

public class CatByTime extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(CatByTime.class);

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf(); // Configuration processed by ToolRunner
    List<String> searchArgs = new ArrayList<String>();

    if (args.length < 6) {
      System.out.println("Usage: " + this.getClass().getSimpleName()
          + " DC SVC COMP START END OUT");
    }
    String dcNumber = args[0];
    String service = args[1];
    String component = args[2];
    long startTime = Long.parseLong(args[3]);
    long endTime = Long.parseLong(args[4]);
    String output = args[5];

    // Add the start and end time to the configuration
    conf.setLong("logdriver.search.start.time", startTime);
    conf.setLong("logdriver.search.end.time", endTime);

    // Get paths
    FileManager fm = new FileManager(conf);
    List<PathInfo> paths = fm.getPathInfo(dcNumber, service, component,
        startTime, endTime);

    if (paths.isEmpty()) {
      System.err
          .println("No logs found for the given component(s) and time range.");
      return 1;
    }

    int retval = 99;
    try {
      // Lock, then get the real paths
      fm.acquireReadLocks(paths);
      for (PathInfo pi : paths) {
        LOG.info("Adding path: {}", pi.getFullPath());
        searchArgs.addAll(fm.getInputPaths(pi));
      }

      // The last arg is output directory
      searchArgs.add(output);

      // Now run Cat
      LOG.info("Sending args to Cat: {}", searchArgs);
      retval = ToolRunner.run(conf, new Cat(),
          searchArgs.toArray(new String[0]));
    } finally {
      fm.releaseReadLocks(paths);
    }

    return retval;
  }

  public static void main(String[] args) throws Exception {
    // Let ToolRunner handle generic command-line options
    int res = ToolRunner.run(new Configuration(), new CatByTime(), args);
    System.exit(res);
  }
}
