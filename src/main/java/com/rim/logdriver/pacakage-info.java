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
 * The libraries required to use the Log Driver system for
 * storing, retrieving and processing logs in Hadoop.
 * <blockquote>
 * For he goes birling down a-down the white water
 * That's where the log driver learns to step lightly
 * It's birling down, a-down white water
 * A log driver's waltz pleases girls completely.
 * </blockquote>
 * <div align="right"><small>&mdash; Wade Hemsworth, The Log Driver's Waltz</small></div>
 * 
 * <h2>Reading Boom Files</h2>
 * This library provides the ability to read in Boom files as the input to a
 * MapReduce job.
 * <p>
 * There are 3 steps in reading data: find the files you need, acquire a read
 * lock on those files, and use those files as input paths to the
 * BoomInputFormat.
 * <p>
 * The {@link com.rim.logdriver.fs.FileManager} class contains methods for
 * finding files in the Log Driver directory structure, and locking those
 * files.
 * <p>
 * The {@link com.rim.logdriver.boom.BoomInputFormat} will read those
 * files in for you.
 * <p>
 * Example:
 * <pre>
 *   String fileRegex = "myfileprefix-.*";
 *   PathFilter myPathFilter = new RegexFilenameFilter(fileRegex);
 *   long start = System.currentTimeMillis() - 60*60000; // 60 minutes ago
 *   long end = System.currentTimeMillis();
 *   List&lt;LockablePath&gt; paths = fileManager.getPaths(myPathFilter, start, end);
 *   fileManager.getReadLocks(paths);
 *   try {
 *     JobConf jobConf = new JobConf(conf, MyClass.class);
 *     ...
 *     jobConf.setInputFormat(BoomInputFormat.class);
 *     for (LockablePath path : paths) {
 *       BoomInputFormat.addInputPath(jobConf, path);
 *     }
 *     ...
 *   } finally {
 *     fileManager.releaseLocks(paths);
 *   }
 * </pre>
 * 
 * {@see com.rim.logdriver.boom.BoomInputFormat}
 * {@see com.rim.logdriver.boom.LogLineData}
 * {@see com.rim.logdriver.fs.FileManager}
 * {@see com.rim.logdriver.fs.LockablePath}
 */
package com.rim.logdriver;

