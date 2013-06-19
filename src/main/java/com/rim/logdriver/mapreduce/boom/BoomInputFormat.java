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

package com.rim.logdriver.mapreduce.boom;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.boom.LogLineData;

public class BoomInputFormat extends CombineFileInputFormat<LogLineData, Text> {
  private static final Logger LOG = LoggerFactory
      .getLogger(BoomInputFormat.class);

  private static final long MAX_SPLIT_LOCATIONS = 100000;

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  public List<InputSplit> getSplits(JobContext context) throws IOException {
    Configuration conf = context.getConfiguration();
    // Ensure we have sensible defaults for how we build blocks.
    if (conf.get("mapreduce.job.max.split.locations") == null) {
      conf.setLong("mapreduce.job.max.split.locations", MAX_SPLIT_LOCATIONS);
    }
    if (conf.get("mapred.max.split.size") == null) {
      // Try to set the split size to the default block size. In case of
      // failure, we'll use this 128MB default.
      long blockSize = 128 * 1024 * 1024; // 128MB
      try {
        blockSize = FileSystem.get(conf).getDefaultBlockSize();
      } catch (IOException e) {
        LOG.error("Error getting filesystem to get get default block size (this does not bode well).");
      }
      conf.setLong("mapred.max.split.size", blockSize);
    }
    for (String key : new String[] { "mapreduce.job.max.split.locations",
        "mapred.max.split.size" }) {
      LOG.info("{} = {}", key, context.getConfiguration().get(key));
    }

    return super.getSplits(context);
  }

  @Override
  public RecordReader<LogLineData, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {

    return new BoomRecordReader((CombineFileSplit) split, context);
  }

}
