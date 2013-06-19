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
 * Reads in whole blocks of Avro files without decompressing or deserializing
 *  them.
 * 
 * There are two uses for this.
 * 
 * <h3>Merging Multiple Avro Files</h3>
 * To merge multiple Avro files that have the same schema and codec, read in
 * multiple files with the AvroBlockInputFormat, map them with
 * AvroBlockWriterMapper (to remove all but one header, and clean up the sync
 * markers), and write the result out with BinaryOutputFormat.
 * 
 * <h3>Low Level Optimizations</h3>
 * Maybe you only want to process certain blocks, and maybe you can tell
 * which blocks those are without deserializing the data.  In that case, you
 * can filter the raw blocks and only process certain ones.  Since
 * deserialization can be a relatively expensive operation, this might be 
 * useful in certain situations.
 * 
 */
package com.rim.logdriver.mapred.avro;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.avro.AvroFileHeader;

public class AvroBlockInputFormat extends
    CombineFileInputFormat<AvroFileHeader, BytesWritable> {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvroBlockInputFormat.class);
  private static final long MAX_SPLIT_LOCATIONS = 100000;

  @Override
  public RecordReader<AvroFileHeader, BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {

    // Ensure we have sensible defaults for how we build blocks.
    if (job.get("mapreduce.job.max.split.locations") == null) {
      job.setLong("mapreduce.job.max.split.locations", MAX_SPLIT_LOCATIONS);
    }
    if (job.get("mapred.max.split.size") == null) {
      // Try to set the split size to the default block size. In case of
      // failure, we'll use this 128MB default.
      long blockSize = 128 * 1024 * 1024; // 128MB
      try {
        blockSize = FileSystem.get(job).getDefaultBlockSize();
      } catch (IOException e) {
        LOG.error("Error getting filesystem to get get default block size (this does not bode well).");
      }
      job.setLong("mapred.max.split.size", blockSize);
    }

    return new AvroBlockRecordReader(split, job);
  }

}
