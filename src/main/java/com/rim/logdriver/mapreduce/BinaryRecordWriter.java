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
 * Record writer that writes bytes literally to a file.
 * <p>
 * Setting <code>output.file.extension</code> in the configuration will cause
 * a specific extension to be used.  The default is no extension.
 */
package com.rim.logdriver.mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryRecordWriter extends
    RecordWriter<BytesWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory
      .getLogger(BinaryRecordWriter.class);

  private FSDataOutputStream out;

  /**
   * Create a writer for the given BinaryOutputFormat and TaskAttemptContext.
   * 
   * @param outputFormat
   * @param context
   */
  public BinaryRecordWriter(BinaryOutputFormat outputFormat,
      TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    String extension = conf.get("output.file.extension", "");

    try {
      Path outputPath = outputFormat.getDefaultWorkFile(context, extension);
      FileSystem fs = FileSystem.get(conf);
      LOG.info("Creating output path: {}", outputPath);
      out = fs.create(outputPath, true);
    } catch (IOException e) {
      LOG.error("Error creating output file.", e);
    }
  }

  @Override
  public synchronized void write(BytesWritable k, NullWritable v)
      throws IOException {
    if (k == null || k.getLength() == 0) {
      return;
    }

    byte[] data = Arrays.copyOfRange(k.getBytes(), 0, k.getLength());
    out.write(data);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    out.close();
  }

}
