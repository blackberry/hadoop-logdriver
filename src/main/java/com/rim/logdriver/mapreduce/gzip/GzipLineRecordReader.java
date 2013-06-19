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

package com.rim.logdriver.mapreduce.gzip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GzipLineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory
      .getLogger(GzipLineRecordReader.class);

  private BufferedReader in = null;

  private long pos = 0;

  private LongWritable key;
  private Text value;

  private float progress = 0.0f;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    key = new LongWritable();
    value = new Text();

    Path path = ((FileSplit) split).getPath();

    LOG.info("Starting to process file {}", path);
    in = new BufferedReader(new InputStreamReader(new GZIPInputStream(
        FileSystem.get(context.getConfiguration()).open(path))));
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    String line = null;
    if ((line = in.readLine()) == null) {
      progress = 1.0f;
      return false;
    }

    LOG.trace("Next line is {}", line);
    key.set(++pos);
    value.set(line);
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    return progress;
  }
}
