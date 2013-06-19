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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

import com.rim.logdriver.boom.ReBoomWriter;

public class PigReBoomRecordWriter extends RecordWriter<Tuple, NullWritable> {

  private Path path;
  private FSDataOutputStream out;
  private ReBoomWriter writer;

  public PigReBoomRecordWriter(PigReBoomOutputFormat boomOutputFormat,
      TaskAttemptContext context) throws IOException {
    path = boomOutputFormat.getDefaultWorkFile(context, ".bm");
    out = path.getFileSystem(context.getConfiguration()).create(path);
    writer = new ReBoomWriter(out);
  }

  @Override
  public void write(Tuple key, NullWritable value) throws IOException,
      InterruptedException {
    writer.writeLine((Long) key.get(0), key.get(1).toString(),
        (Integer) key.get(2), (Long) key.get(3), (Long) key.get(4));
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    writer.close();
  }

}
