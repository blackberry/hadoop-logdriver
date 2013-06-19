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
import java.util.Calendar;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.Tuple;

import com.rim.logdriver.boom.ReBoomWriter;

public class PigBoomHourlyRecordWriter extends
    RecordWriter<Tuple, NullWritable> {

  private int index = 0;

  private Path path;
  private FSDataOutputStream out;
  private ReBoomWriter writer;

  private long lastHour = 0l;
  private PigBoomHourlyOutputFormat outputFormat;
  private TaskAttemptContext context;

  public PigBoomHourlyRecordWriter(PigBoomHourlyOutputFormat outputFormat,
      TaskAttemptContext context) throws IOException {
    this.outputFormat = outputFormat;
    this.context = context;
  }

  @Override
  public void write(Tuple key, NullWritable value) throws IOException,
      InterruptedException {
    Long timestamp = (Long) key.get(0);
    getWriter(timestamp).writeLine(timestamp, key.get(1).toString(),
        (Integer) key.get(2), (Long) key.get(3), (Long) key.get(4));
  }

  private ReBoomWriter getWriter(Long timestamp) throws IOException {
    Long hour = timestamp / (60 * 60 * 1000);

    if (!hour.equals(lastHour)) {
      if (writer != null) {
        writer.close();
      }

      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(timestamp);
      String hr = String.format(".%05d.%04d-%02d-%02d-%02d.bm", index,
          cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
          cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY));

      path = outputFormat.getDefaultWorkFile(context, hr);
      out = path.getFileSystem(context.getConfiguration()).create(path);
      writer = new ReBoomWriter(out);

      index++;
    }

    lastHour = hour;

    return writer;
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    if (writer != null) {
      writer.close();
    }
  }

}
