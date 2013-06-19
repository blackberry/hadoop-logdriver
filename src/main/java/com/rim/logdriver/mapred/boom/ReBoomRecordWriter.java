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

package com.rim.logdriver.mapred.boom;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import com.rim.logdriver.boom.LogLineData;
import com.rim.logdriver.boom.ReBoomWriter;
import com.rim.logdriver.mapred.BinaryOutputFormat;

public class ReBoomRecordWriter implements RecordWriter<LogLineData, Text> {

  private ReBoomWriter writer;

  public ReBoomRecordWriter(ReBoomOutputFormat reBoomOutputFormat, JobConf job)
      throws IOException {
    String taskid = job.get("mapred.task.id");
    Path path = BinaryOutputFormat.getTaskOutputPath(job, taskid + ".bm");
    FSDataOutputStream out = path.getFileSystem(job).create(path);
    writer = new ReBoomWriter(out);
  }

  @Override
  public void write(LogLineData key, Text value) throws IOException {
    writer.writeLine(key.getTimestamp(), value.toString(), key.getEventId(),
        key.getCreateTime(), key.getBlockNumber());
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    writer.close();
  }

}
