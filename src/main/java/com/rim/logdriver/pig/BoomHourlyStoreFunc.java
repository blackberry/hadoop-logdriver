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

package com.rim.logdriver.pig;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import com.rim.logdriver.mapreduce.boom.PigBoomHourlyOutputFormat;
import com.rim.logdriver.mapreduce.boom.PigBoomHourlyRecordWriter;

public class BoomHourlyStoreFunc extends StoreFunc {
  private PigBoomHourlyRecordWriter writer;

  @SuppressWarnings("rawtypes")
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new PigBoomHourlyOutputFormat();
  }

  @SuppressWarnings({ "rawtypes" })
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = (PigBoomHourlyRecordWriter) writer;
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {
    try {
      writer.write(tuple, NullWritable.get());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    PigBoomHourlyOutputFormat.setOutputPath(job, new Path(location));
  }

}
