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
 * Output format that writes raw bytes to to a file.
 */
package com.rim.logdriver.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class BinaryOutputFormat extends
    FileOutputFormat<BytesWritable, NullWritable> {

  @Override
  public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {
    return new BinaryRecordWriter(job);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws IOException {
  }

}
