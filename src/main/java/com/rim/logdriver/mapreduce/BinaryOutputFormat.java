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
package com.rim.logdriver.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BinaryOutputFormat extends
    FileOutputFormat<BytesWritable, NullWritable> {

  /**
   * Gets a {@link BinaryRecordWriter} for a given context.
   * 
   * @param context
   *          The TaskAttemptContext.
   * @return The new BinaryRecordWriter.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordWriter<BytesWritable, NullWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new BinaryRecordWriter(this, context);
  }

}
