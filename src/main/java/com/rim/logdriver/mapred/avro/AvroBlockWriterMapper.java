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
 * Takes the output of the the AvroBlockInputFormat, and rewrites it into a
 * single file.  This is primarily used to merge multiple Avro files into one
 * larger Avro file, since this will rewrite all the sync markers to be
 * consistent.
 * 
 * The output is just the raw bytes of the new Avro file, and so it primarily
 * used with the BinaryOuputFormat.
 */
package com.rim.logdriver.mapred.avro;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.avro.AvroFileHeader;

public class AvroBlockWriterMapper implements
    Mapper<AvroFileHeader, BytesWritable, BytesWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvroBlockWriterMapper.class);

  private AvroFileHeader header = new AvroFileHeader();

  @Override
  public void configure(JobConf job) {
  }

  @Override
  public void map(AvroFileHeader key, BytesWritable value,
      OutputCollector<BytesWritable, NullWritable> output, Reporter reporter)
      throws IOException {
    byte[] valueBytes = null;

    if (header.getSyncMarker() == null) {
      LOG.info("Writing new header for new file: {}", key.toString());
      header.set(key);
      output.collect(new BytesWritable(header.toBytes()), null);
    } else {
      AvroFileHeader newHeader = key;
      if (!header.getSchema().equals(newHeader.getSchema())) {
        throw new IOException("Schemas in files do not match.");
      }
      if (!header.getCodec().equals(newHeader.getCodec())) {
        throw new IOException("Codecs in files do not match.");
      }
    }

    if (value.getLength() == 0) {
      return;
    }

    valueBytes = Arrays.copyOfRange(value.getBytes(), 0, value.getLength());
    output.collect(new BytesWritable(valueBytes), null);
    output.collect(new BytesWritable(header.getSyncMarker()), null);

    reporter.incrCounter("Avro Block", "Blocks processed", 1);
    reporter.incrCounter("Avro Block", "Bytes processed",
        value.getLength() + 16);
  }

  @Override
  public void close() throws IOException {
  }
}
