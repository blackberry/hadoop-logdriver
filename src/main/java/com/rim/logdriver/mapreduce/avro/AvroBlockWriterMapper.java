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
package com.rim.logdriver.mapreduce.avro;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.avro.AvroFileHeader;

public class AvroBlockWriterMapper extends
    Mapper<AvroFileHeader, BytesWritable, BytesWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvroBlockWriterMapper.class);

  private AvroFileHeader header = new AvroFileHeader();

  /**
   * Writes out the blocks into new avro files.
   * 
   * @param key
   *          The header of the Avro file that the current block is from.
   * @param value
   *          The raw bytes of an Avro file data block, excluding the trailing
   *          sync marker.
   * @param context
   *          The mapper Context.
   * @throws IOException
   *           If this instance has received AvroFileHeader instances with
   *           different schemas or codecs, or other IO Error.
   * @throws InterruptedException
   */
  @Override
  protected void map(AvroFileHeader key, BytesWritable value, Context context)
      throws IOException, InterruptedException {
    byte[] valueBytes = null;

    if (header.getSyncMarker() == null) {
      LOG.info("Writing new header for new file: {}", key.toString());
      header.set(key);
      context.write(new BytesWritable(header.toBytes()), null);
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
    context.write(new BytesWritable(valueBytes), null);
    context.write(new BytesWritable(header.getSyncMarker()), null);

    context.getCounter("Avro Block", "Blocks processed").increment(1);
    context.getCounter("Avro Block", "Bytes processed").increment(
        value.getLength() + 16);
  }

}
