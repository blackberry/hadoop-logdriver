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
 * Writes timestamp, message pairs to a Boom file on the given output stream.
 */
package com.rim.logdriver.boom;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.Schemas;

public class BoomWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BoomWriter.class);

  private int maxLinesPerRecord = 1000;
  private int deflateLevel = 6;
  private int avroBlockSize = 2097152;

  private Schema logBlockSchema = null;
  private Schema messageWithMillisArraySchema = null;
  private Schema messageWithMillisSchema = null;

  private DataFileWriter<GenericRecord> writer = null;
  private OutputStream out = null;
  private GenericRecord logBlock = null;

  private long blockNumber = 0;
  private long linesInBlock = 0;

  /**
   * Create a writer that uses the given OutputStream
   * 
   * @param out
   *          The OutputStream to write the file to.
   * @throws IOException
   */
  public BoomWriter(OutputStream out) throws IOException {
    this.out = out;

    logBlockSchema = Schemas.getSchema("logBlock");
    messageWithMillisArraySchema = logBlockSchema.getField("logLines").schema();
    messageWithMillisSchema = messageWithMillisArraySchema.getElementType();

    writer = newWriter();
  }

  public void writeLine(long timestamp, String message) throws IOException {
    long ms = timestamp % 1000l;
    long second = timestamp / 1000l;
    if (logBlock != null && second != (Long) logBlock.get("second")) {
      LOG.debug("Flushing due to new second: old:{}, new:{}",
          (Long) logBlock.get("second"), second);
      writeBlock();
    }

    if (logBlock == null) {
      logBlock = new GenericData.Record(logBlockSchema);
      logBlock.put("second", second);
      logBlock.put("createTime", System.currentTimeMillis());
      logBlock.put("blockNumber", ++blockNumber);
      logBlock.put("logLines", new GenericData.Array<Record>(
          getMaxLinesPerRecord() / 8, messageWithMillisArraySchema));
    }

    @SuppressWarnings("unchecked")
    GenericArray<Record> logLines = (GenericArray<Record>) logBlock
        .get("logLines");
    Record record = new GenericData.Record(messageWithMillisSchema);
    record.put("ms", ms);
    record.put("eventId", 0);
    record.put("message", message);
    logLines.add(record);

    linesInBlock++;

    if (linesInBlock >= getMaxLinesPerRecord()) {
      LOG.debug("Hit max lines, flushing record.");
      writeBlock();
    }

  }

  private DataFileWriter<GenericRecord> newWriter() throws IOException {
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
        logBlockSchema);
    writer = new DataFileWriter<GenericRecord>(datumWriter);
    writer.setCodec(CodecFactory.deflateCodec(getDeflateLevel()));
    writer.setSyncInterval(getAvroBlockSize());

    writer.create(logBlockSchema, out);
    return writer;
  }

  @SuppressWarnings("unchecked")
  private void writeBlock() {
    if (logBlock != null) {
      LOG.debug("Writing block: {} {}", logBlock.get("second"),
          ((GenericArray<String>) logBlock.get("logLines")).size());

      try {
        writer.append(logBlock);
      } catch (IOException e) {
        LOG.error("Error writing out record.  Data lost.", e);
      }

      logBlock = null;
      linesInBlock = 0;
    }
  }

  public void close() throws IOException {
    LOG.debug("Closing BoomWriter");
    writeBlock();
    if (writer != null) {
      writer.close();
    }
  }

  public int getMaxLinesPerRecord() {
    return maxLinesPerRecord;
  }

  public void setMaxLinesPerRecord(int maxLinesPerRecord) {
    this.maxLinesPerRecord = maxLinesPerRecord;
  }

  public int getDeflateLevel() {
    return deflateLevel;
  }

  public void setDeflateLevel(int deflateLevel) {
    this.deflateLevel = deflateLevel;
  }

  public int getAvroBlockSize() {
    return avroBlockSize;
  }

  public void setAvroBlockSize(int avroBlockSize) {
    this.avroBlockSize = avroBlockSize;
  }
}
