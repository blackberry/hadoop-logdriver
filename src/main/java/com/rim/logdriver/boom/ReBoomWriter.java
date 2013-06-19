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
 * Given all the fields in each boom record, records them as given.  
 * Used when old Boom files are disassembled and reassembled for some reason
 * (such as to remove or alter some log lines).
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

public class ReBoomWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ReBoomWriter.class);

  private int maxLinesPerRecord = 1000;
  private int deflateLevel = 6;
  private int avroBlockSize = 2097152;

  private Schema logBlockSchema = null;
  private Schema messageWithMillisArraySchema = null;
  private Schema messageWithMillisSchema = null;

  private DataFileWriter<GenericRecord> writer = null;
  private OutputStream out = null;
  private GenericRecord logBlock = null;

  /**
   * Create a writer that uses the given OutputStream
   * 
   * @param out
   *          The OutputStream to write the file to.
   * @throws IOException
   */
  public ReBoomWriter(OutputStream out) throws IOException {
    this.out = out;

    logBlockSchema = Schemas.getSchema("logBlock");
    messageWithMillisArraySchema = logBlockSchema.getField("logLines").schema();
    messageWithMillisSchema = messageWithMillisArraySchema.getElementType();

    writer = newWriter();
  }

  public void writeLine(long timestamp, String message, int eventId,
      long createTime, long blockNumber) throws IOException {
    long ms = timestamp % 1000l;
    long second = timestamp / 1000l;

    if (logBlock != null) {
      if (second != (Long) logBlock.get("second")) {
        LOG.debug("Flushing due to change in second. old:{} new:{}",
            logBlock.get("second"), second);
        writeBlock();
      } else if (createTime != (Long) logBlock.get("createTime")) {
        LOG.debug("Flushing due to change in createTime. old:{} new:{}",
            logBlock.get("createTime"), createTime);
        writeBlock();
      } else if (blockNumber != (Long) logBlock.get("blockNumber")) {
        LOG.debug("Flushing due to change in blockNumber. old:{} new:{}",
            logBlock.get("blockNumber"), blockNumber);
        writeBlock();
      }
    }

    if (logBlock == null) {
      logBlock = new GenericData.Record(logBlockSchema);
      logBlock.put("second", second);
      logBlock.put("createTime", createTime);
      logBlock.put("blockNumber", blockNumber);
      logBlock.put("logLines", new GenericData.Array<Record>(
          getMaxLinesPerRecord() / 8, messageWithMillisArraySchema));
    }

    @SuppressWarnings("unchecked")
    GenericArray<Record> logLines = (GenericArray<Record>) logBlock
        .get("logLines");
    Record record = new GenericData.Record(messageWithMillisSchema);
    record.put("ms", ms);
    record.put("eventId", eventId);
    record.put("message", message);
    logLines.add(record);
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
