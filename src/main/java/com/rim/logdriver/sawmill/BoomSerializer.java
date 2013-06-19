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

package com.rim.logdriver.sawmill;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.boom.schema.LogBlock;
import com.rim.boom.schema.MessageWithMillis;

public class BoomSerializer {
  private static final Logger LOG = LoggerFactory
      .getLogger(BoomSerializer.class);

  private OutputStream out;
  private DataFileWriter<LogBlock> writer = null;
  private LogBlock logBlock = null;
  private long blockNumber = 0;

  private Schema logBlockSchema = null;

  private int deflateLevel;
  private int syncInterval;

  private long linesWritten = 0;
  private long boomBlocksWritten = 0;

  public BoomSerializer(OutputStream out, Properties conf) {
    this.out = out;

    logBlockSchema = LogBlock.SCHEMA$;

    deflateLevel = Configs.boomDeflateLevel.getInteger(conf);
    syncInterval = Configs.boomSyncInterval.getInteger(conf);
  }

  public void afterCreate() throws IOException {
    SpecificDatumWriter<LogBlock> datumWriter = new SpecificDatumWriter<LogBlock>();
    writer = new DataFileWriter<LogBlock>(datumWriter);
    writer.setCodec(CodecFactory.deflateCodec(deflateLevel));
    writer.setSyncInterval(syncInterval);

    writer.create(logBlockSchema, out);
  }

  public void write() throws IOException {
    if (logBlock != null) {
      LOG.debug("Writing block: second={} numLines={}", logBlock.getSecond(),
          logBlock.getLogLines().size());
      linesWritten += logBlock.getLogLines().size();
      ++boomBlocksWritten;

      writer.append(logBlock);

      logBlock = null;
    }
  }

  public void flush() throws IOException {
    write();
    writer.flush();
  }

  public void write(long timestamp, String message) throws IOException {

    // Split the time into seconds and milliseconds
    long ms = timestamp % 1000l;
    long second = timestamp / 1000l;

    // If we've changed seconds, then flush the current log block.
    if (logBlock != null && second != logBlock.getSecond()) {
      LOG.debug("Flushing due to new second: old:{}, new:{}",
          logBlock.getSecond(), second);
      write();
    }

    // If this is new, or we just flushed, then we'll need to build a new log
    // block.
    if (logBlock == null) {
      logBlock = new LogBlock();
      logBlock.setSecond(second);
      logBlock.setCreateTime(System.currentTimeMillis());
      logBlock.setBlockNumber(++blockNumber);
      logBlock.setLogLines(new ArrayList<MessageWithMillis>(1000));
    }

    // Now we can append a logline to the logblock
    MessageWithMillis record = new MessageWithMillis();
    record.setMs(ms);
    record.setEventId(0);
    record.setMessage(message);
    logBlock.getLogLines().add(record);
  }

  public long getLinesWritten() {
    return linesWritten;
  }

  public void setLinesWritten(long linesWritten) {
    this.linesWritten = linesWritten;
  }

  public long getBoomBlocksWritten() {
    return boomBlocksWritten;
  }

  public void setBoomBlocksWritten(long boomBlocksWritten) {
    this.boomBlocksWritten = boomBlocksWritten;
  }

}
