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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.Schemas;
import com.rim.logdriver.boom.LogLineData;

public class BoomRecordReader extends RecordReader<LogLineData, Text> {
  private static final Logger LOG = LoggerFactory
      .getLogger(BoomRecordReader.class);

  private CombineFileSplit split;
  private TaskAttemptContext context;

  private int currentFile = -1;

  private long start = 0;
  private long end = 0;
  private long pos = 0;

  private DataFileReader<Record> reader = null;

  private LogLineData lld = null;
  private long second = 0;
  private long lineNumber = 0;
  private Deque<Record> lines = new ArrayDeque<Record>();

  private LogLineData key = new LogLineData();
  private Text value = new Text();

  public BoomRecordReader(CombineFileSplit split, TaskAttemptContext context) {
    this.split = split;
    this.context = context;
  }

  private void initCurrentFile() throws IOException {
    if (reader != null) {
      reader.close();
    }

    LOG.info("Initializing {}:{}+{}",
        new Object[] { split.getPath(currentFile),
            split.getOffset(currentFile), split.getLength(currentFile) });

    // Check for zero length files
    if (split.getPath(currentFile).getFileSystem(context.getConfiguration())
        .getFileStatus(split.getPath(currentFile)).getLen() == 0) {
      reader = null;
      return;
    }

    GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(
        Schemas.getSchema("logBlock"));
    reader = new DataFileReader<Record>(new FsInput(split.getPath(currentFile),
        context.getConfiguration()), datumReader);
    datumReader.setExpected(Schemas.getSchema("logBlock"));
    datumReader.setSchema(reader.getSchema());

    long size = split.getLength(currentFile);
    start = split.getOffset(currentFile);
    end = start + size;

    reader.sync(start);
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public LogLineData getCurrentKey() {
    return key;
  }

  @Override
  public Text getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    // If we're done, report 1.
    if (currentFile >= split.getNumPaths()) {
      return 1.0f;
    }

    // These should probably not happen, but if they do, then don't divide by
    // zero.
    if (split.getLength(currentFile) == 0 || split.getNumPaths() == 0) {
      return 0;
    }

    // currentFile = how many we're complete, pos/length = fraction of the next
    // one that we're done. Divide by total number to get a decent estimate.
    return (currentFile + 1.0f * (pos - split.getOffset(currentFile))
        / split.getLength(currentFile))
        / split.getNumPaths();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean nextKeyValue() throws IOException {
    while (lines.size() == 0) {
      // If we're out of lines in the current record, then get the next record -
      // unless we're out of records or past the end of where we should be.
      while (reader == null || reader.hasNext() == false
          || reader.pastSync(end)) {
        // If we have another file, then init that and keep going, otherwise,
        // just return false.
        currentFile++;
        if (currentFile >= split.getNumPaths()) {
          return false;
        }
        initCurrentFile();
      }

      Record record = reader.next();
      lld = new LogLineData();
      lld.setBlockNumber((Long) record.get("blockNumber"));
      lld.setCreateTime((Long) record.get("createTime"));
      second = (Long) record.get("second");
      lineNumber = 0;

      lines.addAll((List<Record>) record.get("logLines"));
    }

    Record line = lines.pollFirst();
    long ms = (Long) line.get("ms");
    String message = line.get("message").toString();
    int eventId = (Integer) line.get("eventId");

    ++lineNumber;

    key.set(lld);
    key.setLineNumber(lineNumber);
    key.setTimestamp(second * 1000 + ms);
    key.setEventId(eventId);

    value.set(message);

    pos = reader.tell();

    return true;
  }

}
