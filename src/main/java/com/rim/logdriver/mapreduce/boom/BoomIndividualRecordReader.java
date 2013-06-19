package com.rim.logdriver.mapreduce.boom;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.Schemas;
import com.rim.logdriver.boom.LogLineData;

public class BoomIndividualRecordReader extends RecordReader<LogLineData, Text> {
  private static final Logger LOG = LoggerFactory
      .getLogger(BoomIndividualRecordReader.class);

  private FileSplit split;

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

  public BoomIndividualRecordReader(FileSplit split, TaskAttemptContext context) {
    this.split = split;
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
    // This should probably not happen, but if it does, then don't divide by
    // zero.
    if (split.getLength() == 0) {
      return 0;
    }

    return (pos - split.getStart()) / split.getLength();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;

    LOG.info("Initializing {}:{}+{}", new Object[] { fileSplit.getPath(),
        fileSplit.getStart(), fileSplit.getLength() });

    // Check for zero length files
    if (fileSplit.getPath().getFileSystem(context.getConfiguration())
        .getFileStatus(fileSplit.getPath()).getLen() == 0) {
      reader = null;
      return;
    }

    GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(
        Schemas.getSchema("logBlock"));
    reader = new DataFileReader<Record>(new FsInput(fileSplit.getPath(),
        context.getConfiguration()), datumReader);
    datumReader.setExpected(Schemas.getSchema("logBlock"));
    datumReader.setSchema(reader.getSchema());

    long size = fileSplit.getLength();
    start = fileSplit.getStart();
    end = start + size;

    reader.sync(start);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean nextKeyValue() throws IOException {
    while (lines.size() == 0) {
      // If we're out of lines in the current record, then get the next record -
      // unless we're out of records or past the end of where we should be.
      while (reader == null || reader.hasNext() == false
          || reader.pastSync(end)) {
        return false;
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
