package com.rim.logdriver.mapreduce.boom;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.rim.logdriver.boom.LogLineData;

public class BoomIndividualInputFormat extends
    FileInputFormat<LogLineData, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  @Override
  public RecordReader<LogLineData, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new BoomIndividualRecordReader((FileSplit) split, context);
  }

}
