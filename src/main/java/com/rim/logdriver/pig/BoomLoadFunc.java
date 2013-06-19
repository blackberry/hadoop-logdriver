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

package com.rim.logdriver.pig;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.rim.logdriver.boom.LogLineData;
import com.rim.logdriver.mapreduce.boom.BoomIndividualInputFormat;
import com.rim.logdriver.mapreduce.boom.BoomIndividualRecordReader;

public class BoomLoadFunc extends LoadFunc {
  private TupleFactory tupleFactory = TupleFactory.getInstance();
  private BoomIndividualRecordReader rr = null;

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
    return new BoomIndividualInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    boolean moreData = rr.nextKeyValue();

    if (!moreData) {
      return null;
    }

    LogLineData lld = rr.getCurrentKey();
    String message = rr.getCurrentValue().toString();

    Tuple tuple = tupleFactory.newTuple(6);
    tuple.set(0, lld.getTimestamp());
    tuple.set(1, message);
    tuple.set(2, lld.getEventId());
    tuple.set(3, lld.getCreateTime());
    tuple.set(4, lld.getBlockNumber());
    tuple.set(5, lld.getLineNumber());

    return tuple;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepareToRead(RecordReader recordReader, PigSplit split)
      throws IOException {
    rr = (BoomIndividualRecordReader) recordReader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    BoomIndividualInputFormat.setInputPaths(job, location);
  }

}
