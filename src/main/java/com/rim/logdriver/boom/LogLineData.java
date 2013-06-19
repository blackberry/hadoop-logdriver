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

package com.rim.logdriver.boom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LogLineData implements WritableComparable<LogLineData> {
  private long timestamp = 0;
  private long createTime = 0;
  private long blockNumber = 0;
  private long lineNumber = 0;
  private int eventId = 0;

  public LogLineData() {
  }

  public LogLineData(long timestamp, long createTime, long blockNumber,
      long lineNumber, int eventId) {
    this.timestamp = timestamp;
    this.createTime = createTime;
    this.blockNumber = blockNumber;
    this.lineNumber = lineNumber;
    this.eventId = eventId;
  }

  public void set(LogLineData o) {
    timestamp = o.timestamp;
    createTime = o.createTime;
    blockNumber = o.blockNumber;
    lineNumber = o.lineNumber;
    eventId = o.eventId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(timestamp);
    out.writeLong(createTime);
    out.writeLong(blockNumber);
    out.writeLong(lineNumber);
    out.writeInt(eventId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    timestamp = in.readLong();
    createTime = in.readLong();
    blockNumber = in.readLong();
    lineNumber = in.readLong();
    eventId = in.readInt();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getBlockNumber() {
    return blockNumber;
  }

  public void setBlockNumber(long blockNumber) {
    this.blockNumber = blockNumber;
  }

  public long getLineNumber() {
    return lineNumber;
  }

  public void setLineNumber(long lineNumber) {
    this.lineNumber = lineNumber;
  }

  public int getEventId() {
    return eventId;
  }

  public void setEventId(int eventId) {
    this.eventId = eventId;
  }

  /**
   * A Comparator optimized for LogLineData.
   */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(LogLineData.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return ((LogLineData) a).compareTo((LogLineData) b);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // Since we packed the values in in the order we're interested in them, we
      // can just compare them in order
      long val = 0;
      for (int i = 0; i < 4; i++) {

        val = readLong(b1, s1 + 8 * i) - readLong(b2, s2 + 8 * i);
        if (val > 0) {
          return 1;
        } else if (val < 0) {
          return -1;
        }
      }

      val = readInt(b1, s1 + 8 * 4) - readInt(b2, s2 + 8 * 4);
      if (val > 0) {
        return 1;
      } else if (val < 0) {
        return -1;
      }

      return 0;
    }
  }

  static {
    WritableComparator.define(LogLineData.class, new Comparator());
  }

  @Override
  public int compareTo(LogLineData o) {
    return new CompareToBuilder().append(this.timestamp, o.timestamp)
        .append(this.createTime, o.createTime)
        .append(this.blockNumber, o.blockNumber)
        .append(this.lineNumber, o.lineNumber).append(this.eventId, o.eventId)
        .toComparison();
  }

  @Override
  public String toString() {
    return String
        .format(
            "[timestamp=%d, createTime=%d, blockNumber=%d, lineNumber=%d, eventId=%d]",
            timestamp, createTime, blockNumber, lineNumber, eventId);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (blockNumber ^ (blockNumber >>> 32));
    result = prime * result + (int) (createTime ^ (createTime >>> 32));
    result = prime * result + eventId;
    result = prime * result + (int) (lineNumber ^ (lineNumber >>> 32));
    result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    LogLineData other = (LogLineData) obj;
    if (blockNumber != other.blockNumber)
      return false;
    if (createTime != other.createTime)
      return false;
    if (eventId != other.eventId)
      return false;
    if (lineNumber != other.lineNumber)
      return false;
    if (timestamp != other.timestamp)
      return false;
    return true;
  }

}
