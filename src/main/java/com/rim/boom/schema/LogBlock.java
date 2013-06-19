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

package com.rim.boom.schema;

@SuppressWarnings("all")
public class LogBlock extends org.apache.avro.specific.SpecificRecordBase
    implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
      .parse("{\"type\":\"record\",\"name\":\"logBlock\",\"fields\":[{\"name\":\"second\",\"type\":\"long\"},{\"name\":\"createTime\",\"type\":\"long\"},{\"name\":\"blockNumber\",\"type\":\"long\"},{\"name\":\"logLines\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"messageWithMillis\",\"fields\":[{\"name\":\"ms\",\"type\":\"long\"},{\"name\":\"eventId\",\"type\":\"int\",\"default\":0},{\"name\":\"message\",\"type\":\"string\"}]}}}]}");
  @Deprecated
  public long second;
  @Deprecated
  public long createTime;
  @Deprecated
  public long blockNumber;
  @Deprecated
  public java.util.List<MessageWithMillis> logLines;

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0:
      return second;
    case 1:
      return createTime;
    case 2:
      return blockNumber;
    case 3:
      return logLines;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader. Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0:
      second = (java.lang.Long) value$;
      break;
    case 1:
      createTime = (java.lang.Long) value$;
      break;
    case 2:
      blockNumber = (java.lang.Long) value$;
      break;
    case 3:
      logLines = (java.util.List<MessageWithMillis>) value$;
      break;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'second' field.
   */
  public java.lang.Long getSecond() {
    return second;
  }

  /**
   * Sets the value of the 'second' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setSecond(java.lang.Long value) {
    this.second = value;
  }

  /**
   * Gets the value of the 'createTime' field.
   */
  public java.lang.Long getCreateTime() {
    return createTime;
  }

  /**
   * Sets the value of the 'createTime' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setCreateTime(java.lang.Long value) {
    this.createTime = value;
  }

  /**
   * Gets the value of the 'blockNumber' field.
   */
  public java.lang.Long getBlockNumber() {
    return blockNumber;
  }

  /**
   * Sets the value of the 'blockNumber' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setBlockNumber(java.lang.Long value) {
    this.blockNumber = value;
  }

  /**
   * Gets the value of the 'logLines' field.
   */
  public java.util.List<MessageWithMillis> getLogLines() {
    return logLines;
  }

  /**
   * Sets the value of the 'logLines' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setLogLines(java.util.List<MessageWithMillis> value) {
    this.logLines = value;
  }

//  /** Creates a new logBlock RecordBuilder */
//  public static LogBlock.Builder newBuilder() {
//    return new LogBlock.Builder();
//  }
//
//  /** Creates a new logBlock RecordBuilder by copying an existing Builder */
//  public static LogBlock.Builder newBuilder(LogBlock.Builder other) {
//    return new LogBlock.Builder(other);
//  }
//
//  /**
//   * Creates a new logBlock RecordBuilder by copying an existing logBlock
//   * instance
//   */
//  public static LogBlock.Builder newBuilder(LogBlock other) {
//    return new LogBlock.Builder(other);
//  }
//
//  /**
//   * RecordBuilder for logBlock instances.
//   */
//  public static class Builder extends
//      org.apache.avro.specific.SpecificRecordBuilderBase<LogBlock> implements
//      org.apache.avro.data.RecordBuilder<LogBlock> {
//
//    private long second;
//    private long createTime;
//    private long blockNumber;
//    private java.util.List<MessageWithMillis> logLines;
//
//    /** Creates a new Builder */
//    private Builder() {
//      super(LogBlock.SCHEMA$);
//    }
//
//    /** Creates a Builder by copying an existing Builder */
//    private Builder(LogBlock.Builder other) {
//      super(other);
//    }
//
//    /** Creates a Builder by copying an existing logBlock instance */
//    private Builder(LogBlock other) {
//      super(LogBlock.SCHEMA$);
//      if (isValidValue(fields()[0], other.second)) {
//        this.second = (java.lang.Long) data().deepCopy(fields()[0].schema(),
//            other.second);
//        fieldSetFlags()[0] = true;
//      }
//      if (isValidValue(fields()[1], other.createTime)) {
//        this.createTime = (java.lang.Long) data().deepCopy(
//            fields()[1].schema(), other.createTime);
//        fieldSetFlags()[1] = true;
//      }
//      if (isValidValue(fields()[2], other.blockNumber)) {
//        this.blockNumber = (java.lang.Long) data().deepCopy(
//            fields()[2].schema(), other.blockNumber);
//        fieldSetFlags()[2] = true;
//      }
//      if (isValidValue(fields()[3], other.logLines)) {
//        this.logLines = (java.util.List<MessageWithMillis>) data().deepCopy(
//            fields()[3].schema(), other.logLines);
//        fieldSetFlags()[3] = true;
//      }
//    }
//
//    /** Gets the value of the 'second' field */
//    public java.lang.Long getSecond() {
//      return second;
//    }
//
//    /** Sets the value of the 'second' field */
//    public LogBlock.Builder setSecond(long value) {
//      validate(fields()[0], value);
//      this.second = value;
//      fieldSetFlags()[0] = true;
//      return this;
//    }
//
//    /** Checks whether the 'second' field has been set */
//    public boolean hasSecond() {
//      return fieldSetFlags()[0];
//    }
//
//    /** Clears the value of the 'second' field */
//    public LogBlock.Builder clearSecond() {
//      fieldSetFlags()[0] = false;
//      return this;
//    }
//
//    /** Gets the value of the 'createTime' field */
//    public java.lang.Long getCreateTime() {
//      return createTime;
//    }
//
//    /** Sets the value of the 'createTime' field */
//    public LogBlock.Builder setCreateTime(long value) {
//      validate(fields()[1], value);
//      this.createTime = value;
//      fieldSetFlags()[1] = true;
//      return this;
//    }
//
//    /** Checks whether the 'createTime' field has been set */
//    public boolean hasCreateTime() {
//      return fieldSetFlags()[1];
//    }
//
//    /** Clears the value of the 'createTime' field */
//    public LogBlock.Builder clearCreateTime() {
//      fieldSetFlags()[1] = false;
//      return this;
//    }
//
//    /** Gets the value of the 'blockNumber' field */
//    public java.lang.Long getBlockNumber() {
//      return blockNumber;
//    }
//
//    /** Sets the value of the 'blockNumber' field */
//    public LogBlock.Builder setBlockNumber(long value) {
//      validate(fields()[2], value);
//      this.blockNumber = value;
//      fieldSetFlags()[2] = true;
//      return this;
//    }
//
//    /** Checks whether the 'blockNumber' field has been set */
//    public boolean hasBlockNumber() {
//      return fieldSetFlags()[2];
//    }
//
//    /** Clears the value of the 'blockNumber' field */
//    public LogBlock.Builder clearBlockNumber() {
//      fieldSetFlags()[2] = false;
//      return this;
//    }
//
//    /** Gets the value of the 'logLines' field */
//    public java.util.List<MessageWithMillis> getLogLines() {
//      return logLines;
//    }
//
//    /** Sets the value of the 'logLines' field */
//    public LogBlock.Builder setLogLines(java.util.List<MessageWithMillis> value) {
//      validate(fields()[3], value);
//      this.logLines = value;
//      fieldSetFlags()[3] = true;
//      return this;
//    }
//
//    /** Checks whether the 'logLines' field has been set */
//    public boolean hasLogLines() {
//      return fieldSetFlags()[3];
//    }
//
//    /** Clears the value of the 'logLines' field */
//    public LogBlock.Builder clearLogLines() {
//      logLines = null;
//      fieldSetFlags()[3] = false;
//      return this;
//    }
//
//    @Override
//    public LogBlock build() {
//      try {
//        LogBlock record = new LogBlock();
//        record.second = fieldSetFlags()[0] ? this.second
//            : (java.lang.Long) defaultValue(fields()[0]);
//        record.createTime = fieldSetFlags()[1] ? this.createTime
//            : (java.lang.Long) defaultValue(fields()[1]);
//        record.blockNumber = fieldSetFlags()[2] ? this.blockNumber
//            : (java.lang.Long) defaultValue(fields()[2]);
//        record.logLines = fieldSetFlags()[3] ? this.logLines
//            : (java.util.List<MessageWithMillis>) defaultValue(fields()[3]);
//        return record;
//      } catch (Exception e) {
//        throw new org.apache.avro.AvroRuntimeException(e);
//      }
//    }
//  }
}
