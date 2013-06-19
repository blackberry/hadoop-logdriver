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
public class MessageWithMillis extends
    org.apache.avro.specific.SpecificRecordBase implements
    org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser()
      .parse("{\"type\":\"record\",\"name\":\"messageWithMillis\",\"fields\":[{\"name\":\"ms\",\"type\":\"long\"},{\"name\":\"eventId\",\"type\":\"int\",\"default\":0},{\"name\":\"message\",\"type\":\"string\"}]}");
  @Deprecated
  public long ms;
  @Deprecated
  public int eventId;
  @Deprecated
  public java.lang.CharSequence message;

  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0:
      return ms;
    case 1:
      return eventId;
    case 2:
      return message;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader. Applications should not call.
  @SuppressWarnings(value = "unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0:
      ms = (java.lang.Long) value$;
      break;
    case 1:
      eventId = (java.lang.Integer) value$;
      break;
    case 2:
      message = (java.lang.CharSequence) value$;
      break;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'ms' field.
   */
  public java.lang.Long getMs() {
    return ms;
  }

  /**
   * Sets the value of the 'ms' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setMs(java.lang.Long value) {
    this.ms = value;
  }

  /**
   * Gets the value of the 'eventId' field.
   */
  public java.lang.Integer getEventId() {
    return eventId;
  }

  /**
   * Sets the value of the 'eventId' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setEventId(java.lang.Integer value) {
    this.eventId = value;
  }

  /**
   * Gets the value of the 'message' field.
   */
  public java.lang.CharSequence getMessage() {
    return message;
  }

  /**
   * Sets the value of the 'message' field.
   * 
   * @param value
   *          the value to set.
   */
  public void setMessage(java.lang.CharSequence value) {
    this.message = value;
  }

//  /** Creates a new messageWithMillis RecordBuilder */
//  public static MessageWithMillis.Builder newBuilder() {
//    return new MessageWithMillis.Builder();
//  }
//
//  /**
//   * Creates a new messageWithMillis RecordBuilder by copying an existing
//   * Builder
//   */
//  public static MessageWithMillis.Builder newBuilder(
//      MessageWithMillis.Builder other) {
//    return new MessageWithMillis.Builder(other);
//  }
//
//  /**
//   * Creates a new messageWithMillis RecordBuilder by copying an existing
//   * messageWithMillis instance
//   */
//  public static MessageWithMillis.Builder newBuilder(MessageWithMillis other) {
//    return new MessageWithMillis.Builder(other);
//  }

//  /**
//   * RecordBuilder for messageWithMillis instances.
//   */
//  public static class Builder extends
//      org.apache.avro.specific.SpecificRecordBuilderBase<MessageWithMillis>
//      implements org.apache.avro.data.RecordBuilder<MessageWithMillis> {
//
//    private long ms;
//    private int eventId;
//    private java.lang.CharSequence message;
//
//    /** Creates a new Builder */
//    private Builder() {
//      super(MessageWithMillis.SCHEMA$);
//    }
//
//    /** Creates a Builder by copying an existing Builder */
//    private Builder(MessageWithMillis.Builder other) {
//      super(other);
//    }
//
//    /** Creates a Builder by copying an existing messageWithMillis instance */
//    private Builder(MessageWithMillis other) {
//      super(MessageWithMillis.SCHEMA$);
//      if (isValidValue(fields()[0], other.ms)) {
//        this.ms = (java.lang.Long) data().deepCopy(fields()[0].schema(),
//            other.ms);
//        fieldSetFlags()[0] = true;
//      }
//      if (isValidValue(fields()[1], other.eventId)) {
//        this.eventId = (java.lang.Integer) data().deepCopy(
//            fields()[1].schema(), other.eventId);
//        fieldSetFlags()[1] = true;
//      }
//      if (isValidValue(fields()[2], other.message)) {
//        this.message = (java.lang.CharSequence) data().deepCopy(
//            fields()[2].schema(), other.message);
//        fieldSetFlags()[2] = true;
//      }
//    }
//
//    /** Gets the value of the 'ms' field */
//    public java.lang.Long getMs() {
//      return ms;
//    }
//
//    /** Sets the value of the 'ms' field */
//    public MessageWithMillis.Builder setMs(long value) {
//      validate(fields()[0], value);
//      this.ms = value;
//      fieldSetFlags()[0] = true;
//      return this;
//    }
//
//    /** Checks whether the 'ms' field has been set */
//    public boolean hasMs() {
//      return fieldSetFlags()[0];
//    }
//
//    /** Clears the value of the 'ms' field */
//    public MessageWithMillis.Builder clearMs() {
//      fieldSetFlags()[0] = false;
//      return this;
//    }
//
//    /** Gets the value of the 'eventId' field */
//    public java.lang.Integer getEventId() {
//      return eventId;
//    }
//
//    /** Sets the value of the 'eventId' field */
//    public MessageWithMillis.Builder setEventId(int value) {
//      validate(fields()[1], value);
//      this.eventId = value;
//      fieldSetFlags()[1] = true;
//      return this;
//    }
//
//    /** Checks whether the 'eventId' field has been set */
//    public boolean hasEventId() {
//      return fieldSetFlags()[1];
//    }
//
//    /** Clears the value of the 'eventId' field */
//    public MessageWithMillis.Builder clearEventId() {
//      fieldSetFlags()[1] = false;
//      return this;
//    }
//
//    /** Gets the value of the 'message' field */
//    public java.lang.CharSequence getMessage() {
//      return message;
//    }
//
//    /** Sets the value of the 'message' field */
//    public MessageWithMillis.Builder setMessage(java.lang.CharSequence value) {
//      validate(fields()[2], value);
//      this.message = value;
//      fieldSetFlags()[2] = true;
//      return this;
//    }
//
//    /** Checks whether the 'message' field has been set */
//    public boolean hasMessage() {
//      return fieldSetFlags()[2];
//    }
//
//    /** Clears the value of the 'message' field */
//    public MessageWithMillis.Builder clearMessage() {
//      message = null;
//      fieldSetFlags()[2] = false;
//      return this;
//    }
//
//    @Override
//    public MessageWithMillis build() {
//      try {
//        MessageWithMillis record = new MessageWithMillis();
//        record.ms = fieldSetFlags()[0] ? this.ms
//            : (java.lang.Long) defaultValue(fields()[0]);
//        record.eventId = fieldSetFlags()[1] ? this.eventId
//            : (java.lang.Integer) defaultValue(fields()[1]);
//        record.message = fieldSetFlags()[2] ? this.message
//            : (java.lang.CharSequence) defaultValue(fields()[2]);
//        return record;
//      } catch (Exception e) {
//        throw new org.apache.avro.AvroRuntimeException(e);
//      }
//    }
//  }
}
