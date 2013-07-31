A Boom file is a place where we store logs.

The goals of Boom are:

* Be splittable by Hadoop, so that we can efficiently run MapReduce jobs against it.
* Be compressed to save storage.
* Be able to determine order of lines, even if they are processed out of order.

## File extention
The .bm file extension is used for Boom files.

## Using Boom in MapReduce
If you don't care about the details, and just want to read the files, use the BoomInputFormat.

BoomInputFormat takes files as usual, and passes <LogLineData, Text> to the mapper.

LogLineData is a class that wraps the timestamp, eventId and various metadata into an object.  The Text is the text of the log line.

If you need to get the timestamp and message for a log line, simply use

    long timestamp = key.getTimestamp();
    String message = value.toString();
    String originalLogLine = myDateFormat.format(timestamp) + " " + message;

## Boom File Format
A Boom file is a specific type of Avro [[Object Container File|http://avro.apache.org/docs/1.6.3/spec.html#Object+Container+Files]].  Familiarize yourself with those docs before you keep going.

Specifically, we always use a compression codec of 'deflate' and we always use the following Schema:

    {
      "type": "record",
      "name": "logBlock",
      "fields": [
        { "name": "second",      "type": "long" },
        { "name": "createTime",  "type": "long" },
        { "name": "blockNumber", "type": "long" },
        { "name": "logLines", "type": {
          "type": "array",
            "items": {
              "type": "record",
              "name": "messageWithMillis",
              "fields": [ 
                { "name": "ms",      "type": "long" },
                { "name": "eventId", "type": "int", "default": 0 },
                { "name": "message", "type": "string" }
              ]
            }
        }}
      ]
    }

### Basic Structure
The file contains any number of "logBlock" records.  Each logBlock contains data for multiple log lines, but all of the log lines in the record are timestamped in the same second.  Log lines in the same logBlock can have difference millisecond timestamps.

### Fields in logBlock
* second : the number of seconds since Jan 1, 1970 UTC.  All log lines in this record are timestamped with a time that occurs within this second.
* createTime : the time (in milliseconds) that this logBlock was created.  This is used for sorting logBlocks.
* blockNumber : a number indicating the sequence in which the logBlocks were written by whatever wrote the file.  This is used for sorting logBlocks.
* logLines : an array of "messageWithMillis" records, one per log line.

### Fields in messageWithMillis
* ms : the milliseconds part of the timestamp for this log line.  To get the complete timestamp, use second * 1000 + ms.
* eventId : an event identifier, reserved for future use.  Use 0 for raw log lines.
* message : the contents of the log line, excluding the timestamp and one space after the timestamp.

## Boom suggested defaults
Although no limitations should be assumed on the file beyond what has already been stated, these are sensible defaults that should be followed.

* The logLines field should contain no more that 1000 messageWithMillis entries.  If there are more than 1000 log lines within a second, then use multiple logBlock's with the same second value.
* The Avro Object Container File defines a "sync interval".  A good value for this seems to be 2MB (2147483648).
* While we are required to use the deflate codec, the compression level is configurable.  If you don't have a specific need, then level 6 is a good default.

## Sorting log lines
If the order of log lines is important, then the fields can be sorted by comparing fields in this order

* timestamp : first timestamp is first (after adding seconds and milliseconds)
* createTime : logBlocks that were written first go first.
* blockNumber : If two logBlocks were written in the same millisecond, then use them in the order they were written.
* index within logLines : If the log lines are the same timestamp, written in the same block, then the order is determined by where they are within the logLines array.

This is the default sorting for LogLineData objects.

