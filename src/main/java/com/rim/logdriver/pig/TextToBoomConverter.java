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
import java.text.ParseException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.timestamp.Rfc3164TimestampParser;
import com.rim.logdriver.timestamp.Rfc5424TimestampParser;
import com.rim.logdriver.timestamp.TimestampParser;

public class TextToBoomConverter extends EvalFunc<Tuple> {
  private static final Logger LOG = LoggerFactory
      .getLogger(TextToBoomConverter.class);

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();
  private static final TimestampParser[] timestampParsers = new TimestampParser[] {
      new Rfc3164TimestampParser(), new Rfc5424TimestampParser() };

  private static final Integer eventId = 0;
  private long lineNumber = 0;

  private TimestampParser timestampParser = null;
  private Long createTime = null;
  private Long blockNumber = null;
  private Long blockLineNumber = null;
  private Long lastSecond = null;

  @Override
  public Tuple exec(Tuple input) throws IOException {

    String logLine = input.get(0).toString();
    try {
      lineNumber++;
      if (lineNumber == 1) {
        // New file! Reset everything!
        blockNumber = 0l;
        blockLineNumber = 0l;

        lastSecond = 0l;

        // Check which timestamp parser looks like it will work for this file
        for (TimestampParser parser : timestampParsers) {
          try {
            String[] split = parser.splitLine(logLine);
            if (split[0] == null || split[1] == null) {
              throw new ParseException("Error splitting line", 0);
            }

            Long timestamp = parser.parseTimestatmp(split[0]);
            createTime = timestamp;
          } catch (Throwable t) {
            LOG.info("No match for {}", parser.getClass().getName());
            continue;
          }

          timestampParser = parser;
          LOG.info("Using parser {}", parser.getClass().getName());
          break;
        }
      }

      // If we didn't get a parser, then blah! Die!
      if (timestampParser == null) {
        throw new IOException("No parser found.");
      }

      // Try to extract the timestamp and message
      String[] split = timestampParser.splitLine(logLine);
      Long timestamp;
      timestamp = timestampParser.parseTimestatmp(split[0]);

      String message = split[1];

      // Do we need a new block, or a new line in an existing block?
      Long second = timestamp / 1000;
      if (blockLineNumber > 1000 || !second.equals(lastSecond)) {
        blockNumber++;
        blockLineNumber = 0l;
      } else {
        blockLineNumber++;
      }

      lastSecond = second;

      Tuple tuple = tupleFactory.newTuple(6);
      tuple.set(0, timestamp);
      tuple.set(1, message);
      tuple.set(2, eventId);
      tuple.set(3, createTime);
      tuple.set(4, blockNumber);
      tuple.set(5, blockLineNumber);

      return tuple;
    } catch (IOException e) {
      LOG.error("Caught IO exception on line: {}", logLine, e);
      throw e;
    } catch (Exception e) {
      LOG.error("Caught parse exception on line: {}", logLine, e);
      throw new IOException(e);
    }
  }
}
