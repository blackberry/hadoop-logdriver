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

/**
 * Interface for classes which parse timestamps from syslog log lines.
 */
package com.rim.logdriver.timestamp;

import java.text.ParseException;

public interface TimestampParser {
  /**
   * Splits the line into two parts - the timestamp and everything else,
   * discarding the space in between (if any). The resulting timestamp can then
   * be passed to parseTimestamp to get a long value.
   * 
   * @param line
   *          The log line with timestamp
   * @return A String array with two entries, the timestamp and everything else.
   */
  public String[] splitLine(String line);

  /**
   * Parses a string and returns a timestamp in milliseconds since 1970.
   * 
   * @param timestamp
   *          A String representing a timestamp.
   * @return The millisecond value of the timestamp.
   * @throws ParseException
   *           If the data cannot be parsed
   */
  public long parseTimestatmp(String timestamp) throws ParseException;

  public String timestampToString(long timestamp);
}
