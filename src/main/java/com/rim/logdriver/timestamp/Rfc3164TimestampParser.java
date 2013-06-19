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
 * Parses timestamps according to RFC3164.
 * <p>
 * This class uses SimpleDateFormat, and so it is not thread safe.
 */
package com.rim.logdriver.timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.FastDateFormat;

public class Rfc3164TimestampParser implements TimestampParser {
  private final SimpleDateFormat dateFormat = new SimpleDateFormat(
      "yyyy MMM dd HH:mm:ss");
  private final FastDateFormat dateFormatWriter = FastDateFormat
      .getInstance("MMM dd HH:mm:ss");

  @Override
  public String[] splitLine(String line) {
    if (line.length() >= 16) {
      String[] result = new String[2];
      result[0] = line.substring(0, 15);
      result[1] = line.substring(16);
      return result;
    } else {
      return new String[] { null, line };
    }
  }

  @Override
  public long parseTimestatmp(String timestamp) throws ParseException {
    if (timestamp == null) {
      throw new ParseException("Timestamp is null", 0);
    }

    // Before we parse the date, add the current year to the string. That way,
    // we get a correct timestamp. If I try to add the year after, we run into
    // issues with leap years.
    Calendar currentCalendar = Calendar.getInstance();
    int currentYear = currentCalendar.get(Calendar.YEAR);
    Date date = dateFormat.parse(currentYear + " " + timestamp);
    Calendar logCalendar = Calendar.getInstance();
    logCalendar.setTime(date);

    // If today is the first of January, and the log is for the 31st of
    // December, then we probably just used the wrong year. So lets roll the
    // year back by one in that case.
    if (logCalendar.get(Calendar.DAY_OF_MONTH) == 31
        && logCalendar.get(Calendar.MONTH) == Calendar.DECEMBER
        && currentCalendar.get(Calendar.DAY_OF_MONTH) == 1
        && currentCalendar.get(Calendar.MONTH) == Calendar.JANUARY) {
      logCalendar.add(Calendar.YEAR, -1);

      // Contrariwise, a bad clock can give us Jan 1 logs on December 31
    } else if (currentCalendar.get(Calendar.DAY_OF_MONTH) == 31
        && currentCalendar.get(Calendar.MONTH) == Calendar.DECEMBER
        && logCalendar.get(Calendar.DAY_OF_MONTH) == 1
        && logCalendar.get(Calendar.MONTH) == Calendar.JANUARY) {
      logCalendar.add(Calendar.YEAR, 1);

    }

    return logCalendar.getTimeInMillis();
  }

  @Override
  public String timestampToString(long timestamp) {
    return dateFormatWriter.format(timestamp);
  }
}
