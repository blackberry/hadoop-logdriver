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

package com.rim.logdriver.timestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.time.FastDateFormat;

public class Rfc5424TimestampParser implements TimestampParser {
  private final SimpleDateFormat dateFormat = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ss");
  private final FastDateFormat dateFormatWriter = FastDateFormat
      .getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  private static final class SizeLimitedLinkedHashMap<K, V> extends
      LinkedHashMap<K, V> {
    private static final long serialVersionUID = 1L;
    private static final int MAX_ENTRIES = 120;

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > MAX_ENTRIES;
    }
  }

  private final Map<String, Long> dateCache = new SizeLimitedLinkedHashMap<String, Long>();
  private final boolean daylightSavings;
  private final TimeZone tz;
  private final long tzOffset;

  public Rfc5424TimestampParser() {
    tz = Calendar.getInstance().getTimeZone();
    tzOffset = tz.getRawOffset();
    daylightSavings = tz.useDaylightTime();
  }

  @Override
  public String[] splitLine(String line) {
    String[] split = line.split(" ", 2);
    if (split.length == 1) {
      return new String[] { null, line };
    } else {
      return split;
    }
  }

  @Override
  public long parseTimestatmp(String timestamp) throws ParseException {
    try {
      // Parse the easy part of the string
      String firstPart = timestamp.substring(0, 19);
      Long time;
      time = dateCache.get(firstPart);
      if (time == null) {
        time = dateFormat.parse(firstPart).getTime();
        dateCache.put(firstPart, time);
      }
      int currentIndex = 19;
      char c = timestamp.charAt(currentIndex);

      // Check for fractional seconds to add. We only record up to millisecond
      // precision, so only grab up to three digits.
      if (timestamp.charAt(currentIndex) == '.') {
        // There are fractional seconds, so grab up to 3.
        // The first digit is guaranteed by the spec. After that, we need to
        // check if we still have digits.
        // The spec requires a timezone, so we can't run out of digits
        // before we run out of string.
        currentIndex++;
        c = timestamp.charAt(currentIndex);
        time += 100 * Character.getNumericValue(c);
        currentIndex++;
        c = timestamp.charAt(currentIndex);
        if (Character.isDigit(c)) {
          time += 10 * Character.getNumericValue(c);
          currentIndex++;
          c = timestamp.charAt(currentIndex);
          if (Character.isDigit(c)) {
            time += Character.getNumericValue(c);
            currentIndex++;
            c = timestamp.charAt(currentIndex);
            // Now just go through the digits until we're done.
            while (Character.isDigit(c)) {
              currentIndex++;
              c = timestamp.charAt(currentIndex);
            }
          }
        }

      }

      // Now adjust for timezone offset. either Z or +/-00:00
      boolean positiveTimeZone = true;
      if (c == 'Z') {
        // That's fine. No adjustment.
      } else {
        if (c == '+') {
          positiveTimeZone = true;
        } else if (c == '-') {
          positiveTimeZone = false;
        } else {
          throw new IllegalArgumentException("Malformed date:" + timestamp);
        }

        // Grab the next 2 for hour. Then skip the colon and grab the next
        // 2.
        currentIndex++;
        int hour = Integer.parseInt(timestamp.substring(currentIndex,
            currentIndex + 2));
        currentIndex += 2;
        c = timestamp.charAt(currentIndex);
        if (c != ':') {
          throw new IllegalArgumentException("Malformed date:" + timestamp);
        }
        currentIndex++;
        int minute = Integer.parseInt(timestamp.substring(currentIndex,
            currentIndex + 2));

        int offset = (60 * hour + minute) * 60 * 1000;
        if (positiveTimeZone) {
          time -= offset;
        } else {
          time += offset;
        }

      }

      // If we support daylight savings, then we need to keep checking if we're
      // in
      // daylight savings or not.
      if (daylightSavings) {
        time += tz.getOffset(time);
      } else {
        time += tzOffset;
      }

      return time;
    } catch (ParseException e) {
      throw e;
    } catch (Throwable t) {
      ParseException e = new ParseException("Unexpected Exception", 0);
      e.initCause(t);
      throw e;
    }
  }

  @Override
  public String timestampToString(long timestamp) {
    return dateFormatWriter.format(timestamp);
  }
}
