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

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateFormatter extends EvalFunc<String> {
  private static final Logger LOG = LoggerFactory
      .getLogger(DateFormatter.class);

  private static final String RFC822_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final String RFC822_SEC_UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  // Not really RFC3164 - the day is zero padded instead of space padded
  private static final String RFC3164_FORMAT = "MMM dd HH:mm:ss";
  private static final String RFC5424_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";

  private final FastDateFormat format;

  public DateFormatter(String formatString) {
    if ("RFC822".equals(formatString.toUpperCase())) {
      format = FastDateFormat.getInstance(RFC822_FORMAT);
    } else if ("RFC822_SEC_UTC".equals(formatString.toUpperCase())) {
      format = FastDateFormat.getInstance(RFC822_SEC_UTC_FORMAT);
    } else if ("RFC3164".equals(formatString.toUpperCase())) {
      format = FastDateFormat.getInstance(RFC3164_FORMAT);
    } else if ("RFC5424".equals(formatString.toUpperCase())) {
      format = FastDateFormat.getInstance(RFC5424_FORMAT);
    } else {
      format = FastDateFormat.getInstance(formatString);
    }
  }

  @Override
  public String exec(Tuple t) throws IOException {
    Object in = t.get(0);

    if (in == null) {
      LOG.info("Got null arguement");
      return null;
    }

    if (!in.getClass().equals(java.lang.Long.class)) {
      LOG.info("Bad input class: {} [expected java.lang.Long]", in.getClass());
      return null;
    }

    return format.format(in);
  }
}
