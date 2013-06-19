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

package com.rim.logdriver.avro;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class AvroUtils {
  private AvroUtils() {
  }

  public static int readInt(InputStream inputStream) throws IOException {
    int value = 0;
    int i = 0;
    long b;
    while (((b = inputStream.read()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i >= 7 * 5) {
        throw new IOException(
            "Didn't reach the end of the varint after 5 bytes.");
      }
    }
    value |= b << i;

    // un-zig-zag it
    int temp = (((value << 31) >> 31) ^ value) >> 1;
    // since we lost that first bit during all that zigging and zagging,
    // make sure it's the right one now
    value = temp ^ (value & (1 << 31));

    return value;
  }

  public static long readLong(InputStream inputStream) throws IOException {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = inputStream.read()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i >= 7 * 10) {
        throw new IOException(
            "Didn't reach the end of the long varint after 10 bytes.");
      }
    }
    value |= b << i;

    // un-zig-zag it
    long temp = (((value << 63) >> 63) ^ value) >> 1;
    // since we lost that first bit during all that zigging and zagging,
    // make sure it's the right one now
    value = temp ^ (value & (1L << 63));

    return value;
  }

  public static String readString(InputStream inputStream) throws IOException {
    int length = readInt(inputStream);

    byte[] buffer = new byte[length];
    int bytesRead = 0;
    int limit = 0;
    while (limit < length && bytesRead != -1) {
      bytesRead = inputStream.read(buffer, limit, length - limit);
      limit += bytesRead;
    }
    if (limit < length) {
      throw new IOException("Not enough bytes to read the string.");
    }
    String value = new String(buffer, "UTF-8");
    return value;
  }

  public static byte[] readBytes(InputStream inputStream) throws IOException {
    int length = readInt(inputStream);
    return readBytes(inputStream, length);
  }

  public static byte[] readBytes(InputStream inputStream, int length)
      throws IOException {
    byte[] buffer = new byte[length];
    int bytesRead = 0;
    int limit = 0;
    while (limit < length && bytesRead != -1) {
      bytesRead = inputStream.read(buffer, limit, length - limit);
      limit += bytesRead;
    }
    if (limit < length) {
      throw new IOException("Not enough bytes to read.");
    }
    return buffer;
  }

  public static byte[] encodeLong(long number) {
    if (number == 0) {
      return new byte[] { 0 };
    }

    // Reformat it to zig-zag encoding
    number = (number << 1) ^ (number >> 63);

    byte[] buffer = new byte[10];

    int numBytes = 0;
    while ((number & 0xFFFFFF80) != 0) {
      buffer[numBytes] = (byte) ((number & 0x7F) | 0x80);
      number >>>= 7;
      numBytes++;
    }
    buffer[numBytes] = (byte) (number & 0x7F);
    numBytes++;

    byte[] result = new byte[numBytes];
    System.arraycopy(buffer, 0, result, 0, numBytes);
    return result;
  }

  public static Map<String, byte[]> readMap(InputStream in) throws IOException {
    Map<String, byte[]> map = new HashMap<String, byte[]>();
    long numKeys;
    String key;
    byte[] value;
    while (true) {
      numKeys = readLong(in);
      if (numKeys == 0) {
        break;
      }
      if (numKeys < 0) {
        numKeys = -numKeys;
        readLong(in);
      }
      for (int i = 0; i < numKeys; i++) {
        key = readString(in);
        value = readBytes(in);
        map.put(key, value);
      }
    }

    return map;
  }

}
