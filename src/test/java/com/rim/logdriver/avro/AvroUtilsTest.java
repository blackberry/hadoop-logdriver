package com.rim.logdriver.avro;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class AvroUtilsTest {
  private static final byte[] val_0 = { 0 };
  private static final byte[] val_maxint = { -2, -1, -1, -1, 15 };
  private static final byte[] val_minint = { -1, -1, -1, -1, 15 };
  private static final byte[] val_maxlong = { -2, -1, -1, -1, -1, -1, -1, -1,
      -1, 1 };
  private static final byte[] val_minlong = { -1, -1, -1, -1, -1, -1, -1, -1,
      -1, 1 };
  private static final byte[] val_toobig_int = { -1, -1, -1, -1, -1, 1 };
  private static final byte[] val_toobig_long = { -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, 1 };

  @Test
  public void testReadWriteInt() throws IOException {
    // read tests
    ByteArrayInputStream in_0 = new ByteArrayInputStream(val_0);
    assertEquals(0, AvroUtils.readInt(in_0));

    ByteArrayInputStream in_maxint = new ByteArrayInputStream(val_maxint);
    assertEquals(Integer.MAX_VALUE, AvroUtils.readInt(in_maxint));

    ByteArrayInputStream in_minint = new ByteArrayInputStream(val_minint);
    assertEquals(Integer.MIN_VALUE, AvroUtils.readInt(in_minint));

    Exception e = null;
    try {
      ByteArrayInputStream in_toobig = new ByteArrayInputStream(val_toobig_int);
      AvroUtils.readInt(in_toobig);
    } catch (Exception ex) {
      e = ex;
    }
    assertEquals("Didn't reach the end of the varint after 5 bytes.",
        e.getMessage());

    // write/read tests
    int[] vals = new int[] { Integer.MIN_VALUE, 0, Integer.MAX_VALUE, 12334,
        -836726 };
    for (int val : vals) {
      assertEquals(val, AvroUtils.readInt(new ByteArrayInputStream(AvroUtils
          .encodeLong(val))));
    }
  }

  @Test
  public void testReadWriteLong() throws IOException {
    // read tests
    ByteArrayInputStream in_0 = new ByteArrayInputStream(val_0);
    assertEquals(0l, AvroUtils.readLong(in_0));

    ByteArrayInputStream in_maxlong = new ByteArrayInputStream(val_maxlong);
    assertEquals(Long.MAX_VALUE, AvroUtils.readLong(in_maxlong));

    ByteArrayInputStream in_minlong = new ByteArrayInputStream(val_minlong);
    assertEquals(Long.MIN_VALUE, AvroUtils.readLong(in_minlong));

    Exception e = null;
    try {
      ByteArrayInputStream in_toobig = new ByteArrayInputStream(val_toobig_long);
      AvroUtils.readLong(in_toobig);
    } catch (Exception ex) {
      e = ex;
    }
    assertEquals("Didn't reach the end of the long varint after 10 bytes.",
        e.getMessage());

    // write/read tests
    // write/read tests
    long[] vals = new long[] { Long.MIN_VALUE, 0, Long.MAX_VALUE, 1782389834,
        -836788926 };
    for (long val : vals) {
      assertEquals(val, AvroUtils.readLong(new ByteArrayInputStream(AvroUtils
          .encodeLong(val))));
    }
  }

  @Test
  public void testReadString() throws IOException {
    String testString = "test";
    byte[] testBytes = new byte[] { 0x08, 0x74, 0x65, 0x73, 0x74 };

    assertEquals(testString,
        AvroUtils.readString(new ByteArrayInputStream(testBytes)));
  }

  @Test
  public void testReadBytes() throws IOException {
    byte[] bytes = new byte[] { 0x74, 0x65, 0x73, 0x74 };
    byte[] encodedBytes = new byte[] { 0x08, 0x74, 0x65, 0x73, 0x74 };

    assertArrayEquals(bytes,
        AvroUtils.readBytes(new ByteArrayInputStream(encodedBytes)));
  }

  @Test
  public void testReadMap() throws IOException {
    Map<String, byte[]> map = new HashMap<String, byte[]>();
    map.put("ABC", new byte[] { 0x74, 0x65, 0x73, 0x74 });
    map.put("XYZ",
        new byte[] { 0x74, 0x65, 0x73, 0x74, 0x74, 0x65, 0x73, 0x74 });
    byte[] encodedMap1 = new byte[] { 0x04, // 2 entries
        0x06, 0x41, 0x42, 0x43, // size 3 : ABC
        0x08, 0x74, 0x65, 0x73, 0x74, // size 4 : value
        0x06, 0x58, 0x59, 0x5A, // size 3 : XYZ
        0x10, 0x74, 0x65, 0x73, 0x74, 0x74, 0x65, 0x73, 0x74, // size 8 : value
        0x00 // done!
    };
    byte[] encodedMap2 = new byte[] { 0x02, // 1 entry
        0x06, 0x41, 0x42, 0x43, // size 3 : ABC
        0x08, 0x74, 0x65, 0x73, 0x74, // size 4 : value
        0x02, // 1 entry
        0x06, 0x58, 0x59, 0x5A, // size 3 : XYZ
        0x10, 0x74, 0x65, 0x73, 0x74, 0x74, 0x65, 0x73, 0x74, // size 8 : value
        0x00 // done!
    };

    {
      Map<String, byte[]> result = AvroUtils.readMap(new ByteArrayInputStream(
          encodedMap1));
      assertEquals(map.keySet(), result.keySet());
      for (String key : map.keySet()) {
        assertArrayEquals(map.get(key), result.get(key));
      }
    }

    {
      Map<String, byte[]> result = AvroUtils.readMap(new ByteArrayInputStream(
          encodedMap2));
      assertEquals(map.keySet(), result.keySet());
      for (String key : map.keySet()) {
        assertArrayEquals(map.get(key), result.get(key));
      }
    }

  }

}
