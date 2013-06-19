package com.rim.logdriver.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class AvroFileHeaderTest {
  private static final byte[] sync1 = new byte[] { '1', '1', '1', '1', '1',
      '1', '1', '1', '1', '1', '1', '1', '1', '1', '1', '1' };
  private static final byte[] sync2 = new byte[] { '2', '2', '2', '2', '2',
      '2', '2', '2', '2', '2', '2', '2', '2', '2', '2', '2' };

  private AvroFileHeader header1;
  private AvroFileHeader header2;
  private AvroFileHeader header3;
  private AvroFileHeader header4;

  @Before
  public void setup() {
    header1 = new AvroFileHeader();
    header1.setSchema("Schema1");
    header1.setCodec("Codec1");
    header1.setSyncMarker(sync1);

    header2 = new AvroFileHeader();
    header2.setSchema("Schema1");
    header2.setCodec("Codec1");
    header2.setSyncMarker(sync2);

    header3 = new AvroFileHeader();
    header3.setSchema("Schema1");
    header3.setCodec("Codec2");
    header3.setSyncMarker(sync1);

    header4 = new AvroFileHeader();
    header4.setSchema("Schema2");
    header4.setCodec("Codec1");
    header4.setSyncMarker(sync1);
  }

  @Test
  public void testReadWrite() throws IOException {
    byte[] buffer = header1.toBytes();
    ByteArrayInputStream in = new ByteArrayInputStream(buffer);
    AvroFileHeader newHeader = AvroFileHeader.readHeader(in);

    assertEquals(header1, newHeader);
  }

  @Test
  public void testSerializeDeserialize() throws IOException {
    // Write out the header
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(outputStream);
    header1.write(dataOut);

    // Read it in to a new header
    ByteArrayInputStream inputStream = new ByteArrayInputStream(
        outputStream.toByteArray());
    DataInputStream dataIn = new DataInputStream(inputStream);

    AvroFileHeader newHeader = new AvroFileHeader();
    newHeader.readFields(dataIn);

    // Compare
    assertEquals(header1, newHeader);
  }

  @Test
  public void testCompare() {
    // The correct sort order is headers 1, 2, 3, 4
    assertTrue(header1.compareTo(header1) == 0);
    assertTrue(header1.compareTo(header2) < 0);
    assertTrue(header1.compareTo(header3) < 0);
    assertTrue(header1.compareTo(header4) < 0);

    assertTrue(header2.compareTo(header1) > 0);
    assertTrue(header2.compareTo(header2) == 0);
    assertTrue(header2.compareTo(header3) < 0);
    assertTrue(header2.compareTo(header4) < 0);

    assertTrue(header3.compareTo(header1) > 0);
    assertTrue(header3.compareTo(header2) > 0);
    assertTrue(header3.compareTo(header3) == 0);
    assertTrue(header3.compareTo(header4) < 0);

    assertTrue(header4.compareTo(header1) > 0);
    assertTrue(header4.compareTo(header2) > 0);
    assertTrue(header4.compareTo(header3) > 0);
    assertTrue(header4.compareTo(header4) == 0);
  }

  @Test
  public void testComparator() {
    AvroFileHeader.Comparator c = new AvroFileHeader.Comparator();

    assertTrue(c.compare(header1, header1) == 0);
    assertTrue(c.compare(header1, header2) < 0);
    assertTrue(c.compare(header1, header3) < 0);
    assertTrue(c.compare(header1, header4) < 0);

    assertTrue(c.compare(header2, header1) > 0);
    assertTrue(c.compare(header2, header2) == 0);
    assertTrue(c.compare(header2, header3) < 0);
    assertTrue(c.compare(header2, header4) < 0);

    assertTrue(c.compare(header3, header1) > 0);
    assertTrue(c.compare(header3, header2) > 0);
    assertTrue(c.compare(header3, header3) == 0);
    assertTrue(c.compare(header3, header4) < 0);

    assertTrue(c.compare(header4, header1) > 0);
    assertTrue(c.compare(header4, header2) > 0);
    assertTrue(c.compare(header4, header3) > 0);
    assertTrue(c.compare(header4, header4) == 0);
  }

  @Test
  public void testRawCompare() throws IOException {
    // Convert all to byte arrays
    byte[] h1;
    byte[] h2;
    byte[] h3;
    byte[] h4;

    {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(outputStream);
      header1.write(dataOut);
      h1 = outputStream.toByteArray();
    }
    {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(outputStream);
      header2.write(dataOut);
      h2 = outputStream.toByteArray();
    }
    {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(outputStream);
      header3.write(dataOut);
      h3 = outputStream.toByteArray();
    }
    {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(outputStream);
      header4.write(dataOut);
      h4 = outputStream.toByteArray();
    }

    // Compare the byte arrays
    AvroFileHeader.Comparator c = new AvroFileHeader.Comparator();

    assertTrue(c.compare(h1, 0, h1.length, h1, 0, h1.length) == 0);
    assertTrue(c.compare(h1, 0, h1.length, h2, 0, h2.length) < 0);
    assertTrue(c.compare(h1, 0, h1.length, h3, 0, h3.length) < 0);
    assertTrue(c.compare(h1, 0, h1.length, h4, 0, h4.length) < 0);

    assertTrue(c.compare(h2, 0, h2.length, h1, 0, h1.length) > 0);
    assertTrue(c.compare(h2, 0, h2.length, h2, 0, h2.length) == 0);
    assertTrue(c.compare(h2, 0, h2.length, h3, 0, h3.length) < 0);
    assertTrue(c.compare(h2, 0, h2.length, h4, 0, h4.length) < 0);

    assertTrue(c.compare(h3, 0, h3.length, h1, 0, h1.length) > 0);
    assertTrue(c.compare(h3, 0, h3.length, h2, 0, h2.length) > 0);
    assertTrue(c.compare(h3, 0, h3.length, h3, 0, h3.length) == 0);
    assertTrue(c.compare(h3, 0, h3.length, h4, 0, h4.length) < 0);

    assertTrue(c.compare(h4, 0, h4.length, h1, 0, h1.length) > 0);
    assertTrue(c.compare(h4, 0, h4.length, h2, 0, h2.length) > 0);
    assertTrue(c.compare(h4, 0, h4.length, h3, 0, h3.length) > 0);
    assertTrue(c.compare(h4, 0, h4.length, h4, 0, h4.length) == 0);
  }
}
