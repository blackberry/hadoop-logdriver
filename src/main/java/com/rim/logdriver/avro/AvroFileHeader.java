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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.file.DataFileConstants;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFileHeader implements WritableComparable<AvroFileHeader> {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvroFileHeader.class);

  private static final byte[] MAGIC = new byte[] { 'O', 'b', 'j', 1 };

  private String schema = null;
  private String codec = null;
  private byte[] syncMarker = null;

  public void set(AvroFileHeader header) {
    this.schema = header.schema;
    this.codec = header.codec;
    this.syncMarker = header.syncMarker;
  }

  public static AvroFileHeader readHeader(InputStream in) throws IOException {
    AvroFileHeader header = new AvroFileHeader();

    // Read the magic number.
    {
      byte[] buf = new byte[4];
      int bytesRead = 0;
      int pos = 0;
      while (pos < 4 && bytesRead > -1) {
        bytesRead = in.read(buf, pos, 4 - pos);
        pos += bytesRead;
      }
      if (bytesRead == -1) {
        throw new IOException("Not enough bytes to read.");
      }
      if (!Arrays.equals(buf, MAGIC)) {
        throw new IOException("Wrong magic number");
      }
    }

    // Read the metadata (but only care about schema and codec)
    {
      Map<String, byte[]> meta = AvroUtils.readMap(in);
      if (meta.containsKey("avro.schema")) {
        header.setSchema(new String(meta.get("avro.schema"), "UTF-8"));
      }
      if (meta.containsKey("avro.codec")) {
        header.setCodec(new String(meta.get("avro.codec"), "UTF-8"));
      }
    }
    // Read the sync marker
    {
      byte[] buf = new byte[DataFileConstants.SYNC_SIZE];
      int bytesRead = 0;
      int pos = 0;
      while (pos < DataFileConstants.SYNC_SIZE && bytesRead > -1) {
        bytesRead = in.read(buf, pos, DataFileConstants.SYNC_SIZE - pos);
        pos += bytesRead;
      }
      if (bytesRead == -1) {
        throw new IOException("Not enough bytes to read.");
      }
      header.setSyncMarker(buf);
    }

    LOG.trace("Read header");
    LOG.trace("  schema={}", header.schema);
    LOG.trace("  codec={}", header.codec);
    LOG.trace("  syncMarker={}", header.syncMarker);
    return header;
  }

  public byte[] toBytes() throws IOException {
    int bufferSize = MAGIC.length // magic bytes
        + 10 // long for number of entries in the map
        + 10 + "avro.schema".length() // Length of key + key
        + 10 + schema.length() // Length of value + value
        + 10 + "avro.codec".length() // Length of key + key
        + 10 + codec.length() // Length of value + value
        + 1 // 0 to end map
        + DataFileConstants.SYNC_SIZE; // Sync marker
    ByteBuffer bb = ByteBuffer.allocate(bufferSize);

    bb.put(MAGIC);

    bb.put(AvroUtils.encodeLong(2));

    bb.put(AvroUtils.encodeLong("avro.schema".length()));
    bb.put("avro.schema".getBytes("UTF-8"));
    bb.put(AvroUtils.encodeLong(schema.length()));
    bb.put(schema.getBytes("UTF-8"));

    bb.put(AvroUtils.encodeLong("avro.codec".length()));
    bb.put("avro.codec".getBytes("UTF-8"));
    bb.put(AvroUtils.encodeLong(codec.length()));
    bb.put(codec.getBytes("UTF-8"));

    bb.put((byte) 0);

    bb.put(syncMarker);

    byte[] result = new byte[bb.position()];
    bb.rewind();
    bb.get(result);

    return result;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getCodec() {
    return codec;
  }

  public void setCodec(String codec) {
    this.codec = codec;
  }

  public byte[] getSyncMarker() {
    return syncMarker;
  }

  public void setSyncMarker(byte[] syncMarker) {
    this.syncMarker = syncMarker;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(schema.length());
    out.write(schema.getBytes("UTF-8"));
    out.writeInt(codec.length());
    out.write(codec.getBytes("UTF-8"));
    out.write(syncMarker);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] buf;
    int len;

    len = in.readInt();
    buf = new byte[len];
    in.readFully(buf);
    schema = new String(buf, "UTF-8");

    len = in.readInt();
    buf = new byte[len];
    in.readFully(buf);
    codec = new String(buf, "UTF-8");

    buf = new byte[DataFileConstants.SYNC_SIZE];
    in.readFully(buf);
    syncMarker = buf;
  }

  @Override
  public int compareTo(AvroFileHeader o) {
    int result = schema.compareTo(o.schema);
    if (result != 0) {
      return result;
    }

    result = codec.compareTo(o.codec);
    if (result != 0) {
      return result;
    }

    for (int i = 0; i < DataFileConstants.SYNC_SIZE; i++) {
      result = syncMarker[i] - o.syncMarker[i];
      if (result != 0) {
        return result;
      }
    }

    return 0;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((codec == null) ? 0 : codec.hashCode());
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    result = prime * result + Arrays.hashCode(syncMarker);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    AvroFileHeader other = (AvroFileHeader) obj;
    if (codec == null) {
      if (other.codec != null)
        return false;
    } else if (!codec.equals(other.codec))
      return false;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    if (!Arrays.equals(syncMarker, other.syncMarker))
      return false;
    return true;
  }

  /**
   * A Comparator optimized for AvroFileHeader.
   */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(AvroFileHeader.class);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      return ((AvroFileHeader) a).compareTo((AvroFileHeader) b);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int pos;
      int len1;
      int len2;
      int result;

      len1 = readInt(b1, s1);
      len2 = readInt(b2, s2);
      result = compareBytes(b1, 8, len1, b2, 8, len2);
      if (result != 0) {
        return result;
      }

      pos = 8 + len1;
      len1 = readInt(b1, s1);
      len2 = readInt(b2, s2);
      result = compareBytes(b1, pos, len1, b2, pos, len2);
      if (result != 0) {
        return result;
      }

      pos += 8 + len1;
      result = compareBytes(b1, pos, DataFileConstants.SYNC_SIZE, b1, pos,
          DataFileConstants.SYNC_SIZE);

      return result;
    }
  }

  static {
    WritableComparator.define(AvroFileHeader.class, new Comparator());
  }

  @Override
  public String toString() {
    return "[" + schema + "," + codec + "," + syncMarker + "]";
  }

}
