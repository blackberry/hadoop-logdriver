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

package com.rim.logdriver.mapred.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

import org.apache.avro.file.DataFileConstants;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.avro.AvroFileHeader;
import com.rim.logdriver.avro.AvroUtils;

public class AvroBlockRecordReader implements
    RecordReader<AvroFileHeader, BytesWritable> {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvroBlockRecordReader.class);

  private static final BytesWritable EMPTY_BYTES = new BytesWritable();

  private FileSystem fs = null;
  private FSDataInputStream in = null;

  private CombineFileSplit split = null;
  private int currentFile = -1;

  private long start = 0;
  private long end = 0;
  private long pos = 0;

  private AvroFileHeader header = null;

  public AvroBlockRecordReader(InputSplit split, JobConf job)
      throws IOException {
    this.split = (CombineFileSplit) split;
    fs = FileSystem.get(job);
  }

  private void advanceToSyncMarker(FSDataInputStream in, byte[] syncMarker)
      throws IOException {
    byte b = 0;
    int bytesRead = 0;
    byte[] sync = header.getSyncMarker();
    Iterator<Byte> iterator = null;
    boolean match = true;

    Deque<Byte> deque = new ArrayDeque<Byte>(DataFileConstants.SYNC_SIZE);
    while (true) {
      b = in.readByte();
      deque.add(b);
      bytesRead++;

      match = true;
      if (deque.size() == DataFileConstants.SYNC_SIZE) {
        match = true;
        iterator = deque.iterator();
        for (int i = 0; i < DataFileConstants.SYNC_SIZE; i++) {
          if (sync[i] != iterator.next()) {
            match = false;
            break;
          }
        }

        if (match) {
          break;
        }

        deque.remove();
      }
    }

    pos = start + bytesRead;
    LOG.info("Found sync marker at {}", pos - 16);
    in.seek(pos);
  }

  private void initCurrentFile() throws IOException {
    LOG.info("Initializing {}:{}+{}",
        new Object[] { split.getPath(currentFile),
            split.getOffset(currentFile), split.getLength(currentFile) });
    start = split.getOffset(currentFile);
    end = start + split.getLength(currentFile);

    // Open the file.
    in = fs.open(split.getPath(currentFile));

    // Read the header, validate it, and save it.
    // If this is a zero length split, then don't try to read the header. The
    // bext call to next() will just cause it to skip to the next file, no harm
    // done.
    if (start == end) {
      header = new AvroFileHeader();
      pos = start;
    } else {
      header = AvroFileHeader.readHeader(in);

      // Seek to the start of the split
      in.seek(start);

      // Seek to the next sync marker
      advanceToSyncMarker(in, header.getSyncMarker());

      pos = in.getPos();
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }

  @Override
  public float getProgress() throws IOException {
    // The fraction of files read, plus where we are within that file
    long currentPosition = 0;
    for (int i = 0; i < currentFile && i < split.getNumPaths(); i++) {
      currentPosition += split.getLength(i);
    }
    if (currentFile < split.getNumPaths()) {
      currentPosition += getPos() - split.getOffset(currentFile);
    }

    return (float) (1.0 * currentPosition / split.getLength());
  }

  @Override
  public boolean next(AvroFileHeader key, BytesWritable value)
      throws IOException {
    while (pos >= end) {
      if (in != null) {
        in.close();
      }
      currentFile++;
      if (split.getNumPaths() > currentFile) {
        initCurrentFile();
      } else {
        return false;
      }
    }

    key.set(header);

    // Get the number of entries in the next block
    int entries = AvroUtils.readInt(in);
    byte[] block = AvroUtils.readBytes(in);

    // Check that the sync marker is what we expect
    LOG.trace("Verifying sync marker");
    byte[] syncMarker = AvroUtils.readBytes(in, DataFileConstants.SYNC_SIZE);
    if (!Arrays.equals(syncMarker, header.getSyncMarker())) {
      LOG.error("Sync marker does not match");
      return false;
    }

    // Now, pack it all back into a byte[], and set the value of value
    {
      ByteBuffer bb = ByteBuffer.allocate(10 + 10 + block.length);
      bb.put(AvroUtils.encodeLong(entries));
      bb.put(AvroUtils.encodeLong(block.length));
      bb.put(block);
      byte[] result = new byte[bb.position()];
      bb.rewind();
      bb.get(result);
      value.set(result, 0, result.length);

      pos = in.getPos();
    }

    return true;
  }

  @Override
  public AvroFileHeader createKey() {
    return new AvroFileHeader();
  }

  @Override
  public BytesWritable createValue() {
    return new BytesWritable();
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }
}
