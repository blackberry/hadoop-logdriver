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

package com.rim.logdriver.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;

import com.rim.logdriver.Schemas;

public class ReadBoom {
  private DataFileStream<Record> reader = null;

  private Deque<Record> lines = new ArrayDeque<Record>();

  public static void main(String[] args) throws IOException {
    new ReadBoom().run(args);
  }

  @SuppressWarnings("unchecked")
  public void run(String[] args) throws IOException {
    if (args.length == 0) {
      args = new String[] { "-" };
    }

    for (String file : args) {
      InputStream in = null;
      if (file.equals("-")) {
        in = System.in;
      } else {
        in = new FileInputStream(file);
      }

      GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(
          Schemas.getSchema("logBlock"));
      reader = new DataFileStream<Record>(in, datumReader);
      datumReader.setExpected(Schemas.getSchema("logBlock"));
      datumReader.setSchema(reader.getSchema());

      // read lines!
      while (lines.size() == 0) {
        if (reader.hasNext() == false) {
          break;
        }

        Record record = reader.next();
        Long blockNumber = (Long) record.get("blockNumber");
        Long createTime = (Long) record.get("createTime");
        Long second = (Long) record.get("second");
        Long lineNumber = 0l;

        for (Record line : (List<Record>) record.get("logLines")) {
          Long ms = (Long) line.get("ms");
          String message = line.get("message").toString();
          Integer eventId = (Integer) line.get("eventId");
          Long timestamp = second * 1000 + ms;

          System.out.println(timestamp + "\t" + message + "\t" + eventId + "\t"
              + createTime + "\t" + blockNumber + "\t" + lineNumber);

          lineNumber++;
        }
      }

    }
  }

}
