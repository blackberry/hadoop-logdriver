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
 * search Logs in a given file set.
 * <p>
 * Usage: [genericOptions] [-Dlogdriver.search.start.time=X] [-Dlogdriver.search.end.time=X] searchString input [input ...] output
 * <p>
 * 
 */
package com.rim.logdriver.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.Schemas;
import com.rim.logdriver.avro.AvroFileHeader;
import com.rim.logdriver.avro.AvroUtils;
import com.rim.logdriver.mapreduce.avro.AvroBlockInputFormat;

public class FastSearch extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(FastSearch.class);

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  // The expected size of the data blocks, after decompressing. Use what we're
  // using for avro sync interval, plus a bit for overrun.
  private static final int BUFFER_SIZE = Math.round(1.2f * 2 * 1024 * 1024);

  private static final String DEFAULT_OUTPUT_SEPARATOR = "\t";
  private static final boolean DEFAULT_WAIT_JOB = true;

  private static final class SearchMapper extends
      Mapper<AvroFileHeader, BytesWritable, Text, NullWritable> {
    private long start;
    private long end;
    private String pattern;
    private byte[] patternBytes;
    private byte[][] patternBytesCaseInsensitive;
    private Inflater inflater;
    private String outputSeparator;
    private boolean caseSensitive = true;
    private boolean caseInsensitive = false;
    private boolean unicode = false;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();

      start = conf.getLong("logdriver.search.start.time", Long.MIN_VALUE);
      end = conf.getLong("logdriver.search.end.time", Long.MAX_VALUE);
      String patternBase64 = conf.get("logdriver.search.string");
      pattern = new String(Base64.decodeBase64(patternBase64),"UTF-8");
      caseInsensitive = conf.getBoolean("logdriver.search.case.insensitive",
          false);
      caseSensitive = !caseInsensitive;
      
      
      // Create byte array for exact pattern if case sensitivity is selected
      if (caseSensitive) {
    	LOG.info("Case sensitive search for {}", pattern);
        try {
          patternBytes = pattern.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
          LOG.error("UTF-8 is unsupported.  Trying with default encoding.", e);
          patternBytes = pattern.getBytes();
        }
        LOG.info("Pattern bytes are {}", Arrays.toString(patternBytes));
      } else {
        // Split pattern into array of strings (char will break UTF-8 encoding)
        String[] patternCharacters = pattern.split("(?!^)");

        // Are any of the characters in the pattern more than one byte? If so,
        // set unicode mode and skip bytescanning.
        if (pattern.toUpperCase().getBytes("UTF-8").length > pattern.length() || pattern.toLowerCase().getBytes("UTF-8").length > pattern.length()) {
        	unicode = true;
        }
        
        // If we're not case sensitive and we're not worrying about unicode,
        // we'll make a two dimension byte array
        if (!unicode) {
          patternBytesCaseInsensitive = new byte[patternCharacters.length][2];
          // Fill the array with bytes for each case
          for (int patternPosition = 0; patternPosition < patternCharacters.length; patternPosition++) {
            patternBytesCaseInsensitive[patternPosition][0] = patternCharacters[patternPosition]
                .toLowerCase().getBytes()[0];
            patternBytesCaseInsensitive[patternPosition][1] = patternCharacters[patternPosition]
                .toUpperCase().getBytes()[0];
          }
        }
      }

      inflater = new Inflater(true);

      outputSeparator = new String(new byte[] { Byte.parseByte(conf
          .get("logdriver.output.field.separator")) }, UTF_8);

      LOG.info("Configuring SearchMapper");
      LOG.info("  start={}", start);
      LOG.info("  end={}", end);
      LOG.info("  pattern={}", pattern);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void map(AvroFileHeader key, BytesWritable value, Context context)
        throws IOException, InterruptedException {

      LOG.trace("Got chunk with {} bytes", value.getLength());
      if (value.getLength() == 0) {
        return;
      }

      // First, grab the headers off the block, then decompress the block
      ByteArrayInputStream bytesIn = new ByteArrayInputStream(value.getBytes());
      int entries = AvroUtils.readInt(bytesIn);
      int dataLength = AvroUtils.readInt(bytesIn);
      LOG.trace("Entries = {}, Data Length={}", entries, dataLength);

      byte[] inBytes = new byte[dataLength];
      int bytesRead = 0;
      int pos = 0;
      while (bytesRead >= 0 && pos < dataLength) {
        bytesRead = bytesIn.read(inBytes, pos, inBytes.length - pos);
        if (bytesRead > 0) {
          pos += bytesRead;
        }
      }
      if (pos != dataLength) {
        throw new IOException(
            "Read a different number of bytes than expected (" + pos + "!="
                + dataLength + ")");
      }

      byte[] buf = new byte[BUFFER_SIZE];
      bytesRead = -1;
      pos = 0;
      inflater.reset();
      inflater.setInput(inBytes);
      while (!inflater.finished() && bytesRead != 0) {
        if (pos == buf.length) {
          LOG.info("Expanding output buffer from {} to {}.", buf.length,
              buf.length * 2);
          byte[] newBuf = new byte[buf.length * 2];
          System.arraycopy(buf, 0, newBuf, 0, buf.length);
          buf = newBuf;
        }
        try {
          bytesRead = inflater.inflate(buf, pos, buf.length - pos);
        } catch (DataFormatException e) {
          throw new IOException("Error inflating data block.", e);
        }
        pos += bytesRead;
        LOG.trace("BytesRead = {}, Position = {}", bytesRead, pos);
      }

      LOG.debug(
          "Read block. Compressed size {}, Expanded size {}, Record count {}",
          new Object[] { dataLength, pos, entries });

      // Find out if the string we're looking for is in the data block
      // somewhere.

      boolean match = false;
      // If we're doing a case sensitive search, use patternBytes[]
      if (caseSensitive) {
        if (patternBytes.length > buf.length) {
          return;
        }

        BUFFER_INDEX: for (int i = 0; i < buf.length - patternBytes.length; i++) {
          for (int j = 0; j < patternBytes.length; j++) {
            if (buf[i + j] != patternBytes[j]) {
              continue BUFFER_INDEX;
            }
          }
          // Hey, we found a match!
          match = true;
          break BUFFER_INDEX;
        }
      }
      // If this is a case insensitive search but we don't care about unicode,
      // assume each character is one byte. Same as above, but check both cases.
      else if (caseInsensitive && !unicode) {
        if (patternBytesCaseInsensitive.length > buf.length) {
          return;
        }
        // Iterate the starting position through the buffer byte by byte
        BUFFER_INDEX: for (int bufferPosition = 0; bufferPosition < buf.length
            - patternBytesCaseInsensitive.length; bufferPosition++) {
          // For each buffer position iterate through character positions in the
          // pattern
          for (int patternPosition = 0; patternPosition < patternBytesCaseInsensitive.length; patternPosition++) {
            // Check through the available cases. If one matches,
            // move to the next position in the pattern
            if (buf[bufferPosition + patternPosition] != patternBytesCaseInsensitive[patternPosition][0]
                && buf[bufferPosition + patternPosition] != patternBytesCaseInsensitive[patternPosition][1]) {
              continue BUFFER_INDEX;
            }
          }
          // If we got here all positions in the pattern matched,
          // so break out of the loop and decode the block
          match = true;
          break BUFFER_INDEX;
        }
      }
      // If this is a case insensitive search and we do care about unicode
      // we're just going to decode the blocks and scan line by line.
      else {
        match = true;
      }

      // If we know there is a match, then we can decode and go line by line.
      if (match) {
        LOG.info("There is a match in this block.");

        GenericDatumReader<Record> datumReader = new GenericDatumReader<Record>(
            new Schema.Parser().parse(key.getSchema()),
            Schemas.getSchema("logBlock"));
        Record record = null;
        Decoder decoder = DecoderFactory.get().binaryDecoder(buf, null);
        long second = 0;
        long blockNumber;
        long createTime;
        long lineNumber = 0;
        long ms = 0;
        for (int i = 0; i < entries; i++) {
          record = datumReader.read(record, decoder);
          LOG.trace("Read record {}", record);

          second = (Long) record.get("second") * 1000;
          if (second < start || second >= end) {
            continue;
          }

          blockNumber = (Long) record.get("blockNumber");
          createTime = (Long) record.get("createTime");

          lineNumber = 0l;

          for (Record line : (List<Record>) record.get("logLines")) {
            String message = line.get("message").toString();
            // Compare lines to the pattern, converting everything into
            // upper case if the caseSensitive option isn't selected
            if ((caseSensitive && message.contains(pattern))
                || (caseInsensitive && message.toUpperCase().contains(
                    pattern.toUpperCase()))) {
              ms = (Long) line.get("ms");

              ++lineNumber;

              StringBuilder sb = new StringBuilder().append((second + ms))
                  .append(outputSeparator).append(StringUtils.chomp(message))
                  .append(outputSeparator).append(line.get("eventId"))
                  .append(outputSeparator).append(createTime)
                  .append(outputSeparator).append(blockNumber)
                  .append(outputSeparator).append(lineNumber);
              context.write(new Text(sb.toString()), null);
            }
          }
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf(); // Configuration processed by ToolRunner
    // If run by Oozie, then load the Oozie conf too
    if (System.getProperty("oozie.action.conf.xml") != null) {
      conf.addResource(new URL("file://"
          + System.getProperty("oozie.action.conf.xml")));
    }

    FileSystem fs = FileSystem.get(conf);

    // The command line options
    String searchString = null;
    List<Path> paths = new ArrayList<Path>();
    Path outputDir = null;

    // Load input files from the command line
    if (args.length < 3) {
      System.out
          .println("usage: [genericOptions] searchString input [input ...] output");
      System.exit(1);
    }

    // Get the files we need from the command line.
    searchString = args[0];
    for (int i = 1; i < args.length - 1; i++) {
      for (FileStatus f : fs.globStatus(new Path(args[i]))) {
        paths.add(f.getPath());
      }
    }
    outputDir = new Path(args[args.length - 1]);

    Job job = new Job(conf);
    Configuration jobConf = job.getConfiguration();

    job.setJarByClass(FastSearch.class);
    jobConf.setIfUnset("mapred.job.name", "Search Files");

    // To propagate credentials within Oozie
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      jobConf.set("mapreduce.job.credentials.binary",
          System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }

    // Good output separators include things that are unsupported by XML. So we
    // just send the byte value of the character through. The restriction here
    // is that it can't be more than 1 byte when UTF-8 encoded, since it will be
    // read by Pig which only deals with single byte separators.
    {
      String outputSeparator = jobConf.get("logdriver.output.field.separator",
          DEFAULT_OUTPUT_SEPARATOR);
      byte[] bytes = outputSeparator.getBytes(UTF_8);
      if (bytes.length != 1) {
        LOG.error("The output separator must be a single byte in UTF-8.");
        return 1;
      }

      jobConf.set("logdriver.output.field.separator", Byte.toString(bytes[0]));
    }

    jobConf.set("logdriver.search.string", Base64.encodeBase64String(searchString.getBytes("UTF-8")));

    job.setInputFormatClass(AvroBlockInputFormat.class);
    job.setMapperClass(SearchMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);

    // And set the output as usual
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);
    for (Path path : paths) {
      AvroBlockInputFormat.addInputPath(job, path);
    }

    // Run the job.
    if (conf.getBoolean("job.wait", DEFAULT_WAIT_JOB)) {
      return job.waitForCompletion(true) ? 0 : 1;
    } else {
      job.submit();
      return 0;
    }
  }

  public static void main(String[] args) throws Exception {
    // Let ToolRunner handle generic command-line options
    int res = ToolRunner.run(new Configuration(), new FastSearch(), args);
    System.exit(res);
  }
}
