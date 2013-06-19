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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.StringUtils;
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

public class MultiSearch extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(MultiSearch.class);

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  // The expected size of the data blocks, after decompressing. Use what we're
  // using for avro sync interval, plus a bit for overrun.
  private static final int BUFFER_SIZE = Math.round(1.2f * 2 * 1024 * 1024);

  private static final String DEFAULT_OUTPUT_SEPARATOR = "\t";
  private static final boolean DEFAULT_WAIT_JOB = true;
  private static boolean ANDsearch = false;
  private static boolean ORsearch = true;
  private static boolean caseSensitive = false;
  private static boolean caseInsensitive = true;
  private static boolean unicode = false;

  protected static final class ByteTree {
    protected boolean endNode = false;
    protected Map<Byte, ByteTree> children = null;
    protected ByteTree parent = null;
    protected int totalBranches = 0;
    protected int activeBranches = 0;
    protected boolean dead = false;

    protected ByteTree() {
    }

    protected void add(byte[] bytes) {
      add(bytes, 0);
    }

    protected void add(byte[] bytes, int i) {
      if (endNode) {
        return;
      }

      if (i >= bytes.length) {
        endNode = true;
        children = null;
        return;
      }

      if (children == null) {
        children = new HashMap<Byte, ByteTree>();
      }

      ByteTree child = null;
      
      // For case sensitive searching, add a single branch for the current byte. 
      // For case insensitive searching, create a new branch, and point both upper
      // and lower case bytes to the same branch.
      
      try {
        if (caseSensitive) {
          child = children.get(bytes[i]);
        } 
        else {
          child = children.get(bytes.toString().toLowerCase().getBytes("UTF-8")[i]);
          if (child == null) {
            child = children.get(bytes.toString().toUpperCase().getBytes("UTF-8")[i]);
          } 
        }
      
        if (child == null) {
          child = new ByteTree();
          if (caseSensitive) {
            children.put(bytes[i], child);
          }
          else {
            children.put(new String(bytes, "UTF-8").toLowerCase().getBytes("UTF-8")[i], child);
            children.put(new String(bytes, "UTF-8").toUpperCase().getBytes("UTF-8")[i], child);
          }
          totalBranches++;
        }
      } catch (IOException e) {
          throw new RuntimeException(e);
      }
      
      child.parent = this;
      child.add(bytes, i + 1);
    }

    /**
     * 
     * @param a
     *          The character array.
     * @param b
     *          The starting index.
     * @return
     */
    
    // If this is the end of a branch, we've matched the branch. For OR searching
    // return true. For AND searching return true, and remove the matched branch.
    
    protected boolean matches(byte[] a, int b) {
      if (children == null || endNode) {
        if (ANDsearch) {
          dead = true;
          parent.removeBranch();
        }
        return true;
      }

      if (b >= a.length) {
        return false;
      }

      ByteTree child = children.get(a[b]);
      if (child == null || child.dead) {
        return false;
      }

      return child.matches(a, b + 1);
    }
    
    // To remove a branch, reduce the activeBranches counter by one, and mark
    // the branch dead once all active branches have been removed. If the parent
    // exists, remove it as well. 
    
    protected void removeBranch() {
      activeBranches--;
      if (activeBranches == 0) {
        dead = true;
        if (parent != null) {
    	  parent.removeBranch();
        }
      }
    }
    
    // To reset the tree, set dead to false, set the number of active branches
    // equal to the total number of branches, and then reset each branch. 
    
    protected void reset() {
      dead = false;
      activeBranches = totalBranches;
      if (children != null) {
        Set<Byte> branchKeys = children.keySet();
        for(Iterator<Byte> branchCount = branchKeys.iterator(); branchCount.hasNext(); ) {
          children.get(branchCount.next()).reset();
        }
      }
    }

    public String toString() {
      return toString(0);
    }

    private String toString(int i) {
      if (children == null) {
        return "";
      }

      String s = "";
      for (byte c : children.keySet()) {
    	for (int j = 0; j < i; j++) {
          s += " ";
        }
        try {
          s += new String(new byte[] { c }, "UTF-8");
        } catch (UnsupportedEncodingException e) {
          LOG.error("", e);
        }
        s += "\n";
        s += children.get(c).toString(i + 1);
      }
      return s;
    }
  }

  private static final class SearchMapper extends
      Mapper<AvroFileHeader, BytesWritable, Text, NullWritable> {
    private long start;
    private long end;
    private Inflater inflater;
    private ByteTree byteTree = new ByteTree();
    private List<String> searchTerms = new ArrayList<String>(); 
    private String outputSeparator;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();

      start = conf.getLong("logdriver.search.start.time", Long.MIN_VALUE);
      end = conf.getLong("logdriver.search.end.time", Long.MAX_VALUE);
      ANDsearch = conf.getBoolean("logdriver.search.and", false);
      ORsearch = !ANDsearch;
      caseInsensitive = conf.getBoolean("logdriver.search.case.insensitive", false);
      caseSensitive = !caseInsensitive;

      String searchStringDir = conf.get("logdriver.search.string.dir");
      FileSystem fs;
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      BufferedReader searchStringReader = null;
      try {
        for (FileStatus f : fs.listStatus(new Path(searchStringDir))) {
          searchStringReader = new BufferedReader(new InputStreamReader(
              fs.open(f.getPath()), "UTF-8"));

          String line;
          while ((line = searchStringReader.readLine()) != null) {
            line = line.trim();
            // If the current line contains more bytes than characters in either upper
            // or lower case, then set unicode to true. 
            if (line.toLowerCase().getBytes("UTF-8").length > line.length() 
                || line.toUpperCase().getBytes("UTF-8").length > line.length()) {
              unicode = true;
            }
            if (!"".equals(line)) {
              // If unicode characters have been detected, and we're doing a case insensitive
              // search, then there's no point in continuing to build the byte tree.
              if (!unicode || caseSensitive) {
                byteTree.add(line.getBytes("UTF-8"));
              }
              searchTerms.add(line);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      if (searchTerms.size() == 0) {
        throw new RuntimeException("No search strings read.");
      }

      inflater = new Inflater(true);

      outputSeparator = new String(new byte[] { Byte.parseByte(conf
          .get("logdriver.output.field.separator")) }, UTF_8);

      LOG.info("Configuring SearchMapper");
      LOG.info("  start={}", start);
      LOG.info("  end={}", end);
      if (!unicode || caseSensitive) {
        LOG.info("  tree=\n{}", byteTree);
      }
      else {
        LOG.info("Unicode case insensitive search detected, not using byte tree.");
      }
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
      // somewhere. Reset the tree before searching. If the input lines
      // contained multibyte characters, skip bytescanning.
      
      boolean match = false;
      byteTree.reset();
      
      if (!unicode || caseSensitive) {
        if (ORsearch) {
    	  for (int i = 0; i < buf.length; i++) {
            if (byteTree.matches(buf, i)) {
              LOG.info("OR match in byte block");
              match = true;
              break;
            }
          }
        }
        else if (ANDsearch) {
          for (int i = 0; i < buf.length; i++) {
            if (byteTree.matches(buf, i) && byteTree.dead) {
              LOG.info("AND match in byte block");
              match = true;
              break;
            }
          }
        }
      }
      else {
    	// It's not worth bytescanning for multi-byte UTF-8, so skip it and deserialize the block.
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
        long blockNo;
        long createTime;
        long second = 0;
        long lineNumber = 0;
        long ms = 0;
        boolean matchline = false;
        byte[] bytes;
        for (int i = 0; i < entries; i++) {
          record = datumReader.read(record, decoder);
          LOG.trace("Read record {}", record);

          second = (Long) record.get("second") * 1000;
          if (second < start || second >= end) {
            LOG.debug("Time out of range: {} < {} || {} >= {}", new Object[] {
                second, start, second, end });
            continue;
          }

          blockNo = (Long) record.get("blockNumber");
          createTime = (Long) record.get("createTime");
          lineNumber = 0l;

          for (Record line : (List<Record>) record.get("logLines")) {
            String message = line.get("message").toString();
            ++lineNumber;
            matchline = false;
            
            // If we're not searching for multi-byte characters, use byte scanning to determine
            // if there is a match in a given message line. FOR loops are contained within checks
            // for OR and AND searching for increased performance.
            
            // Check if each line matches. For an AND search the current branch must match
            // and the entire tree must be dead for the line to match.
            
            if (!unicode || caseSensitive) {
              bytes = message.getBytes("UTF-8");
              byteTree.reset();
              if (ORsearch) {
                for (int j = 0; j < bytes.length; j++) {
                  if (byteTree.matches(bytes, j)) {
                    matchline = true;
                    break;
                  }
                }
              }
              else if (ANDsearch) {
            	for (int j = 0; j < bytes.length; j++) {
            	  if (byteTree.matches(bytes, j) && byteTree.dead) {
                    matchline = true;
                    break;
            	  }
                }
              }
            }
            
            // If we are searching for multi-byte characters, it is faster to do the comparison
            // using strings. 
            
            else {
              String currentTerm = null;
              if (ORsearch) {
                for(Iterator<String> termCounter = searchTerms.iterator(); termCounter.hasNext(); ) {
                  currentTerm = termCounter.next();
              	  if (message.toLowerCase().contains(currentTerm.toLowerCase()) 
              	      || message.toUpperCase().contains(currentTerm.toUpperCase())) {
                    matchline = true;
                    break;
              	  }
                }
              }
              else if (ANDsearch) {
            	matchline = true;
            	for(Iterator<String> termCounter = searchTerms.iterator(); termCounter.hasNext(); ) {
            	  currentTerm = termCounter.next();
                  if (!message.toLowerCase().contains(currentTerm.toLowerCase()) 
            	      && !message.toUpperCase().contains(currentTerm.toUpperCase())) {
            	    matchline = false;
            	    break;
            	  }
            	}
              }
            }
            
            if (matchline) {
              LOG.info("Got match!");
              ms = (Long) line.get("ms");

              StringBuilder sb = new StringBuilder().append((second + ms))
                  .append(outputSeparator).append(StringUtils.chomp(message))
                  .append(outputSeparator).append(line.get("eventId"))
                  .append(outputSeparator).append(createTime)
                  .append(outputSeparator).append(blockNo)
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
    String searchStringDir = null;
    List<Path> paths = new ArrayList<Path>();
    Path outputDir = null;

    // Load input files from the command line
    if (args.length < 3) {
      System.out
          .println("usage: [genericOptions] searchStringDirectory input [input ...] output");
      System.exit(1);
    }

    // Get the files we need from the command line.
    searchStringDir = args[0];
    // We are going to be reading all the files in this directory a lot. So
    // let's up the replication factor by a lot so that they're easy to read.
    for (FileStatus f : fs.listStatus(new Path(searchStringDir))) {
      fs.setReplication(f.getPath(), (short) 16);
    }

    for (int i = 1; i < args.length - 1; i++) {
      for (FileStatus f : fs.globStatus(new Path(args[i]))) {
        paths.add(f.getPath());
      }
    }

    outputDir = new Path(args[args.length - 1]);

    Job job = new Job(conf);
    Configuration jobConf = job.getConfiguration();

    job.setJarByClass(MultiSearch.class);
    jobConf.setIfUnset("mapred.job.name", "MultiSearch");

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

    jobConf.set("logdriver.search.string.dir", searchStringDir);

    // This search is generally too fast to make good use of 128MB blocks, so
    // let's set the value to 256MB (if it's not set already)
    if (jobConf.get("mapred.max.split.size") == null) {
      jobConf.setLong("mapred.max.split.size", 256 * 1024 * 1024);
    }

    job.setInputFormatClass(AvroBlockInputFormat.class);
    job.setMapperClass(SearchMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);

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
    int res = ToolRunner.run(new Configuration(), new MultiSearch(), args);
    System.exit(res);
  }
}
