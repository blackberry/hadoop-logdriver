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
 * Cat Logs in a given file set.
 * <p>
 * Usage: [genericOptions] [-Dlogdriver.search.start.time=X] [-Dlogdriver.search.end.time=X] input [input ...] output
 * <p>
 * 
 */
package com.rim.logdriver.util;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rim.logdriver.boom.LogLineData;
import com.rim.logdriver.mapreduce.boom.BoomInputFormat;

public class Cat extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(Cat.class);

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private static final String DEFAULT_OUTPUT_SEPARATOR = "\t";
  private static final boolean DEFAULT_WAIT_JOB = true;

  private static final class CatMapper extends
      Mapper<LogLineData, Text, Text, NullWritable> {
    private long start;
    private long end;
    private String outputSeparator;

    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();

      start = conf.getLong("logdriver.search.start.time", Long.MIN_VALUE);
      end = conf.getLong("logdriver.search.end.time", Long.MAX_VALUE);

      outputSeparator = new String(new byte[] { Byte.parseByte(conf
          .get("logdriver.output.field.separator")) }, UTF_8);

      LOG.info("Configuring CatMapper");
      LOG.info("  start={}", start);
      LOG.info("  end={}", end);
    }

    @Override
    protected void map(LogLineData key, Text value, Context context)
        throws IOException, InterruptedException {
      long timestamp = key.getTimestamp();
      if (timestamp >= start && timestamp < end) {
        StringBuilder sb = new StringBuilder().append(key.getTimestamp())
            .append(outputSeparator)
            .append(StringUtils.chomp(value.toString()))
            .append(outputSeparator).append(key.getEventId())
            .append(outputSeparator).append(key.getCreateTime())
            .append(outputSeparator).append(key.getBlockNumber())
            .append(outputSeparator).append(key.getLineNumber());

        context.write(new Text(sb.toString()), null);
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
    List<Path> paths = new ArrayList<Path>();
    Path outputDir = null;

    // Load input files from the command line
    if (args.length < 2) {
      System.out.println("usage: [genericOptions] input [input ...] output");
      System.exit(1);
    }

    // Get the files we need from the command line.
    for (int i = 0; i < args.length - 1; i++) {
      for (FileStatus f : fs.globStatus(new Path(args[i]))) {
        paths.add(f.getPath());
      }
    }
    outputDir = new Path(args[args.length - 1]);

    Job job = new Job(conf);
    Configuration jobConf = job.getConfiguration();

    job.setJarByClass(Cat.class);
    jobConf.setIfUnset("mapred.job.name", "Cat Files");

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

    job.setInputFormatClass(BoomInputFormat.class);
    job.setMapperClass(CatMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outputDir);

    for (Path path : paths) {
      BoomInputFormat.addInputPath(job, path);
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
    int res = ToolRunner.run(new Configuration(), new Cat(), args);
    System.exit(res);
  }
}
