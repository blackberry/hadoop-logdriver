If you've read the documentation, you're aware that the logs are all stored in a non-standard format, with various concurrency concerns that can break your jobs if you're not careful.  So how do you actually use the logs?

# Pre-canned tools

In gathering our requirements, the most common requirement by far was the ability to get all logs that contain some given string.  So there are a number of tools available to do just that.

In our deployment there is a <code>logdriver/bin/</code> directory that contains the following tools:

```none
logcat         - turns data from Boom files into raw text.
logsearch      - searches for log lines that match a literal string.
logmultisearch - search for lines which contain any of a list of literal strings.
loggrep        - search for lines which match a regular expression.
```

Each of these tools will return a formatted, sorted, set of logs in plain text format.

### Common Options

All of these tools use a common set of options, described here.

> Note: All of these options support glob style matching.  This includes '?' to match one character, '*' to match any set of characters, and more.  More details are available in [the documentation.](http://hadoop.apache.org/common/docs/r0.20.2/api/org/apache/hadoop/fs/FileSystem.html#globStatus(org.apache.hadoop.fs.Path\))

<code>DC</code> is the data centre top level code that allows you to delinate different geographical sources.

<code>SERVICE</code> is the name of the service to be searched. For example <code>web</code> or <code>web/pod1</code> or whatever.

<code>COMPONENT</code> is a the name of the component of the service that you're interested in.  Some examples are <code>app\*</code>, <code>{webhead\*,app\*}</code>, and so on.

<code>START</code> and <code>END</code> are the start (inclusive) and end (exclusive) of the time range you want logs for.  This can either be specified as a number representing milliseconds since 1970, or as a date string.  The date strings are parsed by the <code>date</code> command, so there is a lot of flexibility there.  Some examples of valid times are <code>1339684481000</code>, <code>2012-02-28 12:00:00</code>, <code>1 hour ago</code>, <code>1pm last tuesday</code>, <code>1pm last tuesday EST</code>.  Don't forget to put multi-word dates in quotes so they're not treated as a list of arguments.

<code>OUTPUT_DIR</code> is where, in HDFS, the output of the command will go.  Relative paths are relative to your home directory.  You can also redirect to standard out by using a <code>-</code> as the target.

<code>-dateFormat</code> specifies what format dates should be written in.  The options are <code>RFC822</code>, <code>RFC3164</code>, <code>RFC5424</code> (the default), or any valid format string for [FastDateFormat](http://commons.apache.org/lang/api-2.5/org/apache/commons/lang/time/FastDateFormat.html).  Here are some examples of what the formats look like:

```
RFC822  2012-06-06T12:34:56.789+0000
RFC3164 Jun 06 12:34:56 [The RFC calls for space padded 'day', but our apps use zero padded.]
RFC5424 2012-06-06T12:34:56.789+00:00
```

### Reading the results

All of these tools write their results to a number of files in the directory in HDFS.  In order to see the data in your console, you can use the command

```
hdfs dfs -cat OUTPUT_DIR/part-*
```

If you'd like to copy the output to your local disk, use

```
hdfs dfs -get OUTPUT_DIR/part-* LOCAL_DIR
```

### logcat 

```
Example:
logcat -v dc1 web applogs '30 minutes ago' 'now' -
logcat -v dc1 web applogs '30 minutes ago' 'now' - | grep ERROR | wc -l
logcat -v dc1 web applogs '2013-01-16 12:00:00' '2013-01-17 12:00:00' -

Usage: logcat [OPTIONS] DC SERVICE COMPONENT START END OUTPUT_DIR
Note:
  If OUTPUT_DIR is '-', then results are written to stdout.
Options:
  -v                  Verbose output.
  -r                  Force remote sort.
  -l                  Force local sort.
  -dateFormat=FORMAT  Valid formats are RFC822, RFC3164 (zero padded day),
                      RFC5424 (default), or any valid format string for FastDateFormat.
  -fieldSeparator=X   The separator to use to separate fields in intermediate
                      files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F).
```

### logsearch

```
Example:
logsearch ERROR dc1 web/pod1 '*' '1 hour ago' 'now' demo
logsearch WARN dc1 web/pod1 '*' '16 hour ago' 'now' demo2
logsearch -i AUTH dc1 web/pod1 applog '2013-02-27 00:00:00' '2013-02-27 01:00:00' test-search

Usage: logsearch [OPTIONS] STRING DC SERVICE COMPONENT START END OUTPUT_DIR
Options:
  -v                  Verbose output.
  -i                  Make search case insensitive.
  -r                  Force remote sort.
  -l                  Force local sort.
  -dateFormat=FORMAT  Valid formats are RFC822, RFC3164 (zero padded day),
                      RFC5424 (default), or any valid format string for FastDateFormat.
  -fieldSeparator=X   The separator to use to separate fields in intermediate
                      files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F).
```

<code>STRING</code> is the string to search for in the line.  Lines which contain this string anywhere (excluding the timestamp at the start of the line) will be returned.

### logmultisearch

```
Example:
logmultisearch -v 'ERROR|WARN' dc1 web/pod1 applog '30 minutes ago' 'now' -

Usage: logmultisearch [OPTIONS] (STRINGS_DIR|STRINGS_FILE|STRING)
                          DC SERVICE COMPONENT START END OUTPUT_DIR
Note:
  If OUTPUT_DIR is '-', then results are written to stdout.
Options:
  -v                  Verbose output.
  -i                  Make search case insensitive.
  -r                  Force remote sort.
  -l                  Force local sort.
  -a                  Enable AND searching.
  -dateFormat=FORMAT  Valid formats are RFC822, RFC3164 (zero padded day),
                      RFC5424 (default), or any valid format string for FastDateFormat.
  -fieldSeparator=X   The separator to use to separate fields in intermediate
                      files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F).
```

logmultisearch takes the strings to search for in one of three different formats.  If the given option is the name of a directory on the local filesystem, then all files in that directory are assumed to contain search strings, one per line.  If the option is the name of a file on the local filesystem, then that file is assumed to contain all of the search strings, one per line.  Otherwise, that argument is assumed to be the only search string.  Any line matching one or more of the given search strings will be returned.

### loggrep

```
Example:
loggrep -v 'URL*' dc1 web/pod1 applog '30 minutes ago' 'now' -

Usage: loggrep [OPTIONS] REGEX DC SERVICE COMPONENT START END OUTPUT_DIR
Note:
  If OUTPUT_DIR is '-', then results are written to stdout.
Options:
  -v                  Verbose output.
  -i                  Make search case insensitive.
  -r                  Force remote sort.
  -l                  Force local sort.
  -dateFormat=FORMAT  Valid formats are RFC822, RFC3164 (zero padded day),
                      RFC5424 (default), or any valid format string for FastDateFormat.
  -fieldSeparator=X   The separator to use to separate fields in intermediate
                      files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F).
```

<code>REGEX</code> is a Java style regular expression.  Any log line that matches the regular expression will be returned.  Note that the timestamp is not included in the line when checking for a match.  For more information on Java regular expressions, see the [JavaDoc for Pattern](http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html).

# Pre-canned mapreduce jobs

Sometimes you want more than just a dump of the logs.  So let's look at how the tools work.

Each of the provided tools does the same thing

- Call a mapreduce job, which writes logs in an intermediate format to a temp directory.
- Call a Pig job to format the timestamps, sort the log lines and output to the final directory.

If you want to do something other than format the dates and sort the output, then you can follow this same pattern and substitute your own Pig script.

The tools listed above are all Perl scripts, and you can use them as a start for your own tools.

# Custom mapreduce jobs

Occasionally, running one of the pre-canned jobs will not be appropriate.  This is generally the case when the amount of data output from the mapreduce job is large (more than a few GB), and the job is run repeatedly.  In this case, you may need to write your own mapreduce job.

```
FileManager fm = new FileManager(conf);
List<PathInfo> paths = fm.getPathInfo(dcNumber, service, component, startTime, endTime);

try {
  // Lock, then get the real paths
  fm.acquireReadLocks(paths);
  List<String> inputPaths = fm.getInputPaths(pi);
 
  // Run your job against the files listed in inputPaths.

} finally {
  fm.releaseReadLocks(paths);
}
```

Important: In order to avoid deadlocks, you must collect all of your paths, and call acquireReadLocks() only one time.  It will ensure that the paths are processed in a consistent order.  Failure to do so may result in dead locks which impact the entire cluster.
