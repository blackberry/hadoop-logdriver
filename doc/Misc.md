# Miscellaneous Tools

## <code>/usr/bin/logdriver</code>

This command is intended to provide general administrative tools required by the logdriver suite.  Currently it supports these commands.

### <code>logdriver version</code>
This will print out the version of logdriver that is installed, and the date it was compiled on.

```
[root]# logdriver version
logdriver version 2.0.0
Built at 2013-03-18T14:29:30-0400
```

### <code>logdriver classpath</code>
This will print out a java classpath that contains everything needed to run any logdriver tool.  This can be used if, for example, you would rather use a <code>java</code> command to run a tool than the <code>hadoop jar</code> command.

For example, these two command lines will both run the logmaintenance tool.

```
hadoop jar /usr/lib/logdriver/jar-with-dependencies/logdriver-with-dependencies.jar com.rim.logdriver.admin.LogMaintenance
```
```
java -cp `logdriver classpath` com.rim.logdriver.admin.LogMaintenance
```

But the second version may allow you to use other Java command line switches more easily.

### <code>logdriver deploy-oozie-workflows</code>
### <code>logdriver setup-logmaintenance</code>
* See:  [Configuration - Log Maintenance](LogMaintenance.md) for information on these commands.


## <code>/usr/bin/hfind</code>
A very small subset of the unix find tool, that works on HDFS.
### Usage

```none
/usr/bin/hfind [path]... [test]... [action]...
```

hfind will do a depth first search of each path, and apply, to each file and directory, all tests (in order) followed by all actions (in order), stopping when any test or action returns false.

Numeric arguments can be specified as
- +n
  - for greater than n,
- -n
  - for less than n,
- n
  - for exactly n.

Tests
=====
- -amin **n**
  - File was last accessed n minutes ago.
- -atime **n**
  - File  was  last  accessed  n*24 hours ago.  When find figures out how many 24-hour periods ago the file was last accessed, any fractional part is ignored, so to match -atime +1, a file has to have been accessed at least two days ago.
- -mmin **n**
  - File’s data was last modified n minutes ago.
- -mtime **n**
  - File’s data was last modified n*24 hours ago.  See the comments for -atime to understand how rounding affects the interpretation of file modification times.
- -regex **pattern**
  - File  name matches regular expression pattern.  This is a match on the whole path, not a search.  For example, to match a file named ‘./fubar3’, you can use the regular expression ‘.*bar.’ or ‘.*b.*3’, but not ‘f.*r3’.  The regular expressions understood by find are by default Java Regular Expressions.

Actions
=======
- -delete
  - Delete files; true if removal succeeded.  If the removal failed, an error message is issued.
- -print
  - True; print the full file name on the standard output, followed by a newline.
```

## <code>/usr/bin/readboom</code>
The readboom command allows you to read raw boom files.

## Usage
```none
/usr/bin/readboom [BOOMFILE]...
```

Each file is read in, and the following is reported for each log line in the file (all tab separated): timestamp, message, eventId, createTime, blockNumber, lineNumber.

If no file is specified, or '-' is specified, then the input is read from stdin.  This is useful for reading from HDFS, for example <code>hdfs dfs -cat file.bm | readboom</code>


