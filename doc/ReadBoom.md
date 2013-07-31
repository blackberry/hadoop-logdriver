The readboom command allows you to read raw boom files.

## Usage
```none
/usr/bin/readboom [BOOMFILE]...
```

Each file is read in, and the following is reported for each log line in the file (all tab separated): timestamp, message, eventId, createTime, blockNumber, lineNumber.

If no file is specified, or '-' is specified, then the input is read from stdin.  This is useful for reading from HDFS, for example <code>hdfs dfs -cat file.bm | readboom</code>


