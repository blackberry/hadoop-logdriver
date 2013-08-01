Before we begin, a quick description of the Boom format is appropriate.

Boom is our custom data storage format for logs.  We put a lot of effort into building a format that allowed efficient storage, search, compression, ordering, etc.  Full details about the boom format can be read on the [Boom Files](BoomFiles.md) document.  The only thing you really need to know here is that the boom format reader will provide a few pieces of information that will only be useful for ordering the output.

## Library Locations
In LogDriver, there is a library that can be included in 'hadoop jar' commands or Pig scripts in order to make the LogDriver tools available.  It is installed by default at
```
/usr/lib/logdriver/jar-with-dependencies/logdriver-with-dependencies.jar
```

## Reading Data In
Reading data into pig from boom-formatted stuff requires using the custom loader provided within the logdriver utilities.  

First, you have to load in the define the LogDriver utilities that contain the tools to read Boom files, mentioned above.

The LoadFunc to load Boom files is <code>com.rim.logdriver.pig.BoomLoadFunc()</code>.  It is useful to create a shorter alias to that.
```
--- Load up the logdriver utilities
REGISTER /usr/lib/logdriver/jar-with-dependencies/logdriver-with-dependencies.jar

--- Create an alias to the load function
DEFINE bmLoad com.rim.logdriver.pig.BoomLoadFunc();
```

Next, we can read in files by specifying the path to file, and that we should use the BoomLoadFunc.

The path for most Boom files processed using this method will be

```none
/service/<DC>/<SVC>/logs/<YYYYMMDD>/<HH>/<COMPONENT>/data/*.bm
```

Wildcards and other types of glob style matching are allowed, so all the logs for all web components that start with 'app' in DC 1, could be written as

```none
/service/dc1/web/logs/20120808/*/app*/data/*.bm
```

There are six fields returned from the BoomLoadFunc.  They are
<table>
  <tr>
    <td>timestamp</td>
    <td>The Java timestamp (ms since Jan 1, 1970) parsed out of the log line.</td>
  </tr>
  <tr>
    <td>message</td>
    <td>The raw log line as a string, excluding the timestamp, and one space immediately following the timestamp.</td>
  </tr>
  <tr>
    <td>eventId</td>
    <td>Reserved for future use.  Currently always 0.</td>
  </tr>
  <tr>
    <td>createTime<br/>blockNumber<br/>lineNumber</td>
    <td>These fields have no actual meaning, but their relative values indicate in what order log lines were received.  They are only used when sorting log lines, to recreate the original log order.</td>
  </tr>
</table>

```
-- Load the files using the BoomLoadFunc we defined.

raw_lines = LOAD '/service/dc1/web/logs/20120808/*/app*/data/*.bm' USING bmLoad AS (
     timestamp: long, 
     message: chararray,
     eventId: long,
     createTime: long, 
     blockNumber: long, 
     lineNumber: long
);
```

So, as an example, if the original line, was

```none
Aug 08 12:34:56 myhost This is a log line.
```

Then you may get a tuple like

```none
(1344429296000,myhost This is a log line,0,1344429294952,17,20)
```

## Formatting Dates
If you want to get the original date string back, then there utilities supplied with LogDriver.  Simply use <code>com.rim.logdriver.pig.DateFormatter()</code> with the date format of your choice to convert it back.

Valid date formats are:
<table>
  <tr>
    <td>Format Name</td>
    <td>Format String</td>
    <td>Example</td>
  </tr>
  <tr>
    <td>RFC822</td>
    <td>yyyy-MM-dd'T'HH:mm:ss.SSSZ</td>
    <td>2012-08-08T12:34:56.000+0000</td>
  </tr>
  <tr>
    <td>RFC3164</td>
    <td>MMM dd HH:mm:ss</td>
    <td>Aug 08 12:34:56<br/>Note: RFC8364 specifies that single digit days be padded with spaces, but this formatter will pad with zeros.  This is not consistent with the RFC - it is this way for backward compatibility with broken apps.</td>
  </tr>
  <tr>
    <td>RFC5424</td>
    <td>yyyy-MM-dd'T'HH:mm:ss.SSSZZ</td>
    <td>2012-08-08T12:34:56.000+00:00</td>
  </tr>
</table>

Continuing our example from above, we can add a definition, then use it format the date.

```
--- define the DateFormatter function
DEFINE DateFormatter com.rim.logdriver.pig.DateFormatter('RFC3164');

line_with_date = FOREACH rawlines GENERATE
    DateFormatter(timestamp) AS formatted_time: chararray,
    timestamp, message, createTime, blockNumber, lineNumber;
```

Following our example line, this would give us a tuple that looks like

```none
(Aug 08 12:34:56,1344429296000,myhost This is a log line,1344429294952,17,20)
```

We can then use Pig's CONCAT function to join the date string and recreate the original logline.

```
with_original_line = FOREACH line_with_date GENERATE
    CONCAT(formatted_time, CONCAT(' ', message)),
    timestamp, createTime, blockNumber, lineNumber;
```

This would give us a tuple that looks like

```none
(Aug 08 12:34:56 myhost This is a log line,1344429296000,1344429294952,17,20)
```

## Sorting log lines
Sometimes you want to get the log lines in the same order they were generated.  This is important for troubleshooting, but not very important for reporting.

To sort, simple use the Pig ORDER command, sorting on the time in the log line, and the three fields we mentioned earlier: createTime, blockNumber and lineNumber.

```none
ordered_lines = ORDER with_original_lines BY timestamp, createTime, blockNumber, lineNumber;
```

We now have the same set of tuples, but ordered.

Note the warning on the ORDER command!  Performing any operation other than STORE or DUMP after an ORDER may result in your data getting out of order again.  So only do this right at the end.

### FirstItemOnlyStoreFunc
So, we now have a sorted list of tuples that look like

```none
(Aug 08 12:34:56 myhost This is a log line,1344429296000,1344429294952,17,20)
```

We only want to store the first element in the tuple (since the rest is fairly useless).  But we can't filter that out, since we would then lose ordering!  This is why the FirstItemOnlyStoreFunc exists.  It will only store the first item in each tuple.

```
DEFINE FirstItemOnlyStoreFunc com.rim.logdriver.pig.FirstItemOnlyStoreFunc();

STORE ordered_lines INTO 'my/output/directory' USING FirstItemOnlyStoreFunc;
```

Hooray!  Now we have all the log lines we wanted, in order.  To get them back in order, simply run:

```none
hdfs dfs -cat my/output/directory/part*
```

## Example 1:  Count the occurrences of a phrase.
Here we will count how many times a given phrase (defined by a regular expression) occur.

```
--- Register the logdriver jar, and define the Boom load function.
REGISTER /usr/lib/logdriver/hadoop-deploy/logdriver-core-hdeploy.jar
DEFINE bmLoad com.rim.logdriver.pig.BoomLoadFunc();

--- Here we use an parameter to specify the day we're looking at.
raw_lines = LOAD '/service/dc1/web/logs/$DATE/*/app*/data/*.bm' USING bmLoad AS (
     timestamp: long, 
     message: chararray,
     eventId: long,
     createTime: long, 
     blockNumber: long, 
     lineNumber: long
);

--- We don't care about any of the fields except the message, so prune early.
messages = FOREACH raw_lines GENERATE message;

--- Filter to only the lines that have the message we want, using a regular expression match.
matching_messages = FILTER messages BY (message MATCHES '$REGEX');

--- Put all the results into one Bag, then get the size of the Bag
message_bag = GROUP matching_messages ALL;
count = FOREACH message_bag GENERATE COUNT(matching_messages);

-- Dump the result to standard out
DUMP count;
```

Usage is something like this:

```none
pig -p DATE=20120808 -p REGEX='.*mystring.*' count.pg
```

And you would get output like

```none
(947053)
```

## Example 2: Getting the log lines.
It's great that we can get the count, but what if we want the actual log lines?
```
--- Register the logdriver jar, and define the functions we need.
REGISTER /usr/lib/logdriver/hadoop-deploy/logdriver-core-hdeploy.jar
DEFINE bmLoad com.rim.logdriver.pig.BoomLoadFunc();
DEFINE DateFormatter com.rim.logdriver.pig.DateFormatter('RFC3164');
DEFINE FirstItemOnlyStoreFunc com.rim.logdriver.pig.FirstItemOnlyStoreFunc();

--- Here we use an parameter to specify the day we're looking at.
raw_lines = LOAD '/service/dc1/web/logs/$DATE/*/app*/data/*.bm' USING bmLoad AS (
     timestamp: long, 
     message: chararray,
     eventId: long,
     createTime: long, 
     blockNumber: long, 
     lineNumber: long
);

--- Filter to only the lines that have the message we want, using a regular expression match.
matching_messages = FILTER raw_lines BY (message MATCHES '$REGEX');

--- Get the formatted date.  By waiting until after we filter, this is called fewer times.
line_with_date = FOREACH matching_messages GENERATE
    DateFormatter(timestamp) AS formatted_time: chararray,
    timestamp, message, createTime, blockNumber, lineNumber;

--- Recreate the original line.
with_original_line = FOREACH line_with_date GENERATE
    CONCAT(formatted_time, CONCAT(' ', message)),
    timestamp, createTime, blockNumber, lineNumber;

--- Sort the lines.
ordered_lines = ORDER with_original_line BY timestamp, createTime, blockNumber, lineNumber;

--- Dump the result to an output directory
STORE ordered_lines INTO '$OUT' USING FirstItemOnlyStoreFunc;
```

The usage here is similar

```
pig -p DATE=20120808 -p REGEX='.*mystring.*' OUT=my/output/dir grep.pg
```
