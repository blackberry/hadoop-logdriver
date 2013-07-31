The HFind tool is a very small subset of the unix find tool, that works on HDFS.

## Usage

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
