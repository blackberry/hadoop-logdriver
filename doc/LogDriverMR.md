## Custom mapreduce jobs

Occasionally, running one of the pre-canned jobs described in the [Log Search Tools](UsingLogs.md) will not be appropriate.  This is generally the case when the amount of data output from the mapreduce job is large (more than a few GB), and the job is run repeatedly.  In this case, you may need to write your own mapreduce job.

If you're going to do this, please make sure you're very familiar with the detailed design documents:
* [Data Mode](DataModel.md)
* [Boom Files](BoomFiles.md)
* [Concurrency and Locking](Concurrency.md)

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

## Library Locations
In LogDriver, there is a library that can be included in 'hadoop jar' commands or Pig scripts in order to make the LogDriver tools available.  It is installed by default at
```
/usr/lib/logdriver/jar-with-dependencies/logdriver-with-dependencies.jar
```
