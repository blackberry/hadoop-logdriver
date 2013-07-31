First, read [[DataModel]].

Now, consider this example.

A job is running to move all of the data for a given hour from /incoming to /data.  Just as it is finished, a user tries to look for this data, by trying all of the possible locations.

```
Running Job                                || User
===========                                || ====
Move data from /incoming to /working/myjob ||
Process data                               ||
                                           || check /archive   - no data found
                                           || check /data      - no data found
                                           || check /incoming  - no data found
Move data from /working/myjob to /data     ||
                                           || check /working/* - no data found
```

There is data, but due to a race condition, the user couldn't find it.  This could lead to corrupt data in jobs, or misleading results in troubleshooting.  Therefore, we must find a way to avoid this.

## Component Locking

To prevent race conditions, we implement distributed advisory locking for datasets.  This means that anyone can claim a read-lock or write-lock on a set of data, ensuring that the data will not change while they are working on it.

Here's what the above example looks like with locking.

```
Running Job                                || User
===========                                || ====
Get Write Lock                             ||
Move data from /incoming to /working/myjob ||
Release Write Lock                         ||
Process data                               ||
                                           || Get Read Lock
                                           || check /archive   - no data found
                                           || check /data      - no data found
                                           || check /incoming  - no data found
Try to get Write lock - failed             ||
                                           || check /working/* - found data
                                           || Process data
                                           || Release Read Lock
Retry get Write Lock - success             ||
Move data from /working/myjob to /data     ||
Release Write Lock                         ||
```

## How our locks work

In order facilitate distributed locking, we are using Zookeeper.

The class <code>com.rim.logdriver.locks.LockUtil</code> provides an implementation of locking based on the standard Zookeeper recipe (http://zookeeper.apache.org/doc/trunk/recipes.html#Shared+Locks).  The locks are based on a combination of  Service Path, Timestamp and Component Name (see definitions of these terms at [[Data Model]])

Locks are done at this granularity because data could be moving between any of the subdirectories within the Service Directory, but the data will never change hour or Component Name.  So this level can ensure consistency when data is being moved.

## Using locks

Generally, the <code>com.rim.logdriver.fs.FileManager</conf> will be used to both get lists of directories, and acquire and release locks on those directories.  This ensures that locks are correctly acquired, and are always acquired in the same order, to avoid deadlocks.

```
// make sure conf includes a setting for zk.connect.string, so that we know where to connect to zookeeper
Configuration conf = ...
FileManager fm = new FileManager(conf);
List<PathInfo> paths = fm.getPathInfo(servicePath, component, startTime, endTime);
fm.acquireReadLocks(paths);
try {
  // Do stuff...
} finally {
  fm.releaseReadLocks(paths);
}
```

## Lock maintenance and troubleshooting

There is a class called <code>com.rim.logdriver.Lock</code> that allows you to view and manage locks.  This is part of the logdriver-core package.

To use it

    java -cp <path/to/logdriver-core-[version].jar:/path/to/logdriver/lib/*>  com.rim.logdriver.Lock -zkConnectString=[zookeeper connection string] command opts

Running it with no command will list the possible commands.

- READ STATUS will report how many read locks are held on a node.
- READ RESET will reset the read locks on a node to 0.  This should never be needed.
- WRITE STATUS will report how many write locks are held on a node.
- WRITE RESET will reset the write locks on a node to 0.  This should never be needed.
- SCAN will search a given node, and all child nodes recursively, reporting the current state of all locks.
- PURGE will delete the given node, and all parent nodes that have no other children.  This can be used to remove old lock nodes that are no longer needed.


