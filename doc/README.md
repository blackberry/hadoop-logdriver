# Overview

LogDriver suite of tools and applications for logging management on Hadoop.  It is primary built and maintained at BlackBerry, but we welcome contributions in any form from the larger community.

# Major Features
* Sawmill:  A low profile, high throughput syslog adapter designed as a drop-in replacement for Flume for delivery of syslog content to HDFS.
* Merge Job:  An Oozie workflow which takes content delivered by Sawmill and performs a lightweight aggregation of the content into large files.
* Filter Job:  An Oozie workflow which will apply data lifecycle rules, filtering and deleting data according to configuration.
* Data Access Tools:  A set of tools designed to be executed from the command line, replacing common log management functions such as cat and grep.
* Data Reporting Tools:  A set of tools designed to collect statistical metadata of various log content.
* Strong Security Model:  Full support for Kerberos+LDAP based authentication and strict security models.

# Current Requirements
* Maven build environment
* CDH4+ is the only tested environment at this time.  Should work with other Hadoop distributions with low effort.
* Oozie and Zookeeper
* MapReduce v1 - YARN is not yet supported.
* SLES10, SLES11, RHEL5.6+ and Ubuntu 12.04 are the currently tested platforms, though other modern Linux distributions should work.
* Perl is required for access tools.

# Build and Installation
After installing Maven, simply run:

* <code>mvn clean package install rpm:rpm</code>

This will build RPMs and Deb packages for installation.

## Supported Operating Systems

* Red Hat Enterprise Linux (RHEL) 5.6
* SUSE Linux Enterprise Server (SLES) 11
* SUSE Linux Enterprise Server (SLES) 10
* Ubuntu 12.04 (Should work with Debian)

## Installation
Install the appropriate RPM or Deb file for your operating system and Hadoop version.

### What gets installed
The following directories are created as part of the install process

* <code>/usr/lib/logdriver</code>
* <code>/etc/logdriver/conf.example</code>
* <code>/var/log/sawmill</code>
* <code>/var/run/sawmill</code>

The following files are created as part of the install process

* <code>/etc/init.d/sawmill</code>
* <code>/usr/bin/hfind</code>
* <code>/usr/bin/logcat</code>
* <code>/usr/bin/loggrep</code>
* <code>/usr/bin/logmultisearch</code>
* <code>/usr/bin/logsearch</code>
* <code>/usr/bin/readboom</code>
* <code>/usr/bin/logdriver</code>
* <code>/usr/bin/logmaintenance</code>
* <code>/usr/bin/indexlogs</code>
* <code>/usr/bin/queryindex</code>

In addition the groups<code>sawmill</code> and the user <code>sawmill</code> are created if they do not exist.

# Configuration
All configuration is stored in
```
/etc/logdriver/conf/
```

LogDriver contains example configuration files that will be placed in
```
/etc/logdriver/conf.example/
```

## LogDriver configuration
General LogDriver configuration is in
```
/etc/logdriver/conf/logdriver-env.sh
```

## Sawmill configuration
The location of the sawmill configuration directory is specified in <code>logdriver-env.sh</code>.  The contents of the directory are

* <code>sawmill-env.sh</code>: Java options and environment configuration.
* <code>sawmill.conf</code>: The Sawmill configuration file, defining syslog listeners, queues and HDFS targets.
* <code>log4j.properties</code>: This logging configuration logs to <code>/var/log/sawmill/</code>.

This is also an appropriate directory to place keytab files for use by Sawmill.  However, if this is a shared directory (such as mounted over nfs), then that may be a security risk.  In that case, you should create a local directory such as <code>/etc/logdriver/conf.local/sawmill/</code> and store the keytab files there.

## Oozie workflow configuration
There are two Oozie workflows that are part of LogDriver.  The Merge Job and the Filter Job.  In LogDriver version 1, these had different names but were still referred to as the merge job and the filter job.  So, in order to better align with what we actually call them, in LogDriver version 2 they have been renamed <code>mergejob</code> and <code>filterjob</code>.

The configurations for these jobs were previously distributed within <code>/usr/lib/logdriver/</code>, but have been moved to <code>/etc/logdriver/conf/</code>.

In addition, the way the workflows are deployed to HDFS has changed.  See the section on the <code>logdriver</code> command below.

### Merge Job Configuration
The merge job configuration is, by default, in <code>/etc/logdriver/conf/mergejob/mergejob.properties</code>.

### Filter Job Configuration
The filter job configuration is, by default, in <code>/etc/logdriver/conf/filterjob/filterjob.properties</code>.

The filters for this job are placed in <code>/etc/logdriver/conf/filterjob/</code>.  The <code>logdriver</code> command can be used to deploy the configurations to HDFS from there.

### Log Maintenance Job Configuration
The log maintenance job configuration is in the directory<code>/etc/logdriver/conf/logmaintenance/</code>.

The files in that directory are

* <code>logmaintenance-env.sh</code> : environmental variables to control to execution of the logmaintenance tool.
* <code>logmaintenance.conf</code> : a list of which services to perform maintenance on.

In addition, there may be an entry in the crontab to for the <code>logmaintenance</code> user.  See the <code>logdriver</code> command below.

# Library Locations
In LogDriver, there is a library that can be included in 'hadoop jar' commands or Pig scripts in order to make the LogDriver tools available.  It is installed by default at
```
/usr/lib/logdriver/jar-with-dependencies/logdriver-with-dependencies.jar
```

# Additional Tools and Functionality

## The log maintenance tool
There is a log maintenance tool that simplifies the execution of log maintenance commands.

The advantages over the old system of putting each command in the crontab are

* All configuration is stored in /etc instead of in crontabs.
* It ensures that only one copy is ever running on any single host, preventing cascading failures.
* It maintains and rotates logs on each run.
* It post-processes all logs to check for errors, and can email out logs if there is an error.

To run it, configure the files in <code>/etc/logdriver/conf/logmaintenance/</code>, and use the <code>logdriver setup-logmaintenance</code> command.

## The logdriver command
A new command is added in LogDriver version 2.0: <code>/usr/bin/logdriver</code>.

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
This command will allow you to deploy the LogDriver Oozie workflows to HDFS.

When run, it will ask you to confirm that you want to deploy each of the mergejob, the filterjob, and the filters for the filterjob.  Any, all or none of these may be done when the command is run.  But note that deploying the filterjob will delete any existing configuration from HDFS, so redeploying the filters afterwards is recommended.

Note that this command will probably only work correctly if you are authenticated as an HDFS superuser such as hdfs.

### <code>logdriver setup-logmaintenance</code>
This command is used to set up a host to run the log maintenance tool.  Generally, that is only one host per cluster.

It performs the following steps:

* Create the logmaintenance group and user
* Create the /var/log/logmaintenance directory, and change it's ownership to logmaintenance:logmaintenance
* Prompt to add a log maintenance entry to logmaintenance's crontab.
