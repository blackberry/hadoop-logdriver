# An overview of the logmaintenance system

In LogDriver, there is a tool for automating log maintenance.  This is an overview of the use of that tool.  This document provides the details on how to Configure and Activate (aka Deploy) these tools on your running cluster.  Once deployed, the content for these jobs is maintained in HDFS.

## Configuration
All configuration is stored in <code>/etc/logdriver/conf/</code>.

LogDriver contains example configuration files that will be placed in <code>/etc/logdriver/conf.example/</code>.  General LogDriver configuration is in <code>/etc/logdriver/conf/logdriver-env.sh</code>.

### Oozie workflow configuration
There are two Oozie workflows that are part of LogDriver.  The Merge Job and the Filter Job.  The configurations for these jobs are stored in <code>/etc/logdriver/conf/</code>.

In addition, the way the workflows are deployed to HDFS has changed.  See the section on the <code>logdriver</code> command below.

The merge job configuration is, by default, in <code>/etc/logdriver/conf/mergejob/mergejob.properties</code>.

The filter job configuration is, by default, in <code>/etc/logdriver/conf/filterjob/filterjob.properties</code>.  The filters for this job are placed in <code>/etc/logdriver/conf/filterjob/</code>.  The <code>logdriver</code> command can be used to deploy the configurations to HDFS from there.

### Log Maintenance Job Configuration
The log maintenance job configuration is in the directory<code>/etc/logdriver/conf/logmaintenance/</code>.

The files in that directory are

* <code>logmaintenance-env.sh</code> : environmental variables to control to execution of the logmaintenance tool.
* <code>logmaintenance.conf</code> : a list of which services to perform maintenance on.

In addition, there may be an entry in the crontab to for the <code>logmaintenance</code> user.  See the <code>logdriver</code> command below.


## Activating

There are two steps to activating the workflows:  Deploying the workflows to HDFS, and setting up crontabs to automatically kick off the workflows on one access/management machine.

### <code>logdriver deploy-oozie-workflows</code>
This command will allow you to deploy the LogDriver Oozie workflows to HDFS.

When run, it will ask you to confirm that you want to deploy each of the mergejob, the filterjob, and the filters for the filterjob.  Any, all or none of these may be done when the command is run.  But note that deploying the filterjob will delete any existing configuration from HDFS, so redeploying the filters afterwards is recommended.

Note that this command will probably only work correctly if you are authenticated as an HDFS superuser such as hdfs.

### <code>logdriver setup-logmaintenance</code>
Next choose only one host to run the logmaintenance tool.  There will be errors reported if it runs on multiple hosts.  This command is used to set up a host to run the log maintenance tool.  Generally, that is only one host per cluster.

It performs the following steps:

* Create the logmaintenance group and user
* Create the /var/log/logmaintenance directory, and change it's ownership to logmaintenance:logmaintenance
* Prompt to add a log maintenance entry to logmaintenance's crontab.

To install it, as the local root user (or using sudo) run

```
logdriver setup-logmaintenance
```

At the end of that, if you added the crontab entry, it will be set up to run every 15 minutes.

## When It Runs

First, it uses flock to create a lock, to ensure that only one instance can run at a time.

It will process each line in the configuration file.  The number of lines processed concurrently is configurable.

A process is run for each configuration line.  First that process will create a lock for the configured username - Oozie does not like multiple simultaneous connections with the same credentials.  That process will then open a temporary directory in /var/log/logmaintenance.  Next it will get a kerberos ticket, then run the LogMaintenance Java program.

All output for that process is piped to a temp file, and when it finishes that file is checked for errors.  If errors are found, they can be emailed out to a configured address.

When all configuration lines have been processed, it exits.

