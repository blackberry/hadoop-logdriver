# An overview of the logmaintenance system

## Overview

In LogDriver, there is a tool for automating log maintenance.  This is an overview of the use of that tool.

## Installing

Everything required is included in the LogDriver package.

Before installing, ensure the Oozie workflows are installed.  Authenticated as <code>hdfs</code>, run

```
logdriver deploy-oozie-workflows
```

Next choose only one host to run the logmaintenance tool.  There will be errors reported if it runs on multiple hosts.

To install it, as the local root user (or using sudo) run

```
logdriver setup-logmaintenance
```

This will:

- Create the logmaintenance local group.
- Create the logmaintenance local user.
- Create the /var/log/logmaintenance/ directory, and change its ownership.
- Prompt to add an entry to logmaintenance's crontab.

At the end of that, if you added the crontab entry, it will be set up to run every 15 minutes.

## Configuring

Configuration is in <code>/etc/logdriver/conf/logmaintenance/logmaintenance-env.sh</code> and <code>/etc/logdriver/conf/logmaintenance/logmaintenance.conf]</code>.

Please see the comments in the example configuration files for details.

## When It Runs

First, it uses flock to create a lock, to ensure that only one instance can run at a time.

It will process each line in the configuration file.  The number of lines processed concurrently is configurable.

A process is run for each configuration line.  First that process will create a lock for the configured username - Oozie does not like multiple simultaneous connections with the same credentials.  That process will then open a temporary directory in /var/log/logmaintenance.  Next it will get a kerberos ticket, then run the LogMaintenance Java program.

All output for that process is piped to a temp file, and when it finishes that file is checked for errors.  If errors are found, they can be emailed out to a configured address.

When all configuration lines have been processed, it exits.

