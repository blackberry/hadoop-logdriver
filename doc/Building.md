# Build and Installation
## Current Requirements
* Maven build environment
* CDH4+ is the only tested environment at this time.  Should work with other Hadoop distributions with low effort.
* Oozie and Zookeeper
* MapReduce v1 - YARN is not yet supported.
* SLES10, SLES11, RHEL5.6+ and Ubuntu 12.04 are the currently tested platforms, though other modern Linux distributions should work.
* Perl is required for access tools.

After installing Maven, simply run:

* <code>mvn clean package install rpm:rpm</code>

This will build RPMs and Deb packages for installation.

## Supported Operating Systems

* Red Hat Enterprise Linux (RHEL) 5.6
* SUSE Linux Enterprise Server (SLES) 11
* SUSE Linux Enterprise Server (SLES) 10
* Ubuntu 12.04 (Should work with Debian)

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
