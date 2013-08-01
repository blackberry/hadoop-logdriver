## Overview

LogDriver suite of tools and applications for logging management on Hadoop.  It is primary built and maintained at BlackBerry, but we welcome contributions in any form from the larger community.

## Major Features
* Sawmill:  A low profile, high throughput syslog adapter designed as a drop-in replacement for Flume for delivery of syslog content to HDFS.
* Merge Job:  An Oozie workflow which takes content delivered by Sawmill and performs a lightweight aggregation of the content into large files.
* Filter Job:  An Oozie workflow which will apply data lifecycle rules, filtering and deleting data according to configuration.
* Data Access Tools:  A set of tools designed to be executed from the command line, replacing common log management functions such as cat and grep.
* Data Reporting Tools:  A set of tools designed to collect statistical metadata of various log content.
* Strong Security Model:  Full support for Kerberos+LDAP based authentication and strict security models.

For further information, please see our [Documentation](doc/README.md)
