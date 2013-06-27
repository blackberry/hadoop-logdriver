LogDriver
=========

LogDriver is a toolset built at BlackBerry to load and manage logging content on Apache Hadoop for services without any
knowledge of what that log content looks like.

Major Components
----------------

This project contains the following major components:

* Sawmill:
        A low profile, high throughput syslog adapter which serializes and compresses log content into the Apache Avro format
* Merge-Filter Job:
        A map-only set of jobs that will merge multiple small files from Sawmill into large format files better suited to Hadoop, and apply data rentention policies on all data.  This job is intended to run hourly.
* Access Tools:
        A set of tools for safely accessing, reconstructing and searching log content.  This includes some thin metadata management, pig UDFs, and commandline tools akin to grep.

