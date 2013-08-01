In order to handle the efficient ingest and usage of log data, data gets moved around between a number of different directories.  This page details where the data goes, and how it is managed.

# Directory Structure

## Base Directory

The base directory for storing data in HDFS will be <code>/service</code>.

This directory is owned by the hdfs superuser, with permissions set to 700.

## Service Names and Service Directories

The logs are stored in directories segregated by service, to allow for security controls.  The exact names of the services are an operational concern.  Service names must adhere to the following rules.

- Allowed characters are [A-Za-z0-9/_-]
- All service names start with a slash (/), and do not end with a slash
- By convention, slashes can be used to separate hierarchical service names
- By convention, all logging services end with /logs

Some examples of valid service names would be

- /webhead/logs
- /webhead/pod1/logs
- /WEB/APod/logs
- /WEBhead-Pod1/logs

Some examples that are not valid

- web/logs
- /web/logs/
- /web(pod1)/logs

Data for a given service will be stored in the Service Directory which is {Base Directory}{Service Name}.  For example

- /service/webhead/pod1/logs
- /service/webhead-pod1/logs

All of these directories will be owned by the hdfs superuser, with permissions set to 700.

## Service Users and Groups

Each service will have a user and group assigned to it.  All of the files and directories within the Service Directory will only be modifiable by that user, but will be readable by the entire group.  Generally, these users are ONLY used for loading and maintaining data, and do not map directly to a person.

## Component Names

Each component type within a service will have its own subdirectories within the Service Directories.

The Component Name follows the same naming rules as the Service Name, except that the name does not start with a slash, and cannot include slashes (/).

For example

- applog
- webserver-a

## Log directories

Within the Service Directory, there are several directories where logs are stored.

- {Service Directory}/incoming
- {Service Directory}/data
- {Service Directory}/archive (Maybe for old, filtered data?  Or we might keep it in data)
- {Service Directory}/working
- {Service Directory}/failed

All of these directories are owned by the service's user and group, with permissions set to 750.

Within each of these directories are subdirectories for each day and hour of logs.  For /incoming, /data and /archive, the date subdirectories are immediately below that directory.  For /working and /failed there is a further directory named for the job that is working.

The date and hour format is /YYYYMMDD/HH.  Note that the time starts with a slash, and does not end with a slash.

Within each hour directory, there are subdirectories for each Component Name.

For example, the directories that exist for a given hour may include:

- {Service Directory}/incoming/20120502/10/{Component Name}
- {Service Directory}/data/20120502/10/{Component Name}
- {Service Directory}/archive/20120502/10/{Component Name}
- {Service Directory}/working/job_XXXXX_00010/20120502/10/{Component Name}
- {Service Directory}/working/job_XXXXX_00015/20120502/10/{Component Name}
- {Service Directory}/failed/job_XXXXX_00007/20120502/10/{Component Name}

## File Names

Files are stored in the [Boom](BoomFiles.md) format.

Each file is named using the convention <code>{Component Name}.{Application Specific}.bm</code>.

The Component Name is as specified above.  It will always match the directory name.

The Application Specific part can be used by the process or job that creates the file to ensure uniqueness of file names.  It should not be assumed to have any useful meaning.

## Data Flow
Data flows from the incoming directory, to the data directory to the archive directory.  In each transition, the files are moved to a working directory while they are processed, and if successful then the result is moved to the next directory.  If the processing fails, then the entire working/{ID} directory is moved to the failed directory, and left to be manually recovered.

```
+--------+      +------------+      +----+      +------------+      +-------+
|incoming| ---> |working/{ID}| ---> |data| ---> |working/{ID}| ---> |archive|
+--------+      +------------+      +----+      +------------+      +-------+
                       |                               |
                       |     +-----------+             |     +-----------+
                       \---> |failed/{ID}|             \---> |failed/{ID}|
                             +-----------+                   +-----------+
```

## So Where Is My Data??

Given a service, component and hour, the data is found in any or all of these directories.

- {Service Directory}/incoming/YYYYMMDD/HH/{Component Name}/{Component Name}.*.bm
- {Service Directory}/data/YYYYMMDD/HH/{Component Name}/{Component Name}.*.bm
- {Service Directory}/archive/YYYYMMDD/HH/{Component Name}/{Component Name}.*.bm
- {Service Directory}/working/\*/YYYYMMDD/HH/{Component Name}/{Component Name}.*.bm

Simply using these directories as input will get you the results you need.

Most MapReduce jobs (those using the FileInputFormat) will simple accept a comma separated list of these globs as input, and find everything.  For example, this input string will get you all the data for the component 'Web-ABC' in the service '/web/logs' for the hour '/20120502/10'

    /service/web/logs/incoming/20120502/10/applog.*.bm,/service/web/logs/data/20120502/10/webhead-ABC.*.bm,/service/web/logs/archive/20120502/10/webhead-ABC.*.bm,/service/web/logs/working/*/20120502/10/webhead-ABC.*.bm

In practice, generating these input strings can and will be done automatically based on service, component and time.

## Concurrency Concerns

In this system, all of the log data must be available at all times, to ensure the integrity of data processing and operational troubleshooting.

But since the data is also being moved and transformed by the system, a user who simple uses 'ls' to find the files they want to process may find that the files have been moved or deleted before the job launches!

To avoid these issues, see [Concurrency and Locking](Concurrency.md).
