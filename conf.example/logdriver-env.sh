## Various settings used in logdriver scripts

##### General configs #####
## The directory where LogDriver was installed.
export LOGDRIVER_HOME=/usr/lib/logdriver

## The connection string for connecting to ZooKeeper
## Usually in form <host1>:2181,<host2>:2181,<host3>:2181
export ZK_CONNECT_STRING=zookeeper1.mydomain:2181,zookeeper2.mydomain:2181,zookeeper3.mydomain:2181

## Pig requires JAVA_HOME to be set
export JAVA_HOME=/usr/java/default

## Oozie requires OOZIE_URL to be set
export OOZIE_URL=http://oozie.mydomain:11000/oozie/



##### Sawmill Specific #####
## Sawmill configs are in $LOGDRIVER_CONF_DIR/sawmill by default
export SAWMILL_CONF_DIR=$LOGDRIVER_CONF_DIR/sawmill



##### Log Maintenance Specific #####
## Use this to run log maintenance on a separate Oozie instance
export ADMIN_OOZIE_URL=http://admin.oozie.mydomain:11000/oozie/

## How many days before the logs are deleted by log maintenance
## Set to -1 to disable deleting logs
export DAYS_BEFORE_DELETE=-1

## How many days old the logs are before the filter job is run on them.
## Set to -1 to disable filtering logs
export DAYS_BEFORE_ARCHIVE=-1

## Limit how many Oozie jobs the log maintenace tool will submit at one time.
## Setting to -1 means no limit.
export MAX_CONCURRENT_MERGE_JOBS=-1
export MAX_CONCURRENT_FILTER_JOBS=-1

## The log maintenance tool requires the path to oozie workflow properties
## files.
export MERGEJOB_CONF=$LOGDRIVER_CONF_DIR/mergejob/mergejob.properties
export FILTERJOB_CONF=$LOGDRIVER_CONF_DIR/filterjob/filterjob.properties
