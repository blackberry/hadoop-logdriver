#!/bin/bash

## Various Sawmill configuration items

## Set various Java options here
JAVA_OPTS=""
# heap size
JAVA_OPTS="$JAVA_OPTS -Xmx1g -Xms1g"
# garbage collection options
JAVA_OPTS="$JAVA_OPTS -XX:+UseConcMarkSweepGC"
# JMX settings
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=20304 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
# Set retries to higher than the number of nodes in a rack, to prevent write
# failures when a rack is lost or restarted all at once.
JAVA_OPTS="$JAVA_OPTS -Ddfs.client.block.write.retries=20"
export JAVA_OPTS

## The path to the configuration file
export SAWMILL_CONF=$SAWMILL_CONF_DIR/sawmill.conf

## The path where STDOUT and STDERR logs will be written.  Useful if it dies
## before logging can start up.
export SAWMILL_INIT_LOG=/var/log/sawmill/sawmill.init.log

## The location of the Sawmill PID file.
export SAWMILL_PID_FILE=/var/run/sawmill/sawmill.pid

# By default, Sawmill will run as the user sawmill. Change this to run as a 
# different user, or leave it blank to run as whomever starts it (probably
# root).
export SAWMILL_USER=sawmill

# Sawmill will attempt to destroy any existing Kerberos tickets before
# starting up.  This is done by issuing the command here (default: kdestroy).
# If you don't want to do this, then explicitly set this to blank.
export KDESTROY=kdestroy
