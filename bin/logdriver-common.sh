#!/bin/bash

# Copyright 2013 BlackBerry, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. 

# Ensure that LOGDRIVER_HOME is set
if [ "x$LOGDRIVER_HOME" == "x" ]
then
  echo "LOGDRIVER_HOME is not set"
  exit 1
fi

# check if we're using CDH3 or CDH4.  The command lines changed between
# versions.

if hadoop version | head -1 | egrep '^Hadoop 2\.' >/dev/null
then
  # Hadoop 2.x, like CDH4
  DFS_LS='hdfs dfs -ls'
  DFS_DU='hdfs dfs -du'
  DFS_DUS='hdfs dfs -du -s'
  DFS_CAT='hdfs dfs -cat'
  DFS_MKDIR='hdfs dfs -mkdir -p'
  DFS_GET='hdfs dfs -get'
  DFS_PUT='hdfs dfs -put'
  DFS_RMR='hdfs dfs -rm -r -skipTrash >&2'
  DFS_TEST='hdfs dfs -test'
  HADOOP_JAR='hadoop jar'
  HADOOP_VERSION='2'
elif hadoop version | head -1 | egrep '^Hadoop (0\.20|1\.)' >/dev/null
then
  # Hadoop 0.20.x or 1.x, like CDH3
  DFS_LS='hadoop dfs -ls'
  DFS_DU='hadoop dfs -du'
  DFS_DUS='hadoop dfs -dus'
  DFS_CAT='hadoop dfs -cat'
  DFS_MKDIR='hadoop dfs -mkdir'
  DFS_GET='hadoop dfs -get'
  DFS_PUT='hadoop dfs -put'
  DFS_RMR='hadoop dfs -rmr'
  DFS_TEST='hadoop dfs -test'
  HADOOP_JAR='hadoop jar'
  HADOOP_VERSION='1'
else
  echo "Cannot figure out which version of hadoop we are running." 1>&2
fi
export DFS_LS DFS_DU DFS_DUS DFS_CAT DFS_MKDIR DFS_GET DFS_PUT DFS_RMR DFS_TEST HADOOP_JAR HADOOP_VERSION
