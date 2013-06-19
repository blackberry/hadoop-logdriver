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

#What version of Hadoop are we running?
version1=`hadoop version | head -1 | egrep '^Hadop (0\.20|1\.)'`
version2=`hadoop version | head -1 | egrep '^Hadoop 2\.'`

#Determine Hadoop version, and remove the testservice folder if it exists and replace it with the reference one.
if [ version2 != "" ] 
then 
  echo "Running on Hadoop 2"
  hdfs dfs -rm -r /service/99/logsearch-testservice 2>&1 1>/dev/null
  hdfs dfs -put logsearch-testservice /service/99 2>&1 1>/dev/null
elif [ version1 != "" ] 
then 
  echo "Running on Hadoop 1"
  hadoop dfs -rmr /service/99/logsearch-testservice 2>&1 1>/dev/null
  hadoop dfs -put logsearch-testservice /service/99 2>&1 1>/dev/null
else
  echo "Can't determine Hadoop version."
  exit
fi

#Remove existing output files
rm -rf logsearch-test-output.txt 2> /dev/null
rm -rf logcat-test-output.txt 2> /dev/null
rm -rf loggrep-test-output.txt 2> /dev/null
rm -rf logmultisearch-test-output.txt 2> /dev/null

#Test local sorting
echo "Executing locally sorted test searches."

#Execute remote-sorted searches on test data, dumping results to the output file. 
logsearch 'test' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > logsearch-test-output.txt
logsearch 'TEST' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'ä' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'Ä' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i 'ä' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'fenêtre' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'FENÊTRE' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i 'feNêtRe' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'человек' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'ЧЕЛОВЕК' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i 'ЧЕЛовЕК' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'رجل' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i 'رجل' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'αβγδε' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch 'ΑΒΓΔΕ' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i 'αβγΔΕ' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '#!A' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i '#!a' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '^X' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -i '^x' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '3.14159265358979' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '1.602E-19' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '1.602x10^-19' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '123,456,789.00' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch '2012-02-28T10:00:01Z' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt

#Compare output of test searches to reference output
echo "****"
logsearchresult=`diff -q logsearch-test-output.txt reference-files/logsearch-reference.txt 2>&1`
if [ "$logsearchresult" = "" ] 
then echo "Logsearch tests successful."
else echo "Logsearch test failed."
fi
echo ""

#Dump the logs from the testservice folder
logcat '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > logcat-test-output.txt

#Compare cat output to reference file
echo "****"
logcatresult=`diff -eq logcat-test-output.txt reference-files/logcat-reference.txt 2>&1`
if [ "$logcatresult" = "" ]
then echo "Logcat test successful."
else echo "Logcat test failed."
fi
echo ""

#Run some basic grep commands on the test file, ensuring that multi-byte UTF-8 encoded regexs return properly
loggrep -i '^THIS IS A TEST MESSAGE' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > loggrep-test-output.txt
loggrep '^This' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt
loggrep 'c?n' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt
loggrep 'c*n' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt
loggrep 'αβγδε|человек|fenêtre|ä|رجل' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt

#Compare grep output to reference file
echo "****"
loggrepresult=`diff -eq loggrep-test-output.txt reference-files/loggrep-reference.txt 2>&1`
if [ "$loggrepresult" = "" ]
then echo "Loggrep test successful."
else echo "Loggrep test failed."
fi
echo ""

#Run a multi search, consisting mostly of special characters.
logmultisearch logmultisearch-strings-OR.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > logmultisearch-test-output.txt
logmultisearch -i logmultisearch-strings-OR.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logmultisearch-test-output.txt
logmultisearch -a logmultisearch-strings-AND.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logmultisearch-test-output.txt
logmultisearch -a -i logmultisearch-strings-AND.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logmultisearch-test-output.txt

#Comapre multisearch output to reference file
echo "****"
logmultisearchresult=`diff -eq logmultisearch-test-output.txt reference-files/logmultisearch-reference.txt 2>&1`
if [ "$logmultisearchresult" = "" ]
then echo "Logmultisearch test successful."
else echo "Logmultisearch test failed."
fi

#Test remote sorting
echo ""
echo "Executing remotely sorted test searches."

logsearch -r 'test' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > logsearch-test-output.txt
logsearch -r 'TEST' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'ä' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'Ä' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i 'ä' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'fenêtre' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'FENÊTRE' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i 'feNêtRe' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'человек' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'ЧЕЛОВЕК' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i 'ЧЕЛовЕК' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'رجل' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i 'رجل' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'αβγδε' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r 'ΑΒΓΔΕ' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i 'αβγΔΕ' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '#!A' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i '#!a' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '^X' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r -i '^x' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '3.14159265358979' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '1.602E-19' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '1.602x10^-19' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '123,456,789.00' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt
logsearch -r '2012-02-28T10:00:01Z' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logsearch-test-output.txt

#Compare output of test searches to reference output
echo "****"
logsearchresult=`diff -q logsearch-test-output.txt reference-files/logsearch-reference.txt 2>&1`
if [ "$logsearchresult" = "" ] 
then echo "Logsearch tests successful."
else echo "Logsearch test failed."
fi
echo ""

#Dump the logs from the testservice folder
logcat -r '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > logcat-test-output.txt

#Compare cat output to reference file
echo "****"
logcatresult=`diff -eq logcat-test-output.txt reference-files/logcat-reference.txt 2>&1`
if [ "$logcatresult" = "" ]
then echo "Logcat test successful."
else echo "Logcat test failed."
fi
echo ""

#Run some basic grep commands on the test file, ensuring that multi-byte UTF-8 encoded regexs return properly
loggrep -r -i '^THIS IS A TEST MESSAGE' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > loggrep-test-output.txt
loggrep -r '^This' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt
loggrep -r 'c?n' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt
loggrep -r 'c*n' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt
loggrep -r 'αβγδε|человек|fenêtre|ä|رجل' '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> loggrep-test-output.txt

#Compare grep output to reference file
echo "****"
loggrepresult=`diff -eq loggrep-test-output.txt reference-files/loggrep-reference.txt 2>&1`
if [ "$loggrepresult" = "" ]
then echo "Loggrep test successful."
else echo "Loggrep test failed."
fi
echo ""

#Run a multi search, consisting mostly of special characters.
logmultisearch -r logmultisearch-strings-OR.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' > logmultisearch-test-output.txt
logmultisearch -r -i logmultisearch-strings-OR.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logmultisearch-test-output.txt
logmultisearch -r -a logmultisearch-strings-AND.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logmultisearch-test-output.txt
logmultisearch -r -a -i logmultisearch-strings-AND.txt '99' 'logsearch-testservice' 'logsearch-test' 'Feb 28, 2012 10:00' 'Feb 28, 2012 11:00' '-' >> logmultisearch-test-output.txt

#Comapre multisearch output to reference file
echo "****"
logmultisearchresult=`diff -eq logmultisearch-test-output.txt reference-files/logmultisearch-reference.txt 2>&1`
if [ "$logmultisearchresult" = "" ]
then echo "Logmultisearch test successful."
else echo "Logmultisearch test failed."
fi
