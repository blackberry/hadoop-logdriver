#Overview
A set of test cases are included with the Logdriver package to determine if logsearch, loggrep, logmultisearch and logcat are behaving as expected. This package creates a small test service, runs the tools against the test service, and finally compares the output of each tool to reference output. 

#Requirements
The test package creates /service/99/logsearch-testservice and adds some test boom files under the component name logsearch-test. The output of the search tools are written into files in the local directory. Therefore the user running the script must have write access to both the local directory and to the /service directory within HDFS. 

#Tested Cases
- Logsearch searches for
 - Latin + accents + case insensitive
 - Cyrillic + accents + case insensitive
 - Arabic (no upper and lower case, tests the -i flag anyway)
 - Greek + case insensitive
 - A variety of numeric, control and escape-type character sequences
 - Formatted dates
- Logcat
 - Dumps entire test log
- Loggrep
 - Tests simple regex expressions
 - Tests expressions including multibyte UTF-8
- Logmultisearch
 - Single search using strings including multibyte UTF-8, control and escape sequences

#Use and Expected Results
To run the tools, execute logdriver/testcases/logsearch/logsearch.test.script.sh from that folder. This tool will create the service folder, run the commands, diff the output against reference and return a pass/fail message for each of the four tools.

Output will include status output as each command runs. After testing each tool a status message like the following will appear:

<pre><code>****
Logsearch tests successful.
</code></pre>

Each of logsearch, logcat, loggrep and logmultisearch will return a success/fail status. 

#Services Tested
The log searching tools test functionality of the following services:
- Mapreduce
- HDFS
- Zookeeper
- Pig (currently remote only, see below)

#Notes
Each of these tools chooses to sort results either in Pig local or remote mode based on the size of the results returned. Sorting locally significantly improves performance when sorting small (<512MB) search results. 
