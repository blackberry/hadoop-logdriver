#!/usr/bin/perl

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

use warnings;
use strict;

## Some things that need to be configured for the environment
my $LOGDRIVER_HOME = $ENV{'LOGDRIVER_HOME'}
                     || die '$LOGDRIVER_HOME is not defined';
my $HADOOP_JAR     = $ENV{'HADOOP_JAR'}
                     || die '$HADOOP_JAR is not defined';
my $DFS_CAT        = $ENV{'DFS_CAT'}
                     || die '$DFS_CAT is not defined';
my $DFS_MKDIR      = $ENV{'DFS_MKDIR'}
                     || die '$DFS_MKDIR is not defined';
my $DFS_PUT        = $ENV{'DFS_PUT'}
                     || die '$DFS_PUT is not defined';
my $DFS_RMR        = $ENV{'DFS_RMR'}
                     || die '$DFS_RMR is not defined';
my $DFS_DUS        = $ENV{'DFS_DUS'}
                     || die '$DFS_DU is not defined';
my $DFS_GET        = $ENV{'DFS_GET'}
                     || die '$DFS_GET is not defined';
my $HADOOP_VERSION = $ENV{'HADOOP_VERSION'}
                     || die '$HADOOP_VERSION is not defined';

my $LOGDRIVER_JAR     = `cd $LOGDRIVER_HOME; ls logdriver-*.jar | head -1`;
chomp $LOGDRIVER_JAR;
my $LOGDRIVER_HDEPLOY = 'hadoop-deploy/logdriver-hdeploy.jar';
my $PIG_DIR           = "$LOGDRIVER_HOME/pig";
my $DATE_FORMAT       = "RFC5424";

## The arguments are 
##  - regex
##  - dc number
##  - service
##  - component
##  - startTime (Something 'date' can parse, or just a time in ms from epoch)
##  - endTime (Same as start)
##  - outputDir

## Generate filename to temporarily store output of mapreduce jobs and pig logs
my $local_output = `mktemp`;
chomp $local_output;
my $pig_tmp = `mktemp`;
chomp $pig_tmp;

## Create local variable with suggestions for diagnosing errors
my $error_output = "

    Please verify the DC and service names, as well
    as that logs exist during the specified time range.
    Use the \"hdfs dfs -ls /service\" command to get started.

    Also note that logsearch tools cannot be run as the HDFS user.

    For more detailed error output, re-run your command with the -v flag.
";

## Other options
my $date_format = $DATE_FORMAT;
my $field_separator = '';
my @D_options = ();
my $quiet = 1;
## Redirect all STDERR data to a file to check for the number of records found.
my $redirects = " 2>$local_output";
my $forceremote = 0;
my $forcelocal = 0;
my $queue_name = "logsearch";

my @args = ();
while (@ARGV > 0) {
  my $arg = shift @ARGV;
  if ($arg eq '--') {
    last;
  }
  elsif ($arg eq '-') {
    # Skip it, since it may be a redirect to stdout
    push @args, $arg;
  }
  elsif ($arg =~ /^-/) {
    if ($arg eq '-v') {
      $quiet = 0;
        ## Use tee to display output as well as capture STDERR for the records check.
        $redirects = " 2>&1 | tee $local_output 1>&2";
    }
    elsif ($arg eq '-i') {
      push @D_options, "-Dlogdriver.search.case.insensitive=true";
    }
    elsif ($arg eq '-r') {
      if ($forcelocal) {
        print STDERR "Can't force both remote and local sorting.";
        exit 1;
      }
      $forceremote = 1;
    }
    elsif ($arg eq '-l') {
      if ($forceremote) {
        print STDERR "Can't force both remote and local sorting.";
        exit 1;
      }
      $forcelocal = 1;
    }
    elsif ($arg =~ '^-dateFormat=(.*)') {
      $date_format = $1;
    }
    elsif ($arg =~ '^-fieldSeparator=(.*)') {
      $field_separator = $1;
    }
    elsif ($arg =~ '^-D') {
      push @D_options, $arg;
    }
    else {
      print "Unrecognized option $arg\n";
      exit 1;
    }
  }
  else {
    push @args, $arg;
  }
}
## Push any remaining args after a --
push @args, @ARGV;

if (@args < 7) {
  print <<"END"
Usage: loggrep [OPTIONS] REGEX DC SERVICE COMPONENT START END OUTPUT_DIR
Note:
  If OUTPUT_DIR is '-', then results are written to stdout.
Options:
  -v                  Verbose output.
  -i                  Make search case insensitive.
  -r                  Force remote sort.
  -l                  Force local sort.
  -dateFormat=FORMAT  Valid formats are RFC822, RFC3164 (zero padded day),
                      RFC5424 (default), or any valid format string for FastDateFormat.
  -fieldSeparator=X   The separator to use to separate fields in intermediate
                      files.  Defaults to 'INFORMATION SEPARATOR ONE' (U+001F).
END
  ;
  exit 1;
}

my $field_separator_in_hex = '';
{
  my @chars = unpack('C*', $field_separator);
  $field_separator_in_hex = sprintf "%1x", $chars[0];
}

my $regex     = shift @args;
my $dc        = shift @args;
my $service   = shift @args;
my $component = shift @args;
my $start     = shift @args;
my $end       = shift @args;
my $out       = shift @args;

# If start and end are numbers, just use them.  Otherwise, pass them through
# date.
if ($start !~ /^\d+$/) {
  $start = `date -d '$start' +%s`;
  if ($start eq '') {
    print "Could not parse start time.\n";
    exit 1;
  }
  $start *= 1000;
}
if ($end !~ /^\d+$/) {
  $end = `date -d '$end' +%s`;
  if ($end eq '') {
    print "Could not parse end time.\n";
    exit 1;
  }
  $end *= 1000;
}

## We'll need a temp dir for some stuff
my $tmp = 'tmp/loggrep-';
my @chars = ('a'..'z', 'A'..'Z', '0'..'9');
for my $i (1 .. 10) {
  $tmp .= $chars[rand @chars];
}

## Compress the output of the mapreduce job
my $mr_opts = "";
$mr_opts .= " -Dzk.connect.string=" . escape($ENV{'ZK_CONNECT_STRING'});
$mr_opts .= " -Dmapred.output.compress=true";
$mr_opts .= " -Dmapred.output.compression.codec="
              . "org.apache.hadoop.io.compress.SnappyCodec";
$mr_opts .= " -Dmapred.max.split.size=" . (256*1024*1024);
$mr_opts .= " -Dlogdriver.output.field.separator=" . escape($field_separator);
$mr_opts .= " -Dmapred.job.queue.name=" . escape($queue_name);

## Get the list of additional jars we'll need for pig
my $additional_jars = "$LOGDRIVER_HOME/$LOGDRIVER_JAR";

## Various pig opts
my $pig_opts = "";
$pig_opts .= " -Dpig.exec.reducers.bytes.per.reducer=" . (100*1000*1000);

## Add any overridden -D options from the command line
for my $opt (@D_options) {
+
  $mr_opts  .= ' ' . $opt;
  $pig_opts .= ' ' . $opt;
}

## Add the required properties
my $props = '';
$props .= " -param dateFormat=" . escape($date_format);
$props .= " -param fs=" . escape($field_separator_in_hex);

my $mkdir_cmd = "$DFS_MKDIR $tmp $redirects 1>&2";

my $mr_cmd = "$HADOOP_JAR $LOGDRIVER_HOME/$LOGDRIVER_HDEPLOY "
        . "com.rim.logdriver.util.GrepByTime"
        . " " . $mr_opts
        . " " . escape($regex)
        . " " . escape($dc)
        . " " . escape($service)
        . " " . escape($component)
        . " " . escape($start)
        . " " . escape($end)
        . " " . escape("$tmp/rawlines")
        . " " . $redirects;

my $rm_tmp_cmd = "$DFS_RMR $tmp $redirects 1>&2";

$quiet or print STDERR "Running: $mkdir_cmd\n";
(0 == system $mkdir_cmd)
  || die $!;

print STDERR "Searching for \"$regex\"...\n";
$quiet or print STDERR "Running: $mr_cmd\n";
(0 == system $mr_cmd)
  || die "\n    Error running mapreduce job." . $error_output . "\nCommand stopped";

#Before sorting, determine the size of the results found
my $foundresults = 0;
open my $redirectoutput, $local_output || die $!;
while(my $line = <$redirectoutput>) {
  if ($line =~ /Map output records/) {
    my @records = split('=',$line);
    $foundresults = $records[1];
  }
}
close($redirectoutput);

my $rawlines = "$tmp/rawlines";
my @resultsize = split(' ',`$DFS_DUS $rawlines`);
my $size = 0;

if ($HADOOP_VERSION eq "2") {
  $size = $resultsize[0];
} else {
  $size = $resultsize[1];
}

#Set maximum size of results to sort locally, in MB
my $maxlocalsize = 256;

#If results found are larger than maxlocalsize, sort on the cluster
#If results found are smaller than maxlocalsize, sort locally
#If results are zero size, print message and exit

## Because Pig is currently broken in local mode, force all searches to sort remotely. 
#$forceremote = 1;
#$forcelocal = 0;

if ($foundresults == 0) {
  print STDERR "No matches found.\n";

  $quiet or print STDERR "Running: $rm_tmp_cmd\n";
  (0 == system $rm_tmp_cmd)
    || die $!;
}
elsif ($forceremote || $size > $maxlocalsize * 1024 * 1024) {

  if ($forceremote) {
    print STDERR "Remote sorting forced.\n";
  } else {
    print STDERR "Results are ".(int(100*$size/(1024*1024))/100)." MB. Using non-local sort...\n";
  }
  
  ## Set input parameter for pig job
  $props .= " -param tmpdir=" . escape($tmp);

  ## Check for an out of '-', meaning write to stdout
  if ($out eq '-') {
    $props.= " -param out=$tmp/final";
  }
  else {
    $props .= " -param out=" . escape($out);
  }

  my $pig_cmd = "pig -Dmapred.job.queue.name=$queue_name -Dpig.additional.jars=$additional_jars $pig_opts $props"
          . " -l $pig_tmp -f $PIG_DIR/formatAndSort.pg $redirects 1>&2";

  $quiet or print STDERR "Running: $pig_cmd\n";
  (0 == system $pig_cmd)
    || die $!;

  if ($out eq '-') {
    system "$DFS_CAT $tmp/final/part-*"
  } else {
    print STDERR "Done. Search results are in $out.\n";
  }

  $quiet or print STDERR "Running: $rm_tmp_cmd\n";
  (0 == system $rm_tmp_cmd)
    || die $!;

} elsif ($forcelocal || $size > 0) {

  if ($forcelocal) {
    print STDERR "Local sorting forced.\n";
  } else {
    print STDERR "Results are ".(int(100*$size/(1024*1024))/100)." MB. Sorting locally...\n";
  }

  ## Create a local tmp folder for the data needed by format and sort
  my $local_tmp = `mktemp -d`;
  chomp $local_tmp;
  (0 == system("mkdir $local_tmp/tmp")) 
    || die $!;

  ## Set input parameter for pig job
  $props .= " -param tmpdir=$local_tmp/$tmp";

  ## Check for an out of '-', meaning write to stdout
  if ($out eq '-') {
    $props.= " -param out=$local_tmp/$tmp/final";
  }
  else {
    $props .= " -param out=$local_tmp/$out";
  }

  my $pig_cmd = "HADOOP_CONF_DIR=/dev/null /usr/lib/pig/bin/pig -Dpig.additional.jars=$additional_jars $pig_opts $props"
          . " -l $pig_tmp -x local -f $PIG_DIR/formatAndSortLocal.pg $redirects 1>&2";

  ## Copy the tmp folder from HDFS to the local tmp directory, and delete the remote folder
  $quiet or print STDERR "Running $DFS_GET $tmp $local_tmp/$tmp\n";
  (0 == system("$DFS_GET $tmp $local_tmp/$tmp"))
    || die $!;

  $quiet or print STDERR "Running $DFS_RMR $tmp\n";
  (0 == system $rm_tmp_cmd)
    || die $!;

  $quiet or print STDERR "Running: $pig_cmd\n";
  (0 == system $pig_cmd)
    || die $!;

  if ($out eq '-') {
    system "cat $local_tmp/$tmp/final/part-*"
  } else {
    ## Copy the result to HDFS
    (0 == system("$DFS_PUT $local_tmp/$out $out 1>/dev/null"))
      || die $!;
    print STDERR "Done. Search results are in $out.\n";
  }

  ## Delete the local files (and folders, if empty)
  ## system("rm -rf $tmp/* $tmp/.* 2>/dev/null; rmdir -p $tmp 2>/dev/null; rm -rf $out/* $out/.* 2>/dev/null; rmdir -p $out 2>/dev/null");
  system("rm -rf $local_tmp");
}

(0 == system("rm -rf $local_output 2>/dev/null"))
  || die $!;

## END

sub escape {
  my $string = shift;
  $string =~ s{'}{'\\''}g;
  return "'$string'";
}
