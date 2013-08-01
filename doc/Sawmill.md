Sawmill is a simple, lightweight syslog adaptor which serializes and compresses content into the [Boom Format](BoomFiles.md), and delivers that content into HDFS.

## Important Notes
### Security
Sawmill supports kerberos and a strict security model through impersonation allowing multiple services to deliver content and maintain separation.

### Guaranteed Delivery - or lack thereof.
Sawmill DOES NOT guarantee delivery.  This is by design, as the syslog protocol itself does not support this.  Additionally, high-volume environments often find disk queueing too expensive to support, especially at 50+ TB/day rates.

Sawmill DOES, however, track and report any time it drops a message, via JMX reporting metrics.  Many other health metrics, including processing volumes, are available via JMX.

### Heavy memory queueing
Sawmill makes active use of memory to support inconsistancies in network performance, HDFS performance and other possible issues.  Sawmill has been successfully used to deliver content to HDFS clusters over 100ms away in latency without issue.

## Configuration

This walks through a generic configuration of Sawmill.  Please review the configuration examples at <code>/etc/logdriver/conf.example</code> for detailed documentation.

### Pre-requisites

* Install the LogDriver RPM or DEB.  This should generally take care of base dependancies.
* Client configuration should be installed in <code>/etc/hadoop/conf</code>.
* Java 6u33 or above is recommended.  Java 7 is not currently supported.
* The high-security pack (US_export_policy.jar and local_policy.jar) supporting AES256 is recommended.
* General hadoop client packages should all be installed, and access to HDFS via standard tools should be working.

### Setting up a service

In this example, we use 'web' as the service located in 'dc1' containing the component 'app'.

* Each service should have an administrative user and group that is unique.  This group is used for sawmill data loading and [Log Maintainance](LogMaintenance.md), and should not be used for any other purpose.
* HDFS basic paths need to be created.  For example, <code>/service/dc1/web/logs/</code> should exist for the service 'web' in 'dc1'.
* A Kerberos key capable of writing to the HDFS directory is required, or impersonation must be set up for the sawmill user.

At this point, you'll need to update your sawmill configuration - see the example configuration for starting points.

### Using impersonation

If you're using sawmill to accept logs for multiple separate services, impersonation will be required.  In order to do this, add the following configuration to your namenode:

```none
<property>
   <name>hadoop.proxyuser.<USER>.groups</name>
   <value><IMPERSONALBLE GROUPS></value>
</property>
<property>
   <name>hadoop.proxyuser.<USER>.hosts</name>
   <value><SAWMILL HOSTS></value>
</property>
```

### Calculating Heap

Setting the correct maximum heap size for Sawmill can be tricky.  We're not terrible happy with how this is just yet, but it works for the time being.  As a general rule, using the values you've configured in the sawmill configuration, each path block contains two statements which contain values for calculating the required size of the java heap
```none
path.<example_path>.queue.capacity = 200000 
path.<example_path>.output.buckets = 4
```
The queue.capacity statement contains the base value of the queue to be assigned for this path block. To simplify the calculation record each 1000 as 1mb.  The output.buckets for the purpose of the heap size calculation acts as a multiplier.  Note that this statement is optional - if it is not included in a path block the multiple defaults to 1.

Using all the path blocks, queue sizes and number of buckets you can calculate the heap size.
```none
Part 1:  Calculate the overhead value

Overhead value = Number of path blocks X 8

Part 2:  Calculate heap size for all path blocks then sum

path block heap size = queue.capacity * output.buckets

total heap size = (path block1 heap size) + (path block2 heap size)

Part 3:  Unadjusted heap size

unadjusted heap size = overhead value + total heap size

Part 4:  Adjusted heap size

A) mutliplier = unadjusted heap size/128
B) adjusted heap size = 128 * (multiplier rounded up)
```

So the example math looks like so:
```none
Part 1: Overhead value = 1x8 = 8
Part 2: path block heap size = 200*4
        total heap size = 800
Part 3: unadjusted heap size = 808
Part 4: multiplier = 808/128 = 6.3125
    adjusted heap size = 128*7 = 896
```

Once the adjusted heap size is calculated it needs to set in the sawmill-env.sh file.  Edit the following line
```none
JAVA_OPTS="-Xmx<heap size>m -Xms<heap size>m -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=20304 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
```
Replace heap size with the adjusted heap size value.
