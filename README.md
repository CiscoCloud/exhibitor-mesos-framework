Exhibitor Mesos Framework
======================

This is still being developed actively and is not yet alpha. [Open issues here](https://github.com/CiscoCloud/exhibitor-mesos-framework/issues).

[Prerequisites](#prerequisites)  
* [Environment Configuration](#environment-configuration)  
* [Scheduler Configuration](#scheduler-configuration)  
* [Run the scheduler](#run-the-scheduler)  
* [Quick start](#quick-start)  

[Typical Operations](#typical-operations)  
* [Running scheduler in Marathon](https://github.com/CiscoCloud/exhibitor-mesos-framework/tree/master/src/marathon)   
* [Changing the location of Zookeeper data](#changing-the-location-of-zookeeper-data)

[Navigating the CLI](#navigating-the-cli)  
* [Requesting help](#requesting-help)  
* [Adding servers to the cluster](#adding-servers-to-the-cluster)  
* [Configuring servers](#configuring-servers-in-the-cluster)  
* [Starting servers](#starting-servers-in-the-cluster)  
* [Stopping servers](#stopping-servers-in-the-cluster)  
* [Removing servers](#removing-servers-from-the-cluster)  

Prerequisites
-------------

* Java 7 (or higher)
* Apache Mesos 0.19 or newer
* Exhibitor Standalone jar file (or `mvn` to build it)
* Zookeeper archive file

Clone and build the project

    # git clone https://github.com/CiscoCloud/exhibitor-mesos-framework.git
    # cd exhibitor-mesos-framework
    # ./gradlew jar
    
Build Exhibitor Standalone if necessary (**NOTE**: version built with Gradle may be affected by [this issue](https://github.com/Netflix/exhibitor/issues/235) so we use [Maven build](https://github.com/Netflix/exhibitor/wiki/Building-Exhibitor#maven) in this example):

    # mkdir tmp-exhibitor && cd tmp-exhibitor
    # wget https://raw.github.com/Netflix/exhibitor/master/exhibitor-standalone/src/main/resources/buildscripts/standalone/maven/pom.xml
    # mvn clean package
    # cp target/exhibitor-*.jar ..
    # cd .. && rm -rf tmp-exhibitor
    
Download Apache Zookeeper distribution if you don't have one (or place the archive to the working folder):

    # wget http://apache.cp.if.ua/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
    
Download Oracle JDK distribution (or place the archive to the working folder). **NOTE**: please pay attention it MUST be Oracle JDK (not OpenJDK and not JRE) as Exhibitor relies on `jps` calls:

    # wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u45-b14/jdk-8u45-linux-x64.tar.gz

Environment Configuration
--------------------------

Before running `./exhibitor-mesos.sh`, set the location of libmesos:

    # export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so

If the host running scheduler has several IP addresses you may also need to

    # export LIBPROCESS_IP=<IP_ACCESSIBLE_FROM_MASTER>

Scheduler Configuration
----------------------

The scheduler is configured through the command line.

Following options are available:
```
Usage: scheduler [options]

  -m <value> | --master <value>
        Mesos Master addresses. Required.
  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
  -u <value> | --user <value>
        Mesos user. Required.
  --ensemblemodifyretries <value>
        Number of retries to modify (add/remove server) ensemble. Defaults to 60. Optional.
  --ensemblemodifybackoff <value>
        Backoff between retries to modify (add/remove server) ensemble in milliseconds. Defaults to 1000. Optional.
  -d <value> | --debug <value>
        Debug mode. Optional. Defaults to false.

```

Run the scheduler
-----------------

Start the Exhibitor scheduler using this command:

    # ./exhibitor-mesos.sh scheduler --master master:5050 --user root --api http://master:6666


Quick start
-----------

In order not to pass the API url to each CLI call lets export the URL as follows:

```
# export EM_API=http://master:6666
```

First lets start 1 Exhibitor with the default settings. Further in the readme you can see how to change these from the defaults.

```
# ./exhibitor-mesos.sh add 0
Added servers 0

cluster:
  server:
    id: 0
    state: Added
    constraints: hostname=unique
    exhibitor config:
    shared config overrides:
    cpu: 0.2
    mem: 256.0
    sharedConfigChangeBackoff: 10000
    port: auto
```

You now have a cluster with 1 server that is not started.

```
# ./exhibitor-mesos.sh status
cluster:
  server:
    id: 0
    state: Added
    constraints: hostname=unique
    exhibitor config:
    shared config overrides:
    cpu: 0.2
    mem: 256.0
    sharedConfigChangeBackoff: 10000
    port: auto
```

Each server requires some basic configuration.

```
# ./exhibitor-mesos.sh config 0 --configtype zookeeper --zkconfigconnect 192.168.3.1:2181 --zkconfigzpath /exhibitor/config --zookeeper-install-directory /tmp/zookeeper --zookeeper-data-directory /tmp/zkdata
Updated configuration for servers 0

cluster:
  server:
    id: 0
    state: Added
    constraints: hostname=unique
    exhibitor config:
      zkconfigzpath: /exhibitor/config
      zkconfigconnect: 192.168.3.1:2181
      configtype: zookeeper
    shared config overrides:
      zookeeper-install-directory: /tmp/zookeeper
      zookeeper-data-directory: /tmp/zkdata
    cpu: 0.2
    mem: 256.0
    sharedConfigChangeBackoff: 10000
    port: auto
```

Now lets start the server. The state should change from `Added` to `Stopped` meaning the task is waiting for resources to be offered.

```
# ./exhibitor-mesos.sh start 0
Started servers 0

cluster:
  server:
    id: 0
    state: Stopped
    constraints: hostname=unique
    exhibitor config:
      zkconfigzpath: /exhibitor/config
      zkconfigconnect: 192.168.3.1:2181
      configtype: zookeeper
    shared config overrides:
      zookeeper-install-directory: /tmp/zookeeper
      zookeeper-data-directory: /tmp/zkdata
    cpu: 0.2
    mem: 256.0
    sharedConfigChangeBackoff: 10000
    port: auto
```

Now as we don't know where the server we may ask for the cluster status to see where the endpoint is.

```
# ./exhibitor-mesos.sh status
cluster:
  server:
    id: 0
    state: Running
    endpoint: http://slave0:31000/exhibitor/v1/ui/index.html
    constraints: hostname=unique
    exhibitor config:
      zkconfigzpath: /exhibitor/config
      zkconfigconnect: 192.168.3.1:2181
      port: 31000
      configtype: zookeeper
    shared config overrides:
      zookeeper-install-directory: /tmp/zookeeper
      zookeeper-data-directory: /tmp/zkdata
    cpu: 0.2
    mem: 256.0
    sharedConfigChangeBackoff: 10000
    port: auto
```

By now you should have a single Exhibitor instance running. Here's how you stop it:

```
# ./exhibitor-mesos.sh stop 0
Stopped servers 0
```

If you want to remove the server from the cluster completely you may skip `stop` step and call `remove` directly (this will call `stop` under the hood anyway):

```
./exhibitor-mesos.sh remove 0
Removed servers 0
```

Typical Operations
===================

Changing the location of Zookeeper data
---------------------------------------

```
# ./exhibitor-mesos.sh stop 0
Stopped servers 0

# ./exhibitor-mesos.sh config 0 --zookeeper-data-directory /tmp/exhibitor_zkdata
Updated configuration for servers 0

cluster:
  server:
    id: 0
    state: Added
    constraints: hostname=unique
    exhibitor config:
      zkconfigzpath: /exhibitor/config
      zkconfigconnect: 192.168.3.1:2181
      configtype: zookeeper
    shared config overrides:
      zookeeper-install-directory: /tmp/zookeeper
      zookeeper-data-directory: /tmp/exhibitor_zkdata
    cpu: 0.2
    mem: 256.0
    sharedConfigChangeBackoff: 10000
    port: auto
```

Navigating the CLI
==================

Requesting help
---------------

```
# ./exhibitor-mesos.sh help
Usage: <command>

Commands:
  help       - print this message.
  help [cmd] - print command-specific help.
  scheduler  - start scheduler.
  status     - print cluster status.
  add        - add servers to cluster.
  config     - configure servers in cluster.
  start      - start servers in cluster.
  stop       - stop servers in cluster.
  remove     - remove servers in cluster.
```

Adding servers to the cluster
-------------------------------

```
# ./exhibitor-mesos.sh help add
Usage: add <id> [options]

  -c <value> | --cpu <value>
        CPUs for server. Optional.
  -m <value> | --mem <value>
        Memory for server. Optional.
  --constraints <value>
        Constraints (hostname=like:master,rack=like:1.*). See below. Defaults to 'hostname=unique'. Optional.
  -b <value> | --configchangebackoff <value>
        Backoff between checks whether the shared configuration changed in milliseconds. Defaults to 10000. Optional.
  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
  --port <value>
        Port ranges to accept, when offer is issued. Optional

constraint examples:
  like:slave0    - value equals 'slave0'
  unlike:slave0  - value is not equal to 'slave0'
  like:slave.*   - value starts with 'slave'
  unique         - all values are unique
  cluster        - all values are the same
  cluster:slave0 - value equals 'slave0'
  groupBy        - all values are the same
  groupBy:3      - all values are within 3 different groups
```

Configuring servers in the cluster
----------------------

**NOTE**: this section is not final and some configurations may change.

```
# ../exhibitor-mesos.sh help config
Usage: config <id> [options]

  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
  --configtype <value>
        Config type to use: s3 or zookeeper. Optional.
  --configcheckms <value>
        Period (ms) to check for shared config updates. Optional.
  --defaultconfig <value>
        Full path to a file that contains initial/default values for Exhibitor/ZooKeeper config values. The file is a standard property file. Optional.
  --headingtext <value>
        Extra text to display in UI header. Optional.
  --hostname <value>
        Hostname to use for this JVM. Optional.
  --jquerystyle <value>
        Styling used for the JQuery-based UI. Optional.
  --loglines <value>
        Max lines of logging to keep in memory for display. Default is 1000. Optional.
  --nodemodification <value>
        If true, the Explorer UI will allow nodes to be modified (use with caution). Default is true. Optional.
  --prefspath <value>
        Certain values (such as Control Panel values) are stored in a preferences file. By default, Preferences.userRoot() is used. Optional.
  --servo <value>
        true/false (default is false). If enabled, ZooKeeper will be queried once a minute for its state via the 'mntr' four letter word (this requires ZooKeeper 3.4.x+). Servo will be used to publish this data via JMX. Optional.
  --timeout <value>
        Connection timeout (ms) for ZK connections. Default is 30000. Optional.
  --s3credentials <value>
        Credentials to use for s3backup or s3config. Optional.
  --s3region <value>
        Region for S3 calls (e.g. "eu-west-1"). Optional.
  --s3config <value>
        The bucket name and key to store the config (s3credentials may be provided as well). Argument is [bucket name]:[key]. Optional.
  --s3configprefix <value>
        When using AWS S3 shared config files, the prefix to use for values such as locks. Optional.
  --zkconfigconnect <value>
        The initial connection string for ZooKeeper shared config storage. E.g: host1:2181,host2:2181... Optional.
  --zkconfigexhibitorpath <value>
        Used if the ZooKeeper shared config is also running Exhibitor. This is the URI path for the REST call. The default is: /. Optional.
  --zkconfigexhibitorport <value>
        Used if the ZooKeeper shared config is also running Exhibitor. This is the port that Exhibitor is listening on. IMPORTANT: if this value is not set it implies that Exhibitor is not being used on the ZooKeeper shared config. Optional.
  --zkconfigpollms <value>
        The period in ms to check for changes in the config ensemble. The default is: 10000. Optional.
  --zkconfigretry <value>
        The retry values to use in the form sleep-ms:retry-qty. The default is: 1000:3. Optional.
  --zkconfigzpath <value>
        The base ZPath that Exhibitor should use. E.g: /exhibitor/config. Optional.
  --filesystembackup <value>
        If true, enables file system backup of ZooKeeper log files. Optional.
  --s3backup <value>
        If true, enables AWS S3 backup of ZooKeeper log files (s3credentials may be provided as well). Optional.
  --aclid <value>
        Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's ID. Optional.
  --aclperms <value>
        Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's Permissions - a comma list of possible permissions. If this isn't specified the permission is set to ALL. Values: read, write, create, delete, admin. Optional.
  --aclscheme <value>
        Enable ACL for Exhibitor's internal ZooKeeper connection. This sets the ACL's Scheme. Optional.
  --log-index-directory <value>
        The directory where indexed Zookeeper logs should be kept. Optional.
  --zookeeper-install-directory <value>
        The directory where the Zookeeper server is installed. Optional.
  --zookeeper-data-directory <value>
        The directory where Zookeeper snapshot data is stored. Optional.
  --zookeeper-log-directory <value>
        The directory where Zookeeper transaction log data is stored. Optional.
  --backup-extra <value>
        Backup extra shared config. Optional.
  --zoo-cfg-extra <value>
        Any additional properties to be added to the zoo.cfg file in form: key1\\=value1&key2\\=value2. Optional.
  --java-environment <value>
        Script to write as the 'java.env' file which gets executed as a part of Zookeeper start script. Optional.
  --log4j-properties <value>
        Contents of the log4j.properties file. Optional.
  --client-port <value>
        The port that clients use to connect to Zookeeper. Defaults to 2181. Optional.
  --connect-port <value>
        The port that other Zookeeper instances use to connect to Zookeeper. Defaults to 2888. Optional.
  --election-port <value>
        The port that other Zookeeper instances use for election. Defaults to 3888. Optional.
  --check-ms <value>
        The number of milliseconds between live-ness checks on Zookeeper server. Defaults to 30000. Optional.
  --cleanup-period-ms <value>
        The number of milliseconds between Zookeeper log file cleanups. Defaults to 43200000. Optional.
  --cleanup-max-files <value>
        The max number of Zookeeper log files to keep when cleaning up. Defaults to 3. Optional.
  --backup-max-store-ms <value>
        Backup max store ms shared config. Optional.
  --backup-period-ms <value>
        Backup period ms shared config. Optional.
  --port <value>
        Port ranges to accept, when offer is issued. Optional
```

Starting servers in the cluster
-------------------------------

```
# ./exhibitor-mesos.sh help start
Usage: start <id> [options]

  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
```

Stopping servers in the cluster
-------------------------------

```
# ./exhibitor-mesos.sh help stop
Usage: stop <id> [options]

  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
```

Removing servers from the cluster
----------------------------------

```
# ./exhibitor-mesos.sh help remove
Usage: remove <id> [options]

  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
```