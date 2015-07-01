Exhibitor Mesos Framework
======================

This is still being developed actively and is not yet alpha. [Open issues here](https://github.com/CiscoCloud/exhibitor-mesos-framework/issues).

[Prerequisites](#prerequisites)  
* [Environment Configuration](#environment-configuration)  
* [Scheduler Configuration](#scheduler-configuration)  
* [Run the scheduler](#run-the-scheduler)  
* [Quick start](#quick-start)  

[Typical Operations](#typical-operations)  
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
Server added

server:
  id: 0
  state: Added
  exhibitor config:
  shared config overrides:
  cpu: 0.2
  mem: 256.0
```

You now have a cluster with 1 server that is not started.

```
# ./exhibitor-mesos.sh status
cluster:
  server:
    id: 0
    state: Added
    exhibitor config:
    shared config overrides:
    cpu: 0.2
    mem: 256.0
```

Each server requires some basic configuration.

```
# ./exhibitor-mesos.sh config 0 --configtype zookeeper --zkconfigconnect 192.168.3.1:2181 --zkconfigzpath /exhibitor/config --zookeeper-install-directory /tmp/zookeeper --zookeeper-data-directory /tmp/zkdata
server:
  id: 0
  state: Added
  exhibitor config:
    zkconfigzpath: /exhibitor/config
    zkconfigconnect: 192.168.3.1:2181
    configtype: zookeeper
  shared config overrides:
    zookeeper-install-directory: /tmp/zookeeper
    zookeeper-data-directory: /tmp/zkdata
  cpu: 0.2
  mem: 256.0
```

Now lets start the server. The state should change from `Added` to `Stopped` meaning the task is waiting for resources to be offered.

```
# ./exhibitor-mesos.sh start 0
Started server

server:
  id: 0
  state: Stopped
  exhibitor config:
    zkconfigzpath: /exhibitor/config
    zkconfigconnect: 192.168.3.1:2181
    configtype: zookeeper
  shared config overrides:
    zookeeper-install-directory: /tmp/zookeeper
    zookeeper-data-directory: /tmp/zkdata
  cpu: 0.2
  mem: 256.0
```

Now as we don't know where the server we may ask for the cluster status to see where the endpoint is.

```
# ./exhibitor-mesos.sh status
cluster:
  server:
    id: 0
    state: Running
    endpoint: http://slave0:31000/exhibitor/v1/ui/index.html
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
```

By now you should have a single Exhibitor instance running. Here's how you stop it:

```
# ./exhibitor-mesos.sh stop 0
Stopped server 0
```

If you want to remove the server from the cluster completely you may skip `stop` step and call `remove` directly (this will call `stop` under the hood anyway):

```
# ./exhibitor-mesos.sh remove 0
Removed server 0
```

Typical Operations
===================

Changing the location of Zookeeper data
---------------------------------------

```
# ./exhibitor-mesos.sh stop 0
Stopped server 0

# ./exhibitor-mesos.sh config 0 --zookeeper-data-directory /tmp/exhibitor_zkdata
server:
  id: 0
  state: Added
  exhibitor config:
    zkconfigzpath: /exhibitor/config
    zkconfigconnect: 192.168.3.1:2181
    configtype: zookeeper
  shared config overrides:
    zookeeper-install-directory: /tmp/zookeeper
    zookeeper-data-directory: /tmp/exhibitor_zkdata
  cpu: 0.2
  mem: 256.0
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
  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
```

Configuring servers in the cluster
----------------------

**NOTE**: this section is far from being final and more configurations will appear soon. Please don't panic if something is missing, it will be added soon.

```
# ./exhibitor-mesos.sh help config
Usage: config <id> [options]

  -a <value> | --api <value>
        Binding host:port for http/artifact server. Optional if EM_API env is set.
  --configtype <value>
        Config type to use: s3 or zookeeper. Optional.
  --zkconfigconnect <value>
        The initial connection string for ZooKeeper shared config storage. E.g: host1:2181,host2:2181... Optional.
  --zkconfigzpath <value>
        The base ZPath that Exhibitor should use. E.g: /exhibitor/config. Optional.
  --s3credentials <value>
        Credentials to use for s3backup or s3config. Optional.
  --s3region <value>
        Region for S3 calls (e.g. "eu-west-1"). Optional.
  --s3config <value>
        The bucket name and key to store the config (s3credentials may be provided as well). Argument is [bucket name]:[key]. Optional.
  --s3configprefix <value>
        When using AWS S3 shared config files, the prefix to use for values such as locks. Optional.
  --zookeeper-install-directory <value>
        Zookeeper install directory shared config. Optional.
  --zookeeper-data-directory <value>
        Zookeeper data directory shared config. Optional.
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