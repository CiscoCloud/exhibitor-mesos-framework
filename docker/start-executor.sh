#!/bin/bash

export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so
export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so
java $JAVA_OPTS -Djava.library.path=/usr/local/lib -cp exhibitor-mesos.jar net.elodina.mesos.exhibitor.Executor $@