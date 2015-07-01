#!/bin/bash

if [ -z "$HADOOP_HOME" ]; then
    echo "Set HADOOP_HOME"
    exit 1
fi

if [ -z "$JAVA_HOME" ]; then
    echo "Set JAVA_HOME"
    exit 1
fi

for file in `find $HADOOP_HOME/ -name *.jar`; do export CLASSPATH=$CLASSPATH:$file; done
export LD_LIBRARY_PATH="$JAVA_HOME/jre/lib/amd64/server/"
export LIBHDFS_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
