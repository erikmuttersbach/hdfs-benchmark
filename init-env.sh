#!/bin/bash
for file in `find $HADOOP_HOME/ -name *.jar`; do export CLASSPATH=$CLASSPATH:$file; done
export LD_LIBRARY_PATH="$JAVA_HOME/jre/lib/amd64/server/"
export LIBHDFS_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
