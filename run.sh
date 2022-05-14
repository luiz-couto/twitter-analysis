#!/bin/sh

rm -r out
export HADOOP_CP="$(hadoop classpath)"
javac --release 8 -cp "${HADOOP_CP}:/home/luizcouto/School/cloud-computing/twitter-analysis/lib/org/*" -d classes src/part_one/TwitterMapReduce.java
jar -cvf src/part_one/TwitterMapReduce.jar -C classes/ .
hadoop jar src/part_one/TwitterMapReduce.jar part_one.TwitterMapReduce database/1.json out