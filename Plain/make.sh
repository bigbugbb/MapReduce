#!/usr/bin/env bash

javac -cp ./opencsv-3.0.jar:$HADOOP_PREFIX/hadoop-core-1.2.1.jar:$HADOOP_PREFIX/lib/* -d ./bin ./src/com/binbo/plain/Plain.java
jar -cvf ./plain.jar -C ./bin/ .
rm -rf ./temp
rm -rf ./output
hadoop jar ./plain.jar com.binbo.plain.Plain data.csv temp output
