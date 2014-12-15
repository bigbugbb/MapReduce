#!/usr/bin/env bash

javac -cp $HADOOP_PREFIX/hadoop-core-1.2.1.jar:$HADOOP_PREFIX/lib/* -d ./bin ./src/com/binbo/wordcount/WordCount.java
jar -cvf ./wordcount.jar -C ./bin/ .
rm -rf ./output
hadoop jar ./wordcount.jar com.binbo.wordcount.WordCount hw2.txt ./output
