#!/bin/bash

git pull

rm TFIDFMapReduce/*.class
rm TFIDFMapReduce.jar

rm TFIDFSummaryMapReduce/*.class
rm TFIDFSummaryMapReduce.jar

hadoop com.sun.tools.javac.Main *.java

mv TFIDFMapReduce*.class TFIDFMapReduce/

mv TFIDFSummaryMapReduce*.class TFIDFSummaryMapReduce/

jar cf TFIDFMapReduce.jar TFIDFMapReduce/

jar cf TFIDFSummaryMapReduce.jar TFIDFSummaryMapReduce/

hadoop fs -rm -r /PA2/output
hadoop fs -rm -r /PA2/tf_output
hadoop fs -rm -r /PA2/tfidf_output
hadoop fs -rm -r /PA2/summary_output
