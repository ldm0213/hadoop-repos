#!/bin/bash
jar=hadoop-repos-1.0-SNAPSHOT-jar-with-dependencies.jar
userInfoDir=hdfs://hadoop1/user/bigdata/tmp/mr/user_info/date=20200831
userActionDir=hdfs://hadoop1/user/bigdata/tmp/mr/user_behavior_log/date=20200831
outputDir=hdfs://hadoop1/user/bigdata/tmp/mr/reduce_side_join_result

hadoop jar ${jar} com.hadoop.mapreduce.main.ReduceJoinMain ${userInfoDir} ${userActionDir} ${outputDir}