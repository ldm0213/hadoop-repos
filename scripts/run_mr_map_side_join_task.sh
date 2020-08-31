#!/bin/bash
jar=hadoop-repos-1.0-SNAPSHOT-jar-with-dependencies.jar
userInfoDir=hdfs://hadoop1/user/bigdata/tmp/mr/user_info_file/date=20200831/user_info.txt
userActionDir=hdfs://hadoop1/user/bigdata/tmp/mr/user_behavior_log/date=20200831
outputDir=hdfs://hadoop1/user/bigdata/tmp/mr/map_side_join_result

hadoop jar ${jar} com.hadoop.mapreduce.main.MapJoinMain ${userInfoDir} ${userActionDir} ${outputDir}