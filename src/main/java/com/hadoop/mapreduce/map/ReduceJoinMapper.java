package com.hadoop.mapreduce.map;

import com.hadoop.mapreduce.enums.FileRecorder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/** 实现Reduce join的Mapper */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields != null && fields.length == 4) { // user action log
            String userId = fields[0];
            String itemId = fields[1];
            String categoryId = fields[2];
            String behavior = fields[3];
            // 添加tag标识
            context.write(new Text(userId), new Text("2;" + itemId + ";" + categoryId + ";" + behavior));
            context.getCounter(FileRecorder.UserActionMapRecorder).increment(1);
        } else if (fields != null && fields.length == 2) { // user info
            String userId = fields[0];
            String userName = fields[1];
            context.write(new Text(userId), new Text("1;" + userName));
            context.getCounter(FileRecorder.UserInfoMapRecorder).increment(1);
        }
    }
}
