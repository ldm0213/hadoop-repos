package com.hadoop.mapreduce.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text,
        Text, IntWritable> {
    public void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] array = line.split(" ");
        for (String item: line.split(" ")) {
            context.write(new Text(item), new IntWritable(1));
        }
    }
}
