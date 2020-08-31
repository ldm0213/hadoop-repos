package com.hadoop.mapreduce.main;

import com.hadoop.mapreduce.enums.FileRecorder;
import com.hadoop.mapreduce.map.ReduceJoinMapper;
import com.hadoop.mapreduce.reduce.ReduceJoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Reducejoin是表连接的基本范式
 * Map处理为key-value格式，并添加标记
 * Reduce处理相同key的数据，进行合并输出
 */
public class ReduceJoinMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("please input 3 params: user_info_File log_user_action_file output_mapjoin directory");
            System.exit(0);
        }
        String userInfoDir = args[0];
        String userLogDir = args[1];
        String output = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        /** 检查输入是否有数据 **/
        if (!fs.exists(new Path(userInfoDir))) {
            System.err.println("not found File: " + userInfoDir);
            System.exit(0);
        }

        if (!fs.exists(new Path(userLogDir))) {
            System.err.println("not found File: " + userLogDir);
            System.exit(0);
        }

        /** 删除结果目录中数据 **/
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        /** Job信息配置 **/
        Job job = new Job(conf, "reduce-side-join-task");
        job.setJarByClass(ReduceJoinMain.class);

        /** 指定Mapper相关参数 **/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(userInfoDir), TextInputFormat.class, ReduceJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(userLogDir), TextInputFormat.class, ReduceJoinMapper.class);

        /** 指定Reducer相关参数 **/
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
