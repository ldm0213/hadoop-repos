package com.hadoop.mapreduce.main;

import com.hadoop.mapreduce.map.MapJoinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Mapjoin是将小表先放入分布式缓存中，在Map开始前先加载到本地，在Mapper中进行Join操作
 * 可以大大缩减Join执行时间
 */
public class MapJoinMain {
    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.err.println("please input 3 params: user_info_File log_user_action_file output_mapjoin directory");
            System.exit(0);
        }
        String userInfoDir = args[0];
        String userLogDir = args[1];
        String output = args[2];
        System.setProperty("user.info.dir", userInfoDir);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        /** 检查输入是否有数据 **/
        if(!fs.exists(new Path(userInfoDir))){
            System.err.println("not found File: " + userInfoDir);
            System.exit(0);
        }

        if(!fs.exists(new Path(userLogDir))){
            System.err.println("not found File: " + userLogDir);
            System.exit(0);
        }

        /** 删除结果目录中数据 **/
        Path outputPath = new Path(output);
        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        /** Job信息配置 **/
        Job job = new Job(conf, "map-side-join-task");
        job.setJarByClass(MapJoinMain.class);

        /** 指定Mapper相关参数 **/
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        /** 输入输出路径指定 **/
        FileInputFormat.setInputPaths(job, new Path(userLogDir));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);
        job.addCacheFile(new Path(userInfoDir).toUri());
        job.waitForCompletion(true);
    }
}
