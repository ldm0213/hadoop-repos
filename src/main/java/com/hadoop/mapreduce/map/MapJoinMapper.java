package com.hadoop.mapreduce.map;

import com.hadoop.mapreduce.bean.UserInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** 实现Map join的Mapper */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    /** 用户userId->userInfo */
    private Map<String, UserInfo> userInfos = new HashMap<>();

    /**  执行Map方法前会调用一次setup方法，可以用于初始化读取用户信息加载到内存 */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("--------MAP初始化：加载用户信息数据到内存------");
        // 缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(cacheFiles[0]));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line;

        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split(",");
            if (fields != null && fields.length == 2) {
                UserInfo info = new UserInfo(fields[0], fields[1]);
                userInfos.put(fields[0], info);
            }
        }
        br.close();
        System.out.println("--------MAP初始化：共加载了" + userInfos.size() + "条用户信息数据------");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields != null && fields.length == 4) {
            String userId = fields[1];
            UserInfo userInfo = userInfos.get(userId);
            if (userInfo == null) {
                return;
            }
            String result = userId + ";" + userInfo.getUserName();
            context.write(new Text(result), NullWritable.get());
        }
    }
}
