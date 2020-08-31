package com.hadoop.mapreduce.reduce;

import com.hadoop.mapreduce.bean.UserActionInfo;
import com.hadoop.mapreduce.enums.FileRecorder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoinReducer extends Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context
    ) throws IOException, InterruptedException {
        String userId = key.toString();
        String userName = "";
        List<UserActionInfo> userActionInfos = new ArrayList<>();

        for (Text value : values) {
            String[] items = value.toString().split(";");
            if (items[0].equals("2")) { // 标记为1的是用户行为信息
                UserActionInfo userActionInfo = new UserActionInfo(userId, items[1], items[2], items[3]);
                userActionInfos.add(userActionInfo);
                context.getCounter(FileRecorder.UserActionReduceRecorder).increment(1);
            } else if (items[0].equals("1")) {
                userName = items[1];
                context.getCounter(FileRecorder.UserInfoReduceRecoreder).increment(1);
            }
        }

        for (UserActionInfo userActionInfo: userActionInfos) {
            context.write(NullWritable.get(), new Text(userActionInfo.getUserId() + ";" +
                    userName  + ";" + userActionInfo.getItemId()));
        }
    }
}