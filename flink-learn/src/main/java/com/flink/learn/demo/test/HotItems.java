package com.flink.learn.demo.test;

import com.flink.learn.demo.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.time.Duration;
import java.util.Date;

/**
 * 每隔5分钟输出最近一小时内点击量最多的前 N 个商品(API实现)
 */
public class HotItems {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> dataStream = env.readTextFile("E:\\test_data\\UserBehavior.csv")
                .map(x -> getUserBehaviorData(x))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000;
                            }
                        }));

    }

    public static UserBehavior getUserBehaviorData(String line) {
        UserBehavior user = new UserBehavior();
        String[] arr = line.split(",");
        long userId = Long.valueOf(arr[0]);
        long itemId = Long.valueOf(arr[1]);
        int categoryId = Integer.valueOf(arr[2]);
        String behavior = arr[3];
        long timestamp = Long.valueOf(arr[4]);
        user.setUserId(userId);
        user.setItemId(itemId);
        user.setCategoryId(categoryId);
        user.setTimestamp(timestamp);
        user.setBehavior(behavior);
        return user;
    }

}


