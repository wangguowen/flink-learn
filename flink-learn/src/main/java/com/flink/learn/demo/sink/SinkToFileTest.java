package com.flink.learn.demo.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class SinkToFileTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        DataStreamSource<String> dataStreamSource = env.fromElements("hadoop", "kafka", "spark", "flink", "es", "mysql");

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("E:\\test_data\\SinkToFileTest.txt"),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()  // 设置滚动策略
                                .withInactivityInterval(5000) // 不活动的间隔时间
                                .withRolloverInterval(10000)  // 每隔10s生成1个文件
                                .build()
                )
                .build();

        dataStreamSource.addSink(streamingFileSink);

        env.execute();
    }
}
