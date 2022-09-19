package com.flink.learn.demo.wc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基于KafkaSource连接kafka（新版本）
 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("test")
                .setGroupId("KafkaSourceDemo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source"); //  接收的是 Source 接口的实现类

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamSource.flatMap(
                        (FlatMapFunction<String, Tuple2<String, Integer>>) (line, out) -> {
                            String[] words = line.split(" ");
                            for (String word : words) {
                                out.collect(new Tuple2(word, 1));
                            }
                        })
                .keyBy(0)
                .sum(1);

        //streamSource.print();
        sum.print();

        env.execute();
    }
}
