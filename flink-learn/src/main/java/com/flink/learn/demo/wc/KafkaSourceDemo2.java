package com.flink.learn.demo.wc;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

/**
 * 基于FlinkKafkaConsumer读取kafka
 */
public class KafkaSourceDemo2 {

    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "KafkaSourceTest");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");

        DataStreamSource<String> kafkaDataStreamSource = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));


        kafkaDataStreamSource.print();

        env.execute();
    }
}
