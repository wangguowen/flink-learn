package com.flink.learn.demo.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaTableDemo {

    public static StreamTableEnvironment tableEnv = null;
    public static StreamExecutionEnvironment env = null;

    static{
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(50000L);
        tableEnv = StreamTableEnvironment.create(env);
    }

    public static void main(String[] args) throws Exception {
        String createSQL = "CREATE TABLE kafka_table (\n" +
                "          topic VARCHAR METADATA FROM 'topic',\n" +
                "          `offset` bigint METADATA,\n" +
                "          rowtime TIMESTAMP(3),\n" +
                "          msg VARCHAR,\n" +
                "          uid VARCHAR,\n" +
                "          proctime AS PROCTIME(),\n" +
                "          WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND\n" +
                "      ) WITH (\n" +
                "          'connector' = 'kafka',\n" +
                "          'topic' = 'test',\n" +
                "          'scan.startup.mode' = 'latest-offset',\n" +
                "          'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "          'properties.group.id' = 'test',\n" +
                "          'format' = 'json',\n" +
                "          'json.ignore-parse-errors' = 'true')\n";
        System.out.println(createSQL);
        tableEnv.executeSql(createSQL);

        tableEnv.toRetractStream(tableEnv.sqlQuery("select msg,count(1) from kafka_table group by msg"), Row.class)
                .print();

        env.execute();

    }
}
