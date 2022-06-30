package com.flink.learn.demo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class BoundStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "C:\\Users\\Administrator\\Desktop\\test\\sample_data.csv";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = line.split(",");
                        for (String word : words) {
                            out.collect(new Tuple2(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);

        resultStream.print();

        env.execute();
    }
}
