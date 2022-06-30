package com.flink.learn.demo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "C:\\Users\\Administrator\\Desktop\\test\\sample_data.csv";
        DataSet<String> dataSet = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> resultSet = dataSet
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, out) -> {
                    String[] words = line.split(",");
                    for (String word : words) {
                        out.collect(new Tuple2(word, 1));
                    }
                })
                .groupBy(0)
                .sum(1);

        resultSet.print();
    }

}
