package com.flink.learn.demo.state;

import com.flink.learn.demo.bean.AdLog;
import com.flink.learn.demo.utils.CommonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple3;
import java.time.Duration;


public class StateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<AdLog> streamOperator = env.readTextFile("E:\\test_data\\adlog.txt")
                .map(x -> new CommonUtil().splitSourceData(x))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AdLog>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((adLog, recordTimestamp) -> adLog.getTimestamp()));


        streamOperator
                .keyBy(x -> x.getUserId())
                .map(new MyFunction())
                .print();

        env.execute();
    }

    public static class MyFunction extends RichMapFunction<AdLog, Tuple3<String, Long, Long>> {
        private ValueState<Long> ec;
        private ValueState<Long> dc;

        @Override
        public Tuple3<String, Long, Long> map(AdLog adLog) throws Exception {
            Long currentEc = ec.value();
            Long currentDc = dc.value();
            Long nowEc = 0L;
            Long nowDc = 0L;
            //System.out.println("current ec :" + currentEc);
            //System.out.println("current dc :" + currentDc);
            if (adLog.getMsgType().equals("exposure")) {
                if (null == currentEc) {
                    currentEc = 0L;
                }
                nowEc = currentEc + 1;
                ec.update(nowEc);
            }
            if (adLog.getMsgType().equals("download")) {
                if (null == currentDc) {
                    currentDc = 0L;
                }
                nowDc = currentDc + 1;
                dc.update(nowDc);
            }
            return new Tuple3<>(adLog.getUserId(), nowEc, nowDc);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ec = getRuntimeContext().getState(new ValueStateDescriptor<Long>("exposure", Long.class));
            dc = getRuntimeContext().getState(new ValueStateDescriptor<Long>("download", Long.class));
        }
    }


}
