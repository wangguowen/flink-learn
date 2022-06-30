package com.flink.learn.demo.state;

import com.flink.learn.demo.bean.AdLog;
import com.flink.learn.demo.utils.CommonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class StateTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<AdLog> stream = env.readTextFile("E:\\test_data\\adlog.txt")
                .map(x -> new CommonUtil().splitSourceData(x))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AdLog>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((adLog, recordTimestamp) -> adLog.getTimestamp()));


        stream.print("adLog");

        stream
                .keyBy(x -> x.getUserId())
                .process(new MyPeriodicResult())
                .print("result");


        env.execute();
    }

    public static class MyPeriodicResult extends KeyedProcessFunction<String, AdLog, String> {
        private ValueState<Long> ec;
        private ValueState<Long> dc;
        private ValueState<Long> tsState;

        @Override
        public void processElement(AdLog adLog, KeyedProcessFunction<String, AdLog, String>.Context ctx, Collector<String> out) throws Exception {
            Long current_ec = ec.value();
            Long current_dc = dc.value();
            if (current_ec == null) current_ec = 0L;
            if (current_dc == null) current_dc = 0L;

            if (adLog.getMsgType().equals("exposure")) {
                current_ec += 1;
            }

            if (adLog.getMsgType().equals("download")) {
                current_dc += 1;
            }

            ec.update(current_ec);
            dc.update(current_dc);

            if (tsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(adLog.getTimestamp() + 60 * 1000L);
                tsState.update(adLog.getTimestamp() + 60 * 1000L);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ec = getRuntimeContext().getState(new ValueStateDescriptor<Long>("exposure", Long.class));
            dc = getRuntimeContext().getState(new ValueStateDescriptor<Long>("download", Long.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-service", Long.class));
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, AdLog, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "," + ec.value() + "," + dc.value());
            tsState.clear();
            //System.out.println("timestamp : " + timestamp);
            //ctx.timerService().registerEventTimeTimer(timestamp + 60 * 1000L);
            //tsState.update(timestamp + 60 * 1000L);
        }
    }


}
