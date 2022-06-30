package com.flink.learn.demo.state;

import com.flink.learn.demo.bean.AdLog;
import com.flink.learn.demo.utils.CommonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<AdLog> stream = env.readTextFile("E:\\test_data\\adlog.txt")
                .map(x -> new CommonUtil().splitSourceData(x))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AdLog>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((adLog, recordTimestamp) -> adLog.getTimestamp()));


        stream.print("input");

        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<AdLog>, CheckpointedFunction {

        private final int size;

        private List<AdLog> bufferedElements;

        private ListState<AdLog> checkpointState;

        public BufferingSink(int size) {
            this.size = size;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(AdLog value, Context context) throws Exception {
            bufferedElements.add(value);

            if (bufferedElements.size() == size) {
                for (AdLog adLog : bufferedElements) {
                    System.out.println(adLog);
                }

                System.out.println("========= 输出完毕 ==========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //清空状态
            checkpointState.clear();

            for (AdLog adLog : bufferedElements) {
                checkpointState.add(adLog);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<AdLog> descriptor = new ListStateDescriptor<>("buffered-elements", AdLog.class);

            checkpointState = context.getOperatorStateStore().getListState(descriptor);

            //如果从故障上恢复
            if (context.isRestored()) {
                for (AdLog adlog : checkpointState.get()) {
                    bufferedElements.add(adlog);
                }
            }
        }
    }
}
