package com.flink.learn.demo.steam;

import com.flink.learn.demo.bean.AdLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class SplitStreamTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\test_data\\adlog.txt");

        SingleOutputStreamOperator<AdLog> adLogStream = dataStreamSource
                .map((MapFunction<String, AdLog>) value -> {
                    AdLog adlog = splitSourceData(value);
                    return adlog;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<AdLog>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((adLog, recordTimestamp) -> adLog.getTimestamp()));

        OutputTag<Tuple3<String, String, Long>> exposureTag = new OutputTag<Tuple3<String, String, Long>>("exposure") {
        };
        OutputTag<Tuple3<String, String, Long>> downloadTag = new OutputTag<Tuple3<String, String, Long>>("download") {
        };

        SingleOutputStreamOperator<Object> processedStream = adLogStream.process(new ProcessFunction<AdLog, Object>() {
            @Override
            public void processElement(AdLog adLog, Context cxt, Collector<Object> out) {
                if (adLog.getMsgType().equals("download")) {
                    System.out.println("水位线 ：" + cxt.timerService().currentWatermark());
                    cxt.output(downloadTag, Tuple3.of(adLog.getUserId(), adLog.getDeviceModel(), adLog.getTimestamp()));
                } else if (adLog.getMsgType().equals("exposure")) {
                    System.out.println("水位线 ：" + cxt.timerService().currentWatermark());
                    cxt.output(exposureTag, Tuple3.of(adLog.getUserId(), adLog.getDeviceModel(), adLog.getTimestamp()));
                } else {
                    System.out.println("水位线 ：" + cxt.timerService().currentWatermark());
                    out.collect(adLog);
                }
            }
        });

        processedStream.print("other");
        processedStream.getSideOutput(downloadTag).print("download");
        processedStream.getSideOutput(exposureTag).print("exposure");
//        adLogStream.map(x -> x.toString()).print();

        env.execute();


    }

    public static AdLog splitSourceData(String line) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        AdLog adLog = new AdLog();
        String[] arr = line.split(",");
        String userId = arr[1];
        int positionId = Integer.valueOf(arr[3]);
        Long timestamp = Long.valueOf(arr[4]);
        String time = sdf.format(new Date(timestamp));
        String msgType = arr[5]; //6
        String packageName = arr[6]; //7
        String net = arr[9]; //10
        String deviceModel = arr[10]; //11
        String ip = arr[11]; //12
        adLog.setUserId(userId);
        adLog.setPositionId(positionId);
        adLog.setTimestamp(timestamp);
        adLog.setPackageName(packageName);
        adLog.setMsgType(msgType);
        adLog.setDeviceModel(deviceModel);
        adLog.setIp(ip);
        adLog.setNet(net);
        adLog.setTime(time);
        return adLog;
    }
}
