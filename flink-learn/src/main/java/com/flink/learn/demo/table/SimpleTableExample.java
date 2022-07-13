package com.flink.learn.demo.table;

import com.flink.learn.demo.bean.AdLog;
import com.flink.learn.demo.utils.CommonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;
import static org.apache.flink.table.api.Expressions.$;

public class SimpleTableExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<AdLog> adLogStream = env.readTextFile("E:\\test_data\\adlog.txt")
                .map(x -> new CommonUtil().splitSourceData(x))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AdLog>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((adLog, recordTimestamp) -> adLog.getTimestamp()));

        Table adLogTable = tableEnv.fromDataStream(adLogStream);

        /*Table resultTable1 = tableEnv.sqlQuery("select * from " + adLogTable);

        Table resultTable2 = adLogTable
                .select($("userId"), $("deviceModel"))
                .where($("deviceModel").isEqual("SM-A5160"));

        tableEnv.toDataStream(resultTable1).print("result1");
        tableEnv.toDataStream(resultTable2).print("result2");*/

        tableEnv.createTemporaryView("adLog", adLogTable);
        Table resultTable = tableEnv.sqlQuery("select userId, msgType, count(msgType) from adLog group by userId,msgType");

        //group by有更新，需要使用toChangelogStream更新日志表
        tableEnv.toChangelogStream(resultTable).print();

        env.execute();
    }
}
