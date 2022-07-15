package com.flink.learn.demo.cep;

import com.flink.learn.demo.bean.Tickets;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 抢票成功失败提示示例
 */
public class SnapUpTicketsExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tickets> ticketsEventStream = env.fromElements(
                        new Tickets("小明", 555, 5, "fail", "2022-07-14 15:47:42", 1657784862000L),
                        new Tickets("小明", 555, 5, "fail", "2022-07-14 15:47:45", 1657784865000L),
                        new Tickets("楠楠", 555, 5, "fail", "2022-07-14 15:47:50", 1657784870000L),
                        new Tickets("楠楠", 555, 5, "fail", "2022-07-14 15:47:51", 1657784871000L),
                        new Tickets("楠楠", 555, 5, "fail", "2022-07-14 15:47:52", 1657784872000L),
                        new Tickets("小明", 555, 5, "fail", "2022-07-14 15:47:55", 1657784875000L),
                        new Tickets("小明", 555, 5, "success", "2022-07-14 15:47:49", 1657784869000L)
                        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tickets>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((tickets, recordTimestamp) -> tickets.getBuy_ts()));


        //定义Pattern：连续三次抢票失败
        Pattern<Tickets, Tickets> ticketsPattern = Pattern.<Tickets>begin("first")
                .where(new SimpleCondition<Tickets>() {
                    @Override
                    public boolean filter(Tickets value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                })
                .next("second")
                .where(new SimpleCondition<Tickets>() {
                    @Override
                    public boolean filter(Tickets value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                })
                .next("third")
                .where(new SimpleCondition<Tickets>() {
                    @Override
                    public boolean filter(Tickets value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                });

        //将定义Pattern应用到数据流，检测事件
        PatternStream<Tickets> ticketsPatternStream = CEP.pattern(ticketsEventStream.keyBy(x -> x.getBuy_user_name()), ticketsPattern);  //这里数据流需要按照用户名分组

        //将检测到的事件输出
        SingleOutputStreamOperator<Object> failStream = ticketsPatternStream.select(new PatternSelectFunction<Tickets, Object>() {
            @Override
            public Object select(Map<String, List<Tickets>> map) throws Exception {
                Tickets first = map.get("first").get(0);
                Tickets second = map.get("second").get(0);
                Tickets third = map.get("third").get(0);
                return first.getBuy_user_name() + "，连续三次抢票失败，抢票时间 ：" +
                        first.getBuy_date() + "," +
                        second.getBuy_date() + "," +
                        third.getBuy_date();
            }
        });

        failStream.print();

        env.execute();


    }


}
