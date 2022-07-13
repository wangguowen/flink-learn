package com.flink.learn.demo.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class TimeWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String tableDDL = "CREATE TABLE user_log( " +
                " user_name STRING," +
                " url STRING," +
                " visit_count BIGINT," +
                " ts BIGINT," + //13位毫秒级时间戳
                " data_time STRING," +
                " et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss'))," + //水位线列的类型必须为TIMESTAMP(3)
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector'='filesystem'," +
                " 'path'='E:\\test_data\\user_log.txt'," +
                " 'format'='csv'" +
                ")";

        tableEnv.executeSql(tableDDL);

        //累积窗口
        Table cumulateResultTable = tableEnv.sqlQuery("select url, count(1) as cnt," +
                " window_start as startT," +
                " window_end as endT " +
                "from TABLE( " +
                " CUMULATE(TABLE user_log, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '20' SECOND)) " +
                "GROUP BY url, window_start, window_end"
        );

        //tableEnv.toChangelogStream(cumulateResultTable).print("cumulate window");
        //tableEnv.toDataStream(cumulateResultTable).print("cumulate window");  //没有更新，所以可以用toDataStream

        //开窗函数
        Table overWindowResultTable = tableEnv.sqlQuery("select url, " +
                " avg(visit_count) OVER( " +
                " PARTITION BY url " +
                " ORDER BY et " +
                " ROWS BETWEEN 5 PRECEDING AND CURRENT ROW" +
                ") AS avg_visit_count " +
                "FROM user_log"
        );

        //tableEnv.toDataStream(overWindowResultTable).print("over window");


        //普通的topN，求所有用户访问量最高的前三位
        Table topNResultTable = tableEnv.sqlQuery("select user_name, cnt, row_num " +
                "FROM (" +
                "  SELECT *, ROW_NUMBER() OVER(" +
                "      ORDER BY cnt DESC" +
                "    ) AS row_num " +
                "  FROM (select user_name, sum(visit_count) as cnt FROM user_log GROUP BY user_name )" +
                ") WHERE row_num <= 3"
        );

        //tableEnv.toChangelogStream(topNResultTable).print("top n"); //这里不能用toDataStream，row_num会更改


        //窗口topN，统计一段时间内访问量前N位用户
        String subQuery = "select user_name, " +
                "  sum(visit_count) as cnt," +
                "  window_start," +
                "  window_end " +
                "FROM TABLE( " +
                "  TUMBLE(TABLE user_log, DESCRIPTOR(et), INTERVAL '5' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end";

        Table topNWindowResultTable = tableEnv.sqlQuery("select user_name, cnt, row_num, window_start, window_end " +
                "FROM (" +
                "  SELECT *, ROW_NUMBER() OVER(" +
                "      PARTITION BY window_start, window_end " +
                "      ORDER BY cnt DESC" +
                "    ) AS row_num " +
                "  FROM (" + subQuery + ")" +
                ") WHERE row_num <= 3"
        );

        tableEnv.toDataStream(topNWindowResultTable).print("window topN");

        env.execute();

    }


}
