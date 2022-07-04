package com.flink.learn.demo.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FileTableDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE carInfoTable( " +
                "id BIGINT," +
                "carLevel STRING," +
                "prince Double," +
                "country STRING," +
                "producer STRING," +
                "seat STRING," +
                "maintainCount BIGINT," +
                "age BIGINT," +
                "drivingYears BIGINT," +
                "area STRING," +
                "sex STRING," +
                "isOrder STRING" +
                ") WITH (" +
                " 'connector'='filesystem'," +
                " 'path'='E:\\test_data\\sample_data.csv'," +
                " 'format'='csv'" +
                ")";

        tableEnv.executeSql(sql);

        Table table = tableEnv.sqlQuery("select id,carLevel,prince,seat,area from  carInfoTable");

        //tableEnv.toDataStream(table).print();

        String outputSql = "CREATE TABLE outCarInfoTable( " +
                "id BIGINT," +
                "carLevel STRING," +
                "prince Double," +
                "seat STRING," +
                "area STRING" +
                ") WITH (" +
                " 'connector'='filesystem'," +
                " 'path'='E:\\test_data\\sample_data_ount.csv'," +
                " 'format'='csv'" +
                ")";

        tableEnv.executeSql(outputSql);
        table.executeInsert("outCarInfoTable");

        //env.execute();

    }


}
