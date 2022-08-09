package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;


public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> tableConfigDescriptor;
    // 定义Phoenix的连接
    private Connection conn;

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableConfigDescriptor) {
        this.tableConfigDescriptor = tableConfigDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> tableConfigState = readOnlyContext.getBroadcastState(tableConfigDescriptor);
        // 获取配置信息
        String sourceTable = jsonObj.getString("table");
        TableProcess tableConfig = tableConfigState.get(sourceTable);
        if (tableConfig != null) {
            JSONObject data = jsonObj.getJSONObject("data");
            String sinkTable = tableConfig.getSinkTable();

            // 根据 sinkColumns 过滤数据
            String sinkColumns = tableConfig.getSinkColumns();
            filterColumns(data, sinkColumns);

            // 将目标表名加入到主流数据中
            data.put("sinkTable", sinkTable);

            out.collect(data);
        }

    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        Set<Map.Entry<String, Object>> dataEntries = data.entrySet();
        dataEntries.removeIf(r -> !sinkColumns.contains(r.getKey()));
    }

    @Override
    public void processBroadcastElement(String jsonStr, Context context, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        TableProcess config = jsonObj.getObject("after", TableProcess.class);

        String sourceTable = config.getSourceTable();
        String sinkTable = config.getSinkTable();
        String sinkColumns = config.getSinkColumns();
        String sinkPk = config.getSinkPk();
        String sinkExtend = config.getSinkExtend();

        BroadcastState<String, TableProcess> tableConfigState = context.getBroadcastState(tableConfigDescriptor);
        tableConfigState.put(sourceTable, config);

        checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        // 为主键及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();
        // 为数据库操作对象赋默认值，执行建表 SQL
        PreparedStatement preparedSt = null;
        try {
            preparedSt = conn.prepareStatement(createStatement);
            preparedSt.execute();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("建表语句\n" + createStatement + "\n执行异常");
        } finally {
            if (preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }

}
