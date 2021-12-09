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
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    //定义Phoenix连接
    private Connection connection;

    //定义侧输出流标签
    private OutputTag<JSONObject> objectOutputTag;

    //定义Map状态表述器
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction() {
    }

    public TableProcessFunction(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //处理广播过来的数据
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        //1.将数据转换为JavaBean value:{"database":"gmall-","table":"table_process","type":"insert",data":{"":""},"before-data":{"":""}}
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject data = jsonObject.getJSONObject("data");
        TableProcess tableProcess = JSON.parseObject(data.toJSONString(), TableProcess.class);

        if (tableProcess != null) {

            //2.校验表是否存在,如果不存在,则创建Phoenix表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                checkTable(tableProcess.getSinkTable(),
                        tableProcess.getSinkColumns(),
                        tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend());
            }

            //3.将数据写入状态广播处理
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
            broadcastState.put(key, tableProcess);
        }

    }

    //处理主流数据  jsonObject:{"database":"gmall-","table":"base_trademark","type":"insert",data":{"":""},"before-data":{"":""}}
    @Override
    public void processElement(JSONObject jsonObject, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //1.提取状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + ":" + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {

            //2.根据配置信息过滤字段
            JSONObject data = jsonObject.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data, sinkColumns);

            //3.分流,将HBase数据写入侧输出流,Kafka数据写入主流
            String sinkType = tableProcess.getSinkType();

            //将待写入的维度表或者主题名添加至数据中,方便后续操作
            jsonObject.put("sinkTable", tableProcess.getSinkTable());

            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                ctx.output(objectOutputTag, jsonObject);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                out.collect(jsonObject);
            }

        } else {
            System.out.println("配置信息中不存在Key：" + key);
        }
    }

    /**
     * 校验表是否存在,如果不存在,则创建Phoenix表
     * <p>
     * create table if not exists xx.xx(id varchar primary key,name varchar,sex varchar) xxx
     *
     * @param sinkTable   表名
     * @param sinkColumns 列信息
     * @param sinkPk      主键
     * @param sinkExtend  扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        //处理主键以及扩展字段
        if (sinkPk == null || sinkPk.equals("")) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        //创建建表SQL
        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        //将建表字段拆开
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {

            String column = columns[i];

            //如果当前字段为主键
            if (sinkPk.equals(column)) {
                createTableSQL.append(column).append(" varchar ").append("primary key");
            } else {
                createTableSQL.append(column).append(" varchar ");
            }
            //如果当前字段不是最后一个字段
            if (i < columns.length - 1) {
                createTableSQL.append(",");
            }
        }

        //拼接扩展字段
        createTableSQL.append(")").append(sinkExtend);

        //打印SQL
        String sql = createTableSQL.toString();
        System.out.println(sql);

        //执行SQL建表
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 根据配置信息过滤字段
     *
     * @param data        待过滤的数据
     * @param sinkColumns 目标字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //1.处理目标字段
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        //2.遍历data
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }
}
