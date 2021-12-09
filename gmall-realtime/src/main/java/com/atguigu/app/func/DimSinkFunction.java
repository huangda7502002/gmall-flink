package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //准备写入数据的sql
        JSONObject data = value.getJSONObject("data");
        String sinkTable = value.getString("sinkTable");
        JSONObject after = value.getJSONObject("data");
        String upsertSql = genUpsertSql(data, sinkTable);
        System.out.println("插入数据sql：" + upsertSql);
        // 预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(upsertSql);

        // 如果判断当前数据为更新操作，则先删除Redis中的数据
        if ("update".equals(value.getString("type"))) {
            DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
        }

        // 执行插入操作
        preparedStatement.execute();
        connection.commit();
    }

    private String genUpsertSql(JSONObject data, String sinkTable) {
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "."
                + sinkTable + "(" + StringUtils.join(keySet, ",")  + ")"
                + "values ( '" + StringUtils.join(values, "','") + "' )";
    }

}
