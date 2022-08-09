package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class MyPhoenixSink implements SinkFunction<JSONObject> {

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 清除 JSON 对象中的 sinkTable 字段，以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");
        PhoenixUtil.insertValues(sinkTable, jsonObj);
    }
}

