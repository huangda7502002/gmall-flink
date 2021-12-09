package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyDeserializerFunc implements DebeziumDeserializationSchema<String> {
    /**
     * {
     *      "data": {"id": 11, "tm_name": "sasa"},
     *      "db": "",
     *      "tableName": "",
     *      "op": "c u d",
     *      "ts": ""
     * }
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // 获取主题信息，提取数据库和表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String db = fields[1];
        String tableName = fields[2];

        // 获取value信息，提取数据本身
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject jsonObject = new JSONObject();
        if (after != null) {
            List<Field> fields1 = after.schema().fields();
            for (Field field : fields1) {
                Object o = after.get(field);
                jsonObject.put(field.name(), o);
            }
        }

        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            List<Field> fields1 = before.schema().fields();
            for (Field field : fields1) {
                Object o = before.get(field);
                beforeJson.put(field.name(), o);
            }
        }

        // 获取操作类型
        String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        // 创建结果json
        JSONObject result = new JSONObject();
        result.put("db", db);
        result.put("tableName", tableName);
        result.put("data", jsonObject);
        result.put("before-data", beforeJson);
        result.put("type", type);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
