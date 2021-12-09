package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.MyDeserializerFunc;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(100000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop200:8020/gmall-flink-201109/ck"));
//        System.setProperty("HADOOP_USER_NAME", "root");

        // 2. 读取kafka ods_base_db 主题的数据
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));
        System.out.println(kafkaDS);
        // 3. 将每行数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        // 4. 过滤空值数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(jsonObject -> {
            String data = jsonObject.getString("data");
            return data != null && data.length() > 0;
        });
        // 5. 使用flinkCDC读取配置表并创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop200")
                .port(3306)
                .username("root")
                .password("Hd7502002$")
                .databaseList("gmall-realtime-201109")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializerFunc())
                .build();
        DataStreamSource<String> tableProcess = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("bc-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcess.broadcast(mapStateDescriptor);
        System.out.println(filterDS);
        // 6. 链接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);
        // 7. 处理广播流数据，发送至主流，主流根据广播流的数据进行处理自身数据
        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<JSONObject>("hbase") {};
        SingleOutputStreamOperator<JSONObject> kafkaFactDS = connectedStream.process(new TableProcessFunction(hbaseOutputTag, mapStateDescriptor));
        // 8. 将HBase流写入HBase
        kafkaFactDS.getSideOutput(hbaseOutputTag).addSink(new DimSinkFunction());
        // 9. 将kafka流写入kafka
        kafkaFactDS.addSink(MyKafkaUtil.getFlinkKafkaProducerBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"), jsonObject.getString("data").getBytes(StandardCharsets.UTF_8));
            }
        }));
        // 10. 启动
        env.execute();
    }
}
