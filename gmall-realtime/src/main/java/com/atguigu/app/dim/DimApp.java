package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.MyBroadcastFunction;
import com.atguigu.app.func.MyPhoenixSink;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
import org.apache.flink.util.Collector;

public class DimApp {
    public static void main(String[] args) {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://ambari01:8020/flinkCK/gmall"));
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        String topic = "topic_db";
        String groupId = "dim_app_211126";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("发现脏数据: " + s);
                }
            }
        });

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder().hostname("ambari01")
                .port(3306)
                .username("root")
                .password("Hd7502002")
                .databaseList("gmall")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 6.2 封装为流
        DataStreamSource<String> mysqlDSSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // 6.3 广播配置流
        MapStateDescriptor<String, TableProcess> tableConfigDescriptor = new MapStateDescriptor<String, TableProcess>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDSSource.broadcast(tableConfigDescriptor);

        // TODO 7. 连接流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastDS);


        SingleOutputStreamOperator<JSONObject> dimDS = connectedStream.process(
                new MyBroadcastFunction(tableConfigDescriptor)
        );

        // TODO 9. 将数据写入 Phoenix 表
        dimDS.addSink(new MyPhoenixSink());

    }
}
