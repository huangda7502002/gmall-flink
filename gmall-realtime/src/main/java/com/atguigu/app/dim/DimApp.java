package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public class DimApp {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://ambari01:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        //TODO 2.读取Kafka topic_db 主题数据创建流
        String topic = "topic_db";
        String groupId = "app_211126";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将数据转换为JSON对象并过滤   主流
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        jsonObjDS.getSideOutput(dirtyTag).print("Dirty>>>>>>");

        //TODO 4.使用FlinkCDC读取配置信息表创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("ambari01")
                .port(3306)
                .username("root")
                .password("Hd7502002")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");
        mysqlDS.print();

        //TODO 5.将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(stateDescriptor);

        //TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 7.根据广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(stateDescriptor));
        hbaseDS.print(">>>>>>>>");

        //TODO 8.将数据写出到Phoenix中
        hbaseDS.addSink(new DimSinkFunction());

        //TODO 9.启动任务
        env.execute();
    }
}
