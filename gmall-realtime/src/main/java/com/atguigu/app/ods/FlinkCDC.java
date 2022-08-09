package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserializerFunc;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
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
        // 2. 使用cdc作为source 读取mysql变化数据
        DebeziumSourceFunction<String> debeziumSourceFunction = MySQLSource
                .<String>builder()
                .hostname("ambari01")
                .port(3306)
                .username("root")
                .password("Hd7502002")
                .databaseList("gmall-flink-201109")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserializerFunc())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(debeziumSourceFunction);

        // 3. 将数据写入kafka
        String topic = "ods_base_db";
        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer(topic));
        env.execute();
    }
}
