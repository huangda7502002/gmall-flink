package com.atguigu;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserializerFunc;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_DataStream {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(100000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop200:8020/gmall-flink-201109/ck"));
        System.setProperty("HADOOP_USER_NAME", "root");

        // 使用cdc的方式读取mysql变化的数据
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop200")
                .port(3306)
                .username("root")
                .password("Hd7502002$")
                .databaseList("gmall-realtime-201109")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializerFunc())
                .build();
        // 打印
        env.addSource(mysqlSource).print();
        // 启动
        env.execute();
    }
}
