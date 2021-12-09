package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class FlinkDataStreamJoinTest {
    public static void main(String[] args) throws Exception {
        // 1. 获取执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        // 2. 读取两个端口数据创建流并提取时间戳生成watermark
        SingleOutputStreamOperator<Bean1> stream1 = executionEnvironment.socketTextStream("hadoop200", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                                    @Override
                                    public long extractTimestamp(Bean1 bean1, long l) {
                                        return bean1.getTs() * 1000L;
                                    }
                                })
                );

        SingleOutputStreamOperator<Bean2> stream2 = executionEnvironment.socketTextStream("hadoop200", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Bean2>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                                    @Override
                                    public long extractTimestamp(Bean2 bean1, long l) {
                                        return bean1.getTs() * 1000L;
                                    }
                                })
                );


        // 3. 双流join
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDS = stream1.keyBy(Bean1::getId)
                .intervalJoin(stream2.keyBy(Bean2::getId))
                .between(Time.seconds(-2), Time.seconds(5))
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 bean1, Bean2 bean2, Context context, Collector<Tuple2<Bean1, Bean2>> collector) throws Exception {
                        collector.collect(Tuple2.of(bean1, bean2));
                    }
                });

        // 4. 打印
        joinDS.print();
        // 5. 启动任务
        executionEnvironment.execute();
    }
}
