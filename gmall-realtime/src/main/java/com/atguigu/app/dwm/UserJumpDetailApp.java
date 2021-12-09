package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
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

        // 2. 读取kafka主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId));

        // 3. 将每行数据转换为JSON对象, 并提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS =
                dataStreamSource.map(JSON::parseObject)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) ->
                                                jsonObject.getLong("ts"))
                        );

        // 4. 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // 5. 将模式序列作用于流上
        KeyedStream<JSONObject, Object> keyedJsonObjDS = jsonObjDS.keyBy(new KeySelector<JSONObject, Object>() {
            @Override
            public Object getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedJsonObjDS, pattern);

        // 6. 提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeOut") {};
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);
        // 7. union两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);
        // 8. 将数据写入kafka
        unionDS.print();
        unionDS
                .map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));
        // 9. 启动任务
        env.execute();
    }
}
