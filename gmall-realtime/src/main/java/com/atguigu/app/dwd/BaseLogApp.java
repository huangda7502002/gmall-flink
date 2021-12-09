package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
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

        // 2. 读取kafka ods_base_log主题的数据
        String sourceTopic = "ods_base_log";
        String groupId = "baseLogapp";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> kafkaDataSource = env.addSource(flinkKafkaConsumer);

        // 3. 将每行数据转成jsonObject对象
        OutputTag<String> dirty = new OutputTag<String>("DirtyData") {};
        SingleOutputStreamOperator<JSONObject> jsonObjectDataStream = kafkaDataSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirty, s);
                }
            }
        });

        // 4. 按照设备id进行分组，使用状态编程做新老用户校验
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjectDataStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlag = jsonObjectStringKeyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private transient ValueState<String> isNewState;

            @Override
            public void open(Configuration parameters) throws Exception {
                isNewState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is-newState", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                if ("1".equals(isNew)) {
                    // 取出状态中的数据，并判断是否为null
                    if (isNewState.value() != null) {
                        // 说明当前mid不是新用户, 膝盖
                        jsonObject.getJSONObject("common").put("is_new", 0);
                    } else {
                        // 说明为真正的新用户
                        isNewState.update("0");
                    }
                }
                // 输出数据
                collector.collect(jsonObject);
            }
        });
        // 5. 使用侧输出流将 启动，曝光, 页面 数据分流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    // 获取启动数据
                    context.output(startOutputTag, jsonObject.toJSONString());
                } else {
                    // 不是启动数据，则一定为页面数据
                    collector.collect(jsonObject.toJSONString());

                    // 获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    // 取出公共字段
                    JSONObject common = jsonObject.getJSONObject("common");
                    JSONObject page = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");

                    if (displays != null && displays.size() > 0) {
                        JSONObject displayObj = new JSONObject();
                        displayObj.put("common", common);
                        displayObj.put("page", page);
                        displayObj.put("ts", ts);
                        for (Object display : displays) {
                            displayObj.put("display", display);
                            context.output(displayOutputTag, displayObj.toJSONString());
                        }
                    }
                }
            }
        });
        // 6. 将三个数据写入kafka
        String pageSinkTopic = "dwd_page_log";
        String startSinkTopic = "dwd_start_log";
        String displaySinkTopic = "dwd_display_log";
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(pageSinkTopic));
        pageDS.getSideOutput(startOutputTag).addSink(MyKafkaUtil.getFlinkKafkaProducer(startSinkTopic));
        pageDS.getSideOutput(displayOutputTag).addSink(MyKafkaUtil.getFlinkKafkaProducer(displaySinkTopic));
        env.execute();
    }
}
