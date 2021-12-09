package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
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

        // 2. 读取kafka dwd_page_log 主题数据
        String groupId = "unique_visit_app_1109";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(flinkKafkaConsumer);
        // 3. 将数据转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(JSON::parseObject);
        // 4. 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        // 5. 使用状态编程的方式过滤数据
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            // 定义状态
            private transient ValueState<String> dtState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>(
                        "last-visit", String.class
                );
                StateTtlConfig stateTtlConfig = StateTtlConfig
                        .newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dtState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取上一跳页面ID
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null || lastPageId.equals("")) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    //取出状态数据
                    String dt = dtState.value();

                    //获取今天的日期
                    Long ts = value.getLong("ts");
                    String curDt = sdf.format(ts);

                    if (dt == null || !dt.equals(curDt)) {
                        //更新状态并保留数据
                        dtState.update(curDt);
                        return true;
                    }
                }
                return false;
            }
        });
        // 6. 打印同时写入kafka
        uvDS.print();
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(sinkTopic));
        // 7. 启动
        env.execute();
    }
}
