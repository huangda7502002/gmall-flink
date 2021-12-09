package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
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
        // 2. 读取kafka主题的数据
        String groupId = "visitor_stats_app_11093";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(userJumpDetailSourceTopic, groupId));
        // 3. 将各个流中的数据转换为统一格式
        // pageDS -> pv, 进入页面数，连续访问时长
        System.out.println(pvDS);
        System.out.println(uvDS);
        System.out.println(ujDS);
        //3.1 处理UV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        //3.2 处理UJ数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        //3.3 处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            //获取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            //获取页面信息
            JSONObject page = jsonObject.getJSONObject("page");

            String last_page_id = page.getString("last_page_id");

            long sv = 0L;

            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //TODO 4.Union几个流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(
                visitorStatsWithUjDS,
                visitorStatsWithPvDS);

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        // 6. 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWMDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(visitorStats.getAr(), visitorStats.getCh(), visitorStats.getIs_new(), visitorStats.getVc());
            }
        });
        // 7. 开窗聚合 10s的滚动窗口
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUj_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                VisitorStats visitorStats = iterable.iterator().next();
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                collector.collect(visitorStats);
            }
        });
        result.print();
        // 8. 将数据写入clickhouse
        env.execute();
    }
}
