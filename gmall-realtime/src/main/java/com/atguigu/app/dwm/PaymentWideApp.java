package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {

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

        // 2. 读取kafka主题的数据 创建流 并转换为javabean对象 提取时间戳生成watermark
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide orderWide, long l) {
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return simpleDateFormat.parse(orderWide.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    // 进入系统的时间
                                    return l;
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return simpleDateFormat.parse(paymentInfo.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    // 进入系统的时间
                                    return l;
                                }
                            }
                        }));

        // 3. 双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS
                .keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        paymentWideDS.print();

        // 4. 将数据写入kafka
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(paymentWideSinkTopic));

        // 5. 启动任务
        env.execute();
    }

}
