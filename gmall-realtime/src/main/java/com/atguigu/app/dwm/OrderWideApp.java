package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;


public class OrderWideApp {
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

        // 2. 读取kafka 主题的数据 并转换为javaBean对象，提取时间戳生成watermark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env
                .addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env
                .addSource(MyKafkaUtil.getFlinkKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(simpleDateFormat.parse(create_time).getTime());
                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        }));

        // 3. 双流join
        SingleOutputStreamOperator<OrderWide> orderWideWithNoDimDS = orderInfoDS
                .keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5)) // 生产环境中给的时间为最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        // 4. 关联维度信息 用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream
                .unorderedWait(orderWideWithNoDimDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setUser_gender(dimInfo.getString("GENDER"));
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        long currentTs = System.currentTimeMillis();
                        long ts = simpleDateFormat.parse(birthday).getTime();
                        Long age = (currentTs - ts) / (1000 * 60 * 60 * 24 * 365L);
                        input.setUser_age(age.intValue());
                    }
                }, 60, TimeUnit.SECONDS);

        // 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
            @Override
            public String getKey(OrderWide input) {
                return input.getProvince_id().toString();
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setProvince_name(dimInfo.getString("NAME"));
                orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
            }
        }, 60, TimeUnit.SECONDS);

        // 关联sku维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getSku_id());
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setSku_name(dimInfo.getString("SKU_NAME"));
                orderWide.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                orderWide.setSpu_id(dimInfo.getLong("SPU_ID"));
                orderWide.setTm_id(dimInfo.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);


        // 关联spu维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getSpu_id());
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setSpu_name(dimInfo.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        // 关联tm维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(OrderWide input) {
                return String.valueOf(input.getTm_id());
            }

            @Override
            public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                orderWide.setTm_name(dimInfo.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        // 关联category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(orderWideWithTmDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide input) {
                        return String.valueOf(input.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        orderWide.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

//        orderWideWithCategory3DS.print();
        // 5. 将数据写入kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(orderWideSinkTopic));

        // 6. 启动任务
        env.execute();
    }
}
