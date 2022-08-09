package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    private static Properties properties = new Properties();
    private static String DEFAULT_TOPIC = "default_topic";

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ambari01:6667,ambari01:6667,ambari01:6667");
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static <T> FlinkKafkaProducer<T> getFlinkKafkaProducerBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
