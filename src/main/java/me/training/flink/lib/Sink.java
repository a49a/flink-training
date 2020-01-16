package me.training.flink.lib;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;


import java.util.Properties;

public class Sink {
//    public static FlinkKafkaProducer<String> getKafka(String topic) {
//        Properties properties = new Properties();
//        KafkaSerializationSchema schema = new ProducerStringSerializationSchema(topic);
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        return new FlinkKafkaProducer<String>(
//                topic,
//                schema,
//                properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//        );
//    }
}
