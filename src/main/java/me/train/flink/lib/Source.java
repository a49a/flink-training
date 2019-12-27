package me.train.flink.lib;

import me.train.flink.ConsumerStringDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Source {
    public static FlinkKafkaConsumer<String> getKafka(String topic) {
        Properties properties = new Properties();
        ConsumerStringDeserializationSchema schema = new ConsumerStringDeserializationSchema();
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
                topic,
                schema,
                properties
        );
        return consumer;
    }
}
