package me.training.flink.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class KafkaTableWordCount {
    private static String KAFKA_VERSION = "universal";
    private static String TOPIC = "foo-topic";
    private static String SERVERS = "localhost:9092";
    private static String GROUP_ID = "";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv
            .connect(
                new Kafka()
                .version(KAFKA_VERSION)
                .topic(TOPIC)
                .property("bootstrap.servers", SERVERS)
//                .property("group.id", GROUP_ID)
//                .startFromEarliest()
            )
            .withFormat(
                    new Csv()
                .fieldDelimiter(',')
                .lineDelimiter("\n")
                    .deriveSchema()
            )
                .withSchema(
                        new Schema()
                        .field("id", Types.INT)
                        .field("name", Types.STRING)
                )
                .registerTableSource("words");
    }
}
