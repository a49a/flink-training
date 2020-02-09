package me.training.flink.window.watermark;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class ExtractKafkaTimestamp extends AscendingTimestampExtractor<String> {

    @Override
    public long extractAscendingTimestamp(String s) {
        return 0L;
    }
}
