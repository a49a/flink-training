package me.training.flink.window.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<FooRecord> {

    //  获取EventTime时间戳
    @Override
    public long extractTimestamp(FooRecord record, long previousElementTimestamp) {
        return record.getEventTime();
    }
    //  每条数据决定Watermark生成，新的Watermark时间要大于之前的才会触发。
    @Override
    public Watermark checkAndGetNextWatermark(FooRecord record, long extractedTimestamp) {
        return record.hasWatermark() ? new Watermark(extractedTimestamp) : null;
    }
}
