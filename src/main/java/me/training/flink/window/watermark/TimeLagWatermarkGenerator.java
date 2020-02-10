package me.training.flink.window.watermark;

import me.training.flink.window.watermark.FooRecord;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<FooRecord> {

    private final long maxTimeLag = 5000; // 5 seconds

    @Override
    public long extractTimestamp(FooRecord element, long previousElementTimestamp) {
        return element.getWatermarkTime();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}
