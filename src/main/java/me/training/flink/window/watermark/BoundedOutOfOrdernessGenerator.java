package me.training.flink.window.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks {
    @Override
    public long extractTimestamp(Object o, long l) {
        return 0;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }
}
