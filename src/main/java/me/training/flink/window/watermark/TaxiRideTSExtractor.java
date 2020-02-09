package me.training.flink.window.watermark;

import me.training.flink.datatype.TaxiRide;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TaxiRideTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaxiRide> {

    public TaxiRideTSExtractor() {
        super(Time.seconds(60));
    }

    @Override
    public long extractTimestamp(TaxiRide ride) {
        if (ride.isStart) {
            return ride.startTime.getMillis();
        } else {
            return ride.endTime.getMillis();
        }
    }
}
