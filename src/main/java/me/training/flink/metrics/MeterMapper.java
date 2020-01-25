package me.training.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;

public class MeterMapper extends RichMapFunction<String, String> {
    private transient Meter meter;

    @Override
    public void open(Configuration conf) throws Exception {
        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("foo-meter", new Meter() {
                    @Override
                    public void markEvent() {

                    }

                    @Override
                    public void markEvent(long l) {

                    }

                    @Override
                    public double getRate() {
                        return 0;
                    }

                    @Override
                    public long getCount() {
                        return 0;
                    }
                });
    }

    @Override
    public String map(String s) throws Exception {
        meter.markEvent();
        return s;
    }
}
