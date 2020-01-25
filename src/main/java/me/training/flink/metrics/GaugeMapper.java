package me.training.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;

public class GaugeMapper extends RichMapFunction<String, String> {
    private transient int valueToExpose = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .gauge("foo-gauge", new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return valueToExpose;
                    }
                });
                
    }

    @Override
    public String map(String s) throws Exception {
        valueToExpose++;
        return s;
    }
}
