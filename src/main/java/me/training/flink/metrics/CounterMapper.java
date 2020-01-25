package me.training.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

public class CounterMapper extends RichMapFunction<String, String> {
    private transient Counter counter;

    @Override
    public void open(Configuration conf) throws Exception {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("foo-counter");
//                .counter("bar-counter", new MyCounter());
    }

    @Override
    public String map(String s) throws Exception {
        this.counter.inc();
        return s;
    }
}

