package me.training.flink.fn;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
    private ValueState<Boolean> blocked;

    @Override
    public void open(Configuration parameters) throws Exception {
        blocked = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("blocked", Boolean.class)
        );
    }

    @Override
    public void flatMap1(String controlValue, Collector<String> out) throws Exception {
        blocked.update(Boolean.TRUE);
    }

    @Override
    public void flatMap2(String dataValue, Collector<String> out) throws Exception {
        if (blocked.value() == null) {
            out.collect(dataValue);
        }
    }
}
