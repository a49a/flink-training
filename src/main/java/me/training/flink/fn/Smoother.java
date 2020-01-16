package me.training.flink.fn;

import me.training.flink.datatype.MovingAverage;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class Smoother extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
    private ValueState<MovingAverage> averageState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<MovingAverage> descriptor =
                new ValueStateDescriptor<MovingAverage>("moving average", MovingAverage.class);
        averageState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Tuple2<String, Double> map(Tuple2<String, Double> item) throws Exception {
        MovingAverage average = averageState.value();

        if (average == null) {
            average = new MovingAverage(2);
        }
        average.add(item.f1);
        averageState.update(average);

        return new Tuple2<>(item.f0, average.getAverage());
    }

}
