package me.training.flink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class Average extends RichMapFunction<Tuple2<String, Integer>, Double> {

    private transient ValueState<Tuple2<Integer, Integer>> sum;

    @Override
    public Double map(Tuple2<String, Integer> input) throws Exception {
        Tuple2<Integer, Integer> currentSum = sum.value();
        if (currentSum == null) {
            currentSum = Tuple2.of(0, 0);
        }
        currentSum.f0 += input.f1;
        currentSum.f1 += 1;
        sum.update(currentSum);
        return (double) currentSum.f0 / currentSum.f1;
    }

    @Override
    public void open(Configuration conf) throws Exception {
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ValueStateDescriptor<Tuple2<Integer, Integer>>(
                        "sum",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})
                );

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(6, false)
        .cleanupInBackground()
                .build();
        descriptor.enableTimeToLive(ttlConfig);

        sum = getRuntimeContext().getState(descriptor);
    }
}
