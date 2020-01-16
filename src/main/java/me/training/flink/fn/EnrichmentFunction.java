package me.training.flink.fn;

import me.training.flink.datatype.TaxiFare;
import me.training.flink.datatype.TaxiRide;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
    private ValueState<TaxiRide> rideState;
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration parameters) throws Exception {
        rideState = getRuntimeContext().getState(
                new ValueStateDescriptor<TaxiRide>("saved ride", TaxiRide.class));
        fareState = getRuntimeContext().getState(
                new ValueStateDescriptor<TaxiFare>("saved fare", TaxiFare.class));
    }

    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiFare fare = fareState.value();
        if (fare != null) {
            fareState.clear();
            out.collect(Tuple2.of(ride, fare));
        } else {
            rideState.update(ride);
        }

    }

    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiRide ride = rideState.value();
        if (ride != null) {
            rideState.clear();
            out.collect(Tuple2.of(ride, fare));
        } else {
            fareState.update(fare);
        }
    }
}
