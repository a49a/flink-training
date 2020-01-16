package me.training.flink.basics;

import me.training.flink.datatype.TaxiRide;
import me.training.flink.source.TaxiRideSource;
import me.training.flink.util.Base;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideCount {
    public static void main(String[] args) throws Exception {
        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", Base.pathToRideData);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));
        DataStream<Tuple2<Long, Long>> tuples = rides.map(new MapFunction<TaxiRide, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(TaxiRide taxiRide) throws Exception {
                return Tuple2.of(taxiRide.driverId, 1L);
            }
        });

        KeyedStream<Tuple2<Long, Long>, Tuple> keyedByDriverId = tuples.keyBy(0);
        DataStream<Tuple2<Long, Long>> rideCounts = keyedByDriverId.sum(1);
        rideCounts.print();

        env.execute("Ride Count");
    }
}
