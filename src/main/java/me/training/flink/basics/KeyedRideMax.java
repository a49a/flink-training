package me.training.flink.basics;

import me.training.flink.datatype.EnrichedRide;
import me.training.flink.datatype.TaxiRide;
import me.training.flink.fn.NYCEnrichment;
import me.training.flink.source.TaxiRideSource;
import me.training.flink.util.Base;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.shaded.org.joda.time.Interval;
import org.apache.flink.table.shaded.org.joda.time.Minutes;
import org.apache.flink.util.Collector;

public class KeyedRideMax {
    public static void main(String[] args) throws Exception {
        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", Base.pathToRideData);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));
        DataStream<EnrichedRide> enrichedNYCRides = rides
                .flatMap(new NYCEnrichment());
        DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
                    @Override
                    public void flatMap(EnrichedRide ride,
                                        Collector<Tuple2<Integer, Minutes>> out) throws Exception {
                        if (!ride.isStart) {
                            Interval rideInterval = new Interval(ride.startTime, ride.endTime);
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            out.collect(new Tuple2<>(ride.startCell, duration));
                        }
                    }
                });

        minutesByStartCell
                .keyBy(0)
                .maxBy(1)
                .print("maxBy: ");
        minutesByStartCell
                .keyBy(0)
                .max(1)
                .print("max: ");

        env.execute("Keyed Ride Max");
    }
}
