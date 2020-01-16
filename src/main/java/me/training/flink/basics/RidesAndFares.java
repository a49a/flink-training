package me.training.flink.basics;

import me.training.flink.datatype.TaxiFare;
import me.training.flink.datatype.TaxiRide;
import me.training.flink.fn.EnrichmentFunction;
import me.training.flink.source.TaxiFareSource;
import me.training.flink.source.TaxiRideSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import me.training.flink.util.Base;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RidesAndFares {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", Base.pathToRideData);
        final String faresFile = params.get("fares", Base.pathToFareData);

        final int delay = 60;					// at most 60 seconds of delay
        final int servingSpeedFactor = 1800;

        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        DataStream<TaxiRide> rides = env.addSource(
                new TaxiRideSource(ridesFile, delay, servingSpeedFactor))
                .keyBy(ride -> ride.rideId);
        DataStream<TaxiFare> fares = env.addSource(
                new TaxiFareSource(faresFile, delay, servingSpeedFactor))
                .keyBy(fare -> fare.rideId);

        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRide = rides
                .connect(fares)
                .flatMap(new EnrichmentFunction())
                .uid("enrichment");

        enrichedRide.print();

        env.execute("Join Rides with Fares using RichCoFlatMap");
    }
}
