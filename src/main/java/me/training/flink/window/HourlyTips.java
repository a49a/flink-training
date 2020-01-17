package me.training.flink.window;

import me.training.flink.datatype.TaxiFare;
import me.training.flink.source.TaxiFareSource;
import me.training.flink.util.Base;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTips {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", Base.pathToFareData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<TaxiFare> fares = env.addSource(
                new TaxiFareSource(input, maxEventDelay, servingSpeedFactor));

        Time time = Time.seconds(5);

        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy(fare -> fare.driverId)
                .timeWindow(time)
                .process(new AddTips());

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .timeWindowAll(time)
                .maxBy(2);

        hourlyMax.print();

        env.execute("Hourly Tips");
    }

    public static class AddTips extends ProcessWindowFunction<
                TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context ctx, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            Float sumOfTips = 0F;
            for (TaxiFare f : fares) {
                sumOfTips += f.tip;
            }
            out.collect(new Tuple3<>(ctx.window().getEnd(), key, sumOfTips));
        }
    }
}
