package me.training.flink.table.stream;

import me.training.flink.table.source.TaxiRideTableSource;
import me.training.flink.util.Base;
import me.training.flink.util.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PopluarPlacesTableApi {
    public static void main(String[] args) throws Exception {
        final int maxEventDelay = 60;
        final int servingSpeedFactor = 600;

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", Base.pathToRideData);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerTableSource("taxi_ride", new TaxiRideTableSource(
                input,
                maxEventDelay,
                servingSpeedFactor
        ));

        tEnv.registerFunction("isInNYC", new GeoUtils.IsInNYC());
        tEnv.registerFunction("toCellId", new GeoUtils.ToCellId());
        tEnv.registerFunction("toCoords", new GeoUtils.ToCoords());

        Table popPlaces = tEnv
                .scan("taxi_ride")
                .filter("isInNYC(startLon, startLat) && isInNYC(endLon, endLat)")
                .select("eventTime, " +
                        "isStart, " +
                        "(isStart = true).?(toCellId(startLon, startLat), toCellId(endLon, endLat)) AS cell")
                .window(Slide.over("15.minutes").every("5.minutes").on("eventTime").as("w"))
                .groupBy("cell, isStart, w")
                .select("cell, isStart, w.start AS start, w.end AS end, count(isStart) AS popCnt")
                .filter("popCnt > 20")
                .select("toCoords(cell) AS location, start, end, isStart, popCnt");

        tEnv.toAppendStream(popPlaces, Row.class).print();

        env.execute();
    }
}
