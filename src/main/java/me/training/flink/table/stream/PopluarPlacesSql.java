package me.training.flink.table.stream;

import me.training.flink.table.source.TaxiRideTableSource;
import me.training.flink.util.Base;
import me.training.flink.util.GeoUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class PopluarPlacesSql {
    public static void main(String[] args) throws Exception {
        // read parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", Base.pathToRideData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a TableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // register TaxiRideTableSource as table "TaxiRides"
        tEnv.registerTableSource(
                "taxi_ride",
                new TaxiRideTableSource(
                        input,
                        maxEventDelay,
                        servingSpeedFactor));
        // register user-defined functions
        tEnv.registerFunction("isInNYC", new GeoUtils.IsInNYC());
        tEnv.registerFunction("toCellId", new GeoUtils.ToCellId());
        tEnv.registerFunction("toCoords", new GeoUtils.ToCoords());

        Table result = tEnv.sqlQuery(
                "SELECT toCoords(cell), w_start, w_end, isStart, popCnt " +
                "FROM " +
                    "(SELECT " +
                        "cell, " +
                        "isStart, " +
                        "HOP_START(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS w_start, " +
                        "HOP_END(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS w_end, " +
                        "COUNT(isStart) AS popCnt " +
                    "FROM " +
                        "(SELECT " +
                            "eventTime, " +
                            "isStart, " +
                            "CASE WHEN isStart THEN toCellId(startLon, startLat) ELSE toCellId(endLon, endLat) END AS cell " +
                        "FROM taxi_ride " +
                        "WHERE isInNYC(startLon, startLat) AND isInNYC(endLon, endLat)) " +
                    "GROUP BY cell, isStart, HOP(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)) " +
                "WHERE popCnt > 20 ");

        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
