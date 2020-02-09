package me.training.flink.table.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.util.Random;

public class Sort {
    public static final int OUT_OF_ORDERNESS = 1000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Event> eventStream = env.addSource(new OutOfOrderEventSource())
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());
        Table event = tEnv.fromDataStream(eventStream, "eventTime.rowtime");
        tEnv.registerTable("event", event);
        Table sorted = tEnv.sqlQuery("SELECT eventTime FROM event ORDER BY eventTime ASC");
        DataStream<Row> sortedEventStream = tEnv.toAppendStream(sorted, Row.class);

        sortedEventStream.print();
        env.execute();
    }

    public static class Event {
        public long eventTime;

        Event() {
            this.eventTime = Instant.now().toEpochMilli() + (new Random().nextInt(OUT_OF_ORDERNESS));
        }
    }

    private static class OutOfOrderEventSource implements SourceFunction<Event> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                ctx.collect(new Event());
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class TimestampsAndWatermarks extends BoundedOutOfOrdernessTimestampExtractor<Event> {

        public TimestampsAndWatermarks() {
            super(Time.milliseconds(OUT_OF_ORDERNESS));
        }

        @Override
        public long extractTimestamp(Event event) {
            return event.eventTime;
        }
    }
}
