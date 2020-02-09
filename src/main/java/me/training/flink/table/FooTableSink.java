package me.training.flink.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;

public class FooTableSink implements StreamTableSink<Row> {

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return null;
    }
}
