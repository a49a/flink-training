package me.training.flink.basics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class KeyedStream {
    public static void main(String[] args) throws Exception {
        final String host = "localhost";
        final int port = 9999;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream(host, port, "\n");
        DataStream random = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String input) throws Exception {
                        return Tuple2.of(input, new Random().nextInt());
                    }
                });
        random.print("random: ");
        AllWindowedStream windowedStream = random.timeWindowAll(Time.seconds(4));

        windowedStream.max(0)
                .print("max: ")
                .setParallelism(1);
        windowedStream.maxBy(0)
                .print("maxBy: ")
                .setParallelism(1);

        env.execute("Socket Word Count");
    }
}
