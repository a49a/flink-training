package me.training.flink.basics;

import me.training.flink.fn.ControlFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConnectedStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // control流用于指定必须从streamOfWords流中过滤掉的单词
        DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        // data和artisans不在control流中，状态是不会被记录为true的（flatMap1），即为null，所以streamOfWords在调用flatMap2时会被out输出
        DataStream<String> streamOfWords = env.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(x -> x);

        // 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
        control.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        /**
         * 1> data
         * 4> artisans
         */
        env.execute("ConnectedStreamsExerise Job");
    }
}
