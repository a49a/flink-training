package me.training.flink.state;

import me.training.flink.lib.EnvUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;


public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvUtils.genEnv();
        env.execute("Broadcast state");
        String FILE_PATH = "";
        DataStream<String> text = env.readTextFile(FILE_PATH);
        DataStream<Action> actions =
                text
                .map(new MapFunction<String, Action>() {
            @Override
            public Action map(String s) throws Exception {
                String[] text = s.split(" ", -1);
                long id = Long.parseLong(text[0]);
                return new Action(id, text[1]);
            }
        });

        KeyedStream actionsByUser = actions.keyBy("userId");
        DataStream<Pattern> patterns = text.map(new MapFunction<String, Pattern>() {
            @Override
            public Pattern map(String s) throws Exception {
                return new Pattern("", "");
            }
        });

        MapStateDescriptor<Void, Pattern> bcStateDescriptor =
                new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID, Types.POJO(Pattern.class));

        BroadcastStream<Pattern> bcedPatterns = patterns.broadcast(bcStateDescriptor);

        actionsByUser.connect(bcedPatterns).process();
    }

    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Parttern>> {

    }
}

class Action {
    public Long userId;
    public String action;

    public Action(Long userId, String action) {
        this.userId = userId;
        this.action = action;
    }
}

class Pattern {
    public String firstAction;
    public String secondAction;

    public Pattern(String firstAction, String secondAction) {
        this.firstAction = firstAction;
        this.secondAction =secondAction;
    }
}
