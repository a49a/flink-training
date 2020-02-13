package me.training.flink.state;

import me.training.flink.lib.EnvUtils;
import org.apache.calcite.avatica.Meta;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


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

        actionsByUser.connect(bcedPatterns).process(new PatternEvaluator());
    }

    static class Action {
        public Long userId;
        public String action;

        public Action(Long userId, String action) {
            this.userId = userId;
            this.action = action;
        }
    }

    static class Pattern {
        public String firstAction;
        public String secondAction;

        public Pattern(String firstAction, String secondAction) {
            this.firstAction = firstAction;
            this.secondAction = secondAction;
        }
    }

    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Pattern>> {

        private transient ValueState<String> prevActionState;
        private transient MapStateDescriptor<Void, Pattern> patternDesc;

        @Override
        public void open(Configuration conf) throws Exception {
            prevActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastAction", Types.STRING)
            );
            patternDesc = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        }
        //  处理常规流元素
        @Override
        public void processElement(Action action, ReadOnlyContext ctx, Collector<Tuple2<Long, Pattern>> out) throws Exception {
            Pattern pattern = ctx
                    .getBroadcastState(this.patternDesc)
                    .get(null);
            String prevAction = prevActionState.value();
            //  匹配模式
            if (pattern != null &&
                prevAction != null &&
                pattern.firstAction.equals(prevAction) &&
                pattern.secondAction.equals(action.action)) {
                    out.collect(Tuple2.of(ctx.getCurrentKey(), pattern));
            }

            prevActionState.update(action.action);
        }

        //  处理广播流元素
        @Override
        public void processBroadcastElement(Pattern pattern, Context ctx, Collector<Tuple2<Long, Pattern>> collector) throws Exception {
            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(patternDesc);
            bcState.put(null, pattern);
        }
    }
}


