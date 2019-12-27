package me.train.flink.lib;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int threshold;
    private List<Tuple2<String, Integer>> buffer;
    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.buffer = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() == threshold) {
            //TODO send it to sink
            System.out.println(buffer);
            buffer.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> item : buffer) {
            checkpointedState.add(item);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                "buffer-items",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        // 故障恢复
        if (context.isRestored()) {
            for (Tuple2<String, Integer> item : checkpointedState.get()) {
                buffer.add(item);
            }
        }
    }
}
