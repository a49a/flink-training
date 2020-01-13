package me.training.flink.state;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

public class CounterSource extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {

    private Long offset;
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
    // id是checkpoint的自增id，timestamp是flink master触发checkpoint时候的时间戳
    @Override
    public List snapshotState(long id, long timestamp) throws Exception {
        return Collections.singletonList(offset);
    }
    // 故障恢复
    @Override
    public void restoreState(List<Long> state) throws Exception {
        for (Long item : state) {
            offset = item;
        }
    }
}
