package me.training.flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class RandomSource extends RichParallelSourceFunction<Long> {
    private boolean canceled = false;
    private Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();
        while (!canceled) {
            Long next = random.nextLong();
            synchronized (lock) {
                ctx.collect(next);
            }
        }
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}
