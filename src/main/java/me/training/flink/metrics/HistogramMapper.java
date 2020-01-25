package me.training.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

public class HistogramMapper extends RichMapFunction<Long, Long> {
    private transient Histogram histogram;

    @Override
    public void open(Configuration conf) throws Exception {
        getRuntimeContext()
                .getMetricGroup()
                .histogram("foo-histogram", new Histogram() {
                    @Override
                    public void update(long l) {

                    }

                    @Override
                    public long getCount() {
                        return 0;
                    }

                    @Override
                    public HistogramStatistics getStatistics() {
                        return null;
                    }
                });
    }

    @Override
    public Long map(Long v) throws Exception {
        histogram.update(v);
        return v;
    }
}
