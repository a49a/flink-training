package me.training.flink.window.watermark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.Iterator;

public class EventTimeSource implements SourceFunction<EventTimeSource.Record> {

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
//        现实业务中是读数据源数据，现在用迭代器模拟一下
        ArrayList list = new ArrayList<Record>();
        Iterator iter = list.iterator();

        while (iter.hasNext()) {
            Record record = (Record) iter.next();
//            告知Flink这条数据版绑定的EventTime时间戳
            ctx.collectWithTimestamp(record, record.getEventTime());
            if (record.hasWatermark()) {
//                告知Flink触发Watermark
                ctx.emitWatermark(new Watermark(record.getWatermarkTime()));
            }
        }
    }

    @Override
    public void cancel() { }

    public static class Record {
//        获取数据条目的EventTime时间戳
        public Long getEventTime() {
            return 0L;
        }
//        判断记录是否触发Watermark的逻辑
        public boolean hasWatermark() {
            return true;
        }
//        自定义实现获取Watermark时间戳的逻辑
        public Long getWatermarkTime() {
            return 0L;
        }
    }
}
