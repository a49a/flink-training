package me.training.flink.window.watermark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.Iterator;

public class EventTimeSource implements SourceFunction<FooRecord> {

    @Override
    public void run(SourceContext<FooRecord> ctx) throws Exception {
//        现实业务中是读数据源数据，现在用迭代器模拟一下
        ArrayList list = new ArrayList<FooRecord>();
        Iterator iter = list.iterator();

        while (iter.hasNext()) {
            FooRecord record = (FooRecord) iter.next();
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


}
