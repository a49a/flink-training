package me.training.flink.window.watermark;

public class FooRecord {
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