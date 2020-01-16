package me.training.flink.source;

import me.training.flink.datatype.TaxiRide;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.*;
import java.util.Calendar;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.zip.GZIPInputStream;

public class TaxiRideSource implements SourceFunction<TaxiRide> {

    private final int maxDelayMsecs;
    private final int watermarkDelayMsecs;
    private final int servingSpeed;

    private String dataFilePath;

    private transient InputStream gzipStream;
    private transient BufferedReader reader;

    public TaxiRideSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) throws Exception {
        if (maxEventDelaySecs < 0) {
            throw new IllegalAccessException("Max event delay must be positive");
        }
        this.dataFilePath = dataFilePath;
        this.maxDelayMsecs = maxEventDelaySecs * 1000;
        this.watermarkDelayMsecs = maxDelayMsecs < 10000 ? 10000 : maxDelayMsecs;
        this.servingSpeed = servingSpeedFactor;
    }

    @Override
    public void run(SourceContext<TaxiRide> ctx) throws Exception {
        gzipStream = new GZIPInputStream(new FileInputStream(dataFilePath));
        reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"));

        genUnorderdStream(ctx);

        this.gzipStream.close();
        this.gzipStream = null;
        this.reader.close();
        this.reader = null;
    }

    private void genUnorderdStream(SourceContext<TaxiRide> ctx) throws Exception {
        long servingStartTime = Calendar.getInstance().getTimeInMillis();
        long dataStartTime;

        PriorityQueue<Tuple2<Long, Object>> emitSchedule = new PriorityQueue<>(
                32,
                new Comparator<Tuple2<Long, Object>>() {
                    @Override
                    public int compare(Tuple2<Long, Object> o1, Tuple2<Long, Object> o2) {
                        return o1.f0.compareTo(o2.f0);
                    }
                });

        Random rand = new Random(7452);

        String line;
        TaxiRide ride;

        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = TaxiRide.fromString(line);
            dataStartTime = getEventTime(ride);
            long delayedEventTime = dataStartTime + getNormalDelayMsecs(rand);
            emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

            long watermarkTime = dataStartTime + watermarkDelayMsecs;
            Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
            emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
        } else {
            return;
        }

        // peek at next ride
        if (reader.ready() && (line = reader.readLine()) != null) {
            ride = TaxiRide.fromString(line);
        }

        while (emitSchedule.size() > 0 || reader.ready()) {
            long curNextDelayedEventTime = !emitSchedule.isEmpty() ? emitSchedule.peek().f0 : -1;
            long rideEventTime = ride != null ? getEventTime(ride) : -1;

            while (
                    ride != null && (
                        emitSchedule.isEmpty() ||
                        rideEventTime < curNextDelayedEventTime + maxDelayMsecs
                    )
            ) {
                long delayedEventTime = rideEventTime + getNormalDelayMsecs(rand);
                emitSchedule.add(new Tuple2<Long, Object>(delayedEventTime, ride));

                if (reader.ready() && (line = reader.readLine()) != null) {
                    ride = TaxiRide.fromString(line);
                    rideEventTime = getEventTime(ride);
                } else {
                    ride = null;
                    rideEventTime = -1;
                }
            }

            Tuple2<Long, Object> head = emitSchedule.poll();
            long delayedEventTime = head.f0;
            long now = Calendar.getInstance().getTimeInMillis();
            long serveringTime = toServingTime(servingStartTime, dataStartTime, delayedEventTime);
            long waitTime = serveringTime - now;

            Thread.sleep((waitTime > 0) ? waitTime : 0);

            if (head.f1 instanceof TaxiRide) {
                TaxiRide emitRide = (TaxiRide) head.f1;
                ctx.collectWithTimestamp(emitRide, getEventTime(emitRide));
            } else if (head.f1 instanceof Watermark) {
                Watermark emitWatermark = (Watermark) head.f1;
                ctx.emitWatermark(emitWatermark);

                //schedule next watermark
                long watermarkTime = delayedEventTime + watermarkDelayMsecs;
                Watermark nextWatermark = new Watermark(watermarkTime - maxDelayMsecs - 1);
                emitSchedule.add(new Tuple2<Long, Object>(watermarkTime, nextWatermark));
            }
        }


    }

    public long toServingTime(long servingStartTime, long dataStartTime, long eventTime) {
        long dataDiff = eventTime - dataStartTime;
        return servingStartTime + (dataDiff / this.servingSpeed);
    }

    public long getEventTime(TaxiRide ride) {
        return ride.getEventTime();
    }

    public long getNormalDelayMsecs(Random rand) {
        long delay = 1;
        long x = maxDelayMsecs / 2;
        while (delay < 0 || delay > maxDelayMsecs) {
            delay = (long)(rand.nextGaussian() * x) + x;
        }
        return delay;
    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.gzipStream != null) {
                this.gzipStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Can not cancel source: ", ioe);
        } finally {
            this.reader = null;
            this.gzipStream = null;
        }
    }
}
