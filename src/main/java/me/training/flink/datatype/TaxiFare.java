package me.training.flink.datatype;

import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Locale;

public class TaxiFare implements Serializable {

    private static transient DateTimeFormatter timeFormatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC();

    public long rideId;
    public long taxiId;
    public long driverId;
    public DateTime startTime;
    public String paymentType;
    public float tip;
    public float tolls;
    public float totalFare;

    public TaxiFare(long rideId, long taxiId, long driverId, DateTime startTime, String paymentType, float tip, float tolls, float totalFare) {
        this.rideId = rideId;
        this.taxiId = taxiId;
        this.driverId = driverId;
        this.startTime = startTime;
        this.paymentType = paymentType;
        this.tip = tip;
        this.tolls = tolls;
        this.totalFare = totalFare;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rideId).append(",");
        sb.append(taxiId).append(",");
        sb.append(driverId).append(",");
        sb.append(startTime.toString(timeFormatter)).append(",");
        sb.append(paymentType).append(",");
        sb.append(tip).append(",");
        sb.append(tolls).append(",");
        sb.append(totalFare);

        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TaxiFare &&
                this.rideId == ((TaxiFare) other).rideId;
    }

    @Override
    public int hashCode() {
        return (int) this.rideId;
    }

    public long getEventTime() {
        return this.startTime.getMillis();
    }
}