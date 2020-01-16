package me.training.flink.fn;

import me.training.flink.datatype.TaxiRide;
import me.training.flink.util.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;

public class NYCFilter implements FilterFunction<TaxiRide> {
    @Override
    public boolean filter(TaxiRide taxiRide) throws Exception {

        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
    }
}
