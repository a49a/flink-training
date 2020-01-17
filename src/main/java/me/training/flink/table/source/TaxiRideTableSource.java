package me.training.flink.table.source;

import me.training.flink.datatype.TaxiRide;
import me.training.flink.source.TaxiRideSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

public class TaxiRideTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {
    private final TaxiRideSource taxiRideSource;

    public TaxiRideTableSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        this.taxiRideSource = new TaxiRideSource(dataFilePath, maxEventDelaySecs, servingSpeedFactor);
    }

    @Override
    public TypeInformation<Row> getReturnType() {

        TypeInformation<?>[] types = new TypeInformation[] {
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.BOOLEAN,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.SHORT
        };

        String[] names = new String[]{
                "rideId",
                "taxiId",
                "driverId",
                "isStart",
                "startLon",
                "startLat",
                "endLon",
                "endLat",
                "passengerCnt"
        };

        return new RowTypeInfo(types, names);
    }

    @Override
    public TableSchema getTableSchema() {
        TypeInformation<?>[] types = new TypeInformation[] {
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.BOOLEAN,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.FLOAT,
                Types.SHORT,
                Types.SQL_TIMESTAMP
        };

        String[] names = new String[]{
                "rideId",
                "taxiId",
                "driverId",
                "isStart",
                "startLon",
                "startLat",
                "endLon",
                "endLat",
                "passengerCnt",
                "eventTime"
        };

        return new TableSchema(names, types);
    }

    @Override
    public String explainSource() {
        return "TaxiRide";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {

        return env
                .addSource(this.taxiRideSource)
                .map(new TaxiRideToRow()).returns(getReturnType());
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor("eventTime", new StreamRecordTimestamp(), new PreserveWatermarks());
        return Collections.singletonList(descriptor);
    }

    public static class TaxiRideToRow implements MapFunction<TaxiRide, Row> {

        @Override
        public Row map(TaxiRide ride) throws Exception {

            return Row.of(
                    ride.rideId,
                    ride.taxiId,
                    ride.driverId,
                    ride.isStart,
                    ride.startLon,
                    ride.startLat,
                    ride.endLon,
                    ride.endLat,
                    ride.passengerCnt);
        }
    }
}
