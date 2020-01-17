package me.training.flink.table.source;

import me.training.flink.datatype.TaxiFare;
import me.training.flink.source.TaxiFareSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;

public class TaxiFareTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {
    private final TaxiFareSource taxiFareSource;

    public TaxiFareTableSource(String dataFilePath, int maxEventDelaySecs, int servingSpeedFactor) {
        this.taxiFareSource = new TaxiFareSource(dataFilePath, maxEventDelaySecs, servingSpeedFactor);
    }

    @Override
    public TableSchema getTableSchema() {
        DataType[] types = new DataType[] {
                DataTypes.BIGINT(),
                DataTypes.BIGINT(),
                DataTypes.BIGINT(),
                DataTypes.STRING(),
                DataTypes.FLOAT(),
                DataTypes.FLOAT(),
                DataTypes.FLOAT(),
                DataTypes.TIMESTAMP()
        };

        String[] names = new String[]{
                "rideId",
                "taxiId",
                "driverId",
                "paymentType",
                "tip",
                "tolls",
                "totalFare",
                "eventTime"
        };
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.fields(names, types);
        return builder.build();
    }

    @Override
    public String explainSource() {
        return "TaxiFare";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        return env
                .addSource(this.taxiFareSource)
                .map(new TaxiFareToRow());
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor("eventTime", new StreamRecordTimestamp(), new PreserveWatermarks());
        return Collections.singletonList(descriptor);
    }

    public static class TaxiFareToRow implements MapFunction<TaxiFare, Row> {

        @Override
        public Row map(TaxiFare fare) throws Exception {
            return Row.of(
                    fare.rideId,
                    fare.taxiId,
                    fare.driverId,
                    fare.paymentType,
                    fare.tip,
                    fare.tolls,
                    fare.totalFare
            );
        }
    }
}
