package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.SolaceMicroBatch;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Optional;

public class SolaceSparkStream implements DataSourceV2, MicroBatchReadSupport, DataSourceRegister, Serializable {
    @Override
    public String shortName() {
        return "solace";
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> optional, String s, DataSourceOptions dataSourceOptions) {
        return new SolaceMicroBatch(dataSourceOptions);
    }
}
