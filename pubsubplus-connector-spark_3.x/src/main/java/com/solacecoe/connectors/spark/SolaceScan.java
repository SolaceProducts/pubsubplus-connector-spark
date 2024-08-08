package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.SolaceMicroBatchNew;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SolaceScan implements Scan {

    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public SolaceScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return Scan.super.description();
    }

    @Override
    public Batch toBatch() {
        return new SolaceBatch(schema, properties, options);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
//        return new SolaceMicroBatch(schema,properties,options);
        return new SolaceMicroBatchNew(schema,properties,options);
    }

//    @Override
//    public ContinuousStream toContinuousStream(String checkpointLocation) {
//        return Scan.super.toContinuousStream(checkpointLocation);
//    }
}
