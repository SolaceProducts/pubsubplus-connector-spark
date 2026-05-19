package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.SolaceMicroBatch;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
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
        boolean isDatabricks = false;
        boolean isUCVolume = false;
        if(this.properties.getOrDefault(SolaceSparkStreamingProperties.RUNTIME_PLATFORM, SolaceSparkStreamingProperties.RUNTIME_PLATFORM_DEFAULT).equals(SolaceSparkStreamingProperties.RUNTIME_PLATFORM_DEFAULT)) {
            isDatabricks = true;

            if(checkpointLocation.contains(SolaceSparkStreamingProperties.DATABRICKS_VOLUME_PREFIX)) {
                isUCVolume = true;
            }
        }
        return new SolaceMicroBatch(properties, checkpointLocation, isDatabricks, isUCVolume);
    }
}
