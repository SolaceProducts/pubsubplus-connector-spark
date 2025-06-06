package com.solacecoe.connectors.spark;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SolaceBatch implements Batch {

    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private String filename;

    public SolaceBatch(StructType schema,
                    Map<String, String> properties,
                    CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.filename = options.get("fileName");
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return null;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return null;
                //new SolacePartitionReaderFactory(schema, filename);
    }
}
