package com.solace.connector.spark;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;


public class SolaceScanBuilder implements ScanBuilder {

    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public SolaceScanBuilder(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
        this.properties = properties;
    }

    @Override
    public Scan build() {
        return new SolaceScan(schema,properties,options);
    }
}
