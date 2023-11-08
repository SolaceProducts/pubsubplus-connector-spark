package com.solace.connector.spark;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SolaceStreamStructure implements SupportsRead, Table {

    private final StructType schema;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;

    public SolaceStreamStructure(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new SolaceScanBuilder(schema, properties, options);
    }

    @Override
    public String name() {
        return this.getClass().toString();
    }

    @Override
    public StructType schema() {
        return getSchema();
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.MICRO_BATCH_READ);
        }
        return capabilities;
    }

    private static StructType getSchema() {
//        StructField[] headers = new StructField[]{
//                new StructField("key", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("value", DataTypes.StringType, true, Metadata.empty())
//        };
        StructField[] structFields = new StructField[]{
                new StructField("Id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Payload", DataTypes.BinaryType, true, Metadata.empty()),
                new StructField("Topic", DataTypes.StringType, true, Metadata.empty()),
                new StructField("TimeStamp", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("Headers", new MapType(DataTypes.StringType, DataTypes.BinaryType, false), true, Metadata.empty())
        };
        return new StructType(structFields);
    }

}
