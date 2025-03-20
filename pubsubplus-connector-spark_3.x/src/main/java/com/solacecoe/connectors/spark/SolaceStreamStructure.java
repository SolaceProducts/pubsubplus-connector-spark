package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkSchemaProperties;
import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SolaceStreamStructure implements SupportsRead, SupportsWrite, Table {

    private final StructType schema;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;

    private CaseInsensitiveStringMap options;

    public SolaceStreamStructure(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
        return new SolaceScanBuilder(schema, properties, options);
    }

    @Override
    public String name() {
        return this.getClass().toString();
    }

    @Override
    public StructType schema() {
        boolean includeHeaders = Boolean.parseBoolean(this.properties.getOrDefault(SolaceSparkStreamingProperties.INCLUDE_HEADERS, SolaceSparkStreamingProperties.INCLUDE_HEADERS_DEFAULT));
        return getSchema(includeHeaders);
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.MICRO_BATCH_READ);
            capabilities.add(TableCapability.BATCH_WRITE);
            capabilities.add(TableCapability.STREAMING_WRITE);
            capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA); // Required for WriteStream
        }
        return capabilities;
    }

    private static StructType getSchema(boolean includeHeaders) {
        return new StructType(SolaceSparkSchemaProperties.structFields(includeHeaders));

    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        return new SolaceWriteBuilder(logicalWriteInfo.schema(), properties, logicalWriteInfo.options());
    }
}
