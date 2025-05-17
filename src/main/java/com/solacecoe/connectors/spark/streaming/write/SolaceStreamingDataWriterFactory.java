package com.solacecoe.connectors.spark.streaming.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.util.Map;

public class SolaceStreamingDataWriterFactory implements StreamingDataWriterFactory, Serializable {
    private final StructType schema;
    private final Map<String, String> properties;
    public SolaceStreamingDataWriterFactory(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new SolaceDataWriter(partitionId, schema, properties);
    }
}
