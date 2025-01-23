package com.solacecoe.connectors.spark.streaming.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.util.Map;

public class SolaceDataWriterFactory implements DataWriterFactory, Serializable {
    private final StructType schema;
    private final Map<String, String> properties;
    public SolaceDataWriterFactory(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new SolaceDataWriter(schema, properties);
    }
}
