package com.solacecoe.connectors.spark.streaming.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class SolaceDataWriterFactory implements DataWriterFactory {
    private final StructType schema;
    private final Map<String, String> options;
    public SolaceDataWriterFactory(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new SolaceDataWriter(schema, options);
    }
}
