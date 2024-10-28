package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.write.SolaceBatchWriter;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class SolaceWriteBuilder implements WriteBuilder {
    private final StructType schema;
    private final Map<String, String> options;
    public SolaceWriteBuilder(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;
    }
    @Override
    public Write build() {
        return new Write() {
            @Override
            public BatchWrite toBatch() {
                return new SolaceBatchWriter(schema, options);
            }
        };
    }
}
