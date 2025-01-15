package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.write.SolaceBatchWrite;
import com.solacecoe.connectors.spark.streaming.write.SolaceStreamingWrite;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SolaceWriteBuilder implements WriteBuilder {
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    public SolaceWriteBuilder(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }
    @Override
    public Write build() {
        return new Write() {
            @Override
            public BatchWrite toBatch() {
                return new SolaceBatchWrite(schema, properties, options);
            }

            @Override
            public StreamingWrite toStreaming() {
                return new SolaceStreamingWrite(schema, properties, options);
            }
        };
    }
}
