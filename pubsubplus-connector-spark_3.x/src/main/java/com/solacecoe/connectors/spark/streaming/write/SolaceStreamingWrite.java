package com.solacecoe.connectors.spark.streaming.write;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class SolaceStreamingWrite implements StreamingWrite, Serializable {
    private static Logger log = LoggerFactory.getLogger(SolaceBatchWrite.class);
    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    public SolaceStreamingWrite(StructType schema, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.properties = properties;
        this.options = options;

        SolaceUtils.validateCommonProperties(properties);

//        if(!properties.containsKey("topic") || properties.get("topic") == null || properties.get("topic").isEmpty()) {
//            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
//            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
//        }
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new SolaceStreamingDataWriterFactory(schema, properties, options);
    }

    @Override
    public void commit(long l, WriterCommitMessage[] writerCommitMessages) {

    }

    @Override
    public void abort(long l, WriterCommitMessage[] writerCommitMessages) {

    }
}
