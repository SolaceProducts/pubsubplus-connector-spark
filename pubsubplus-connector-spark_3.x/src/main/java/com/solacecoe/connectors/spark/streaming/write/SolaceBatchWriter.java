package com.solacecoe.connectors.spark.streaming.write;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SolaceBatchWriter implements BatchWrite {
    private static Logger log = LoggerFactory.getLogger(SolaceBatchWriter.class);
    private final StructType schema;
    private final Map<String, String> options;
    public SolaceBatchWriter(StructType schema, Map<String, String> options) {
        this.schema = schema;
        this.options = options;

        if(!options.containsKey("host") || options.get("host") == null || options.get("host").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Host name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Host name in configuration options");
        }
        if(!options.containsKey("vpn") || options.get("vpn") == null || options.get("vpn").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace VPN name in configuration options");
        }

        if(!options.containsKey("username") || options.get("username") == null || options.get("username").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Username in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Username in configuration options");
        }

        if(!options.containsKey("password") || options.get("password") == null || options.get("password").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Password in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Password in configuration options");
        }

        if(!options.containsKey("topic") || options.get("topic") == null || options.get("topic").isEmpty()) {
            log.error("SolaceSparkConnector - Please provide Solace Queue name in configuration options");
            throw new RuntimeException("SolaceSparkConnector - Please provide Solace Queue in configuration options");
        }
    }
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new SolaceDataWriterFactory(schema, options);
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {

    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {

    }
}
