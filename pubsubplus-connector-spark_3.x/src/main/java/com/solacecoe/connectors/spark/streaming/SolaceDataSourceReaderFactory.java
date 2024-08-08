package com.solacecoe.connectors.spark.streaming;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class SolaceDataSourceReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LogManager.getLogger(SolaceDataSourceReaderFactory.class);
    private final boolean includeHeaders;

    public SolaceDataSourceReaderFactory(boolean includeHeaders) {
        this.includeHeaders = includeHeaders;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        SolaceInputPartition solaceInputPartition = (SolaceInputPartition) partition;
        log.info("SolaceSparkConnector - Creating reader for input partition reader factory");
        return new SolaceInputPartitionReader(solaceInputPartition, includeHeaders);
    }
}
