package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.Map;

public class SolaceDataSourceReaderFactoryNew implements PartitionReaderFactory {

    private static final Logger log = LogManager.getLogger(SolaceDataSourceReaderFactoryNew.class);
    private final boolean includeHeaders;
    private final Map<String, String> properties;

    public SolaceDataSourceReaderFactoryNew(boolean includeHeaders, Map<String, String> properties) {
        this.includeHeaders = includeHeaders;
        this.properties = properties;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        SolaceInputPartitionNew solaceInputPartition = (SolaceInputPartitionNew) partition;
        log.info("SolaceSparkConnector - Creating reader for input partition reader factory for id {}", solaceInputPartition.getId());
        return new SolaceInputPartitionReaderNew(solaceInputPartition, includeHeaders, properties);
    }
}
