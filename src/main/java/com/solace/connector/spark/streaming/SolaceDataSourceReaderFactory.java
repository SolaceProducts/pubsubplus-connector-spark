package com.solace.connector.spark.streaming;

import com.solace.connector.spark.SolaceRecord;
import com.solace.connector.spark.streaming.solace.AppSingleton;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SolaceDataSourceReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(SolaceDataSourceReaderFactory.class);
    private int batchSize;
    private String offsetJson;

    public SolaceDataSourceReaderFactory(int batchSize, String offsetJson) {
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
        this.batchSize = batchSize;
        this.offsetJson = offsetJson;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        SolaceInputPartition solaceInputPartition = (SolaceInputPartition) partition;
        log.info("SolaceSparkConnector - Creating reader for input partition reader factory");
        return new SolaceInputPartitionReader(solaceInputPartition, batchSize, offsetJson);
    }
}
