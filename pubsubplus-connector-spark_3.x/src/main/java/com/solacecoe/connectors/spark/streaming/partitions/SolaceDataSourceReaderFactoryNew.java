package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.streaming.MicroBatchExecution;
import org.apache.spark.sql.execution.streaming.StreamExecution;

import java.util.Map;

public class SolaceDataSourceReaderFactoryNew implements PartitionReaderFactory {

    private static final Logger log = LogManager.getLogger(SolaceDataSourceReaderFactoryNew.class);
    private final boolean includeHeaders;
    private Map<String, String> properties;
    public SolaceDataSourceReaderFactoryNew(boolean includeHeaders, Map<String, String> properties) {
        this.includeHeaders = includeHeaders;
        this.properties = properties;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        TaskContext taskCtx = TaskContext.get();
        String queryId = taskCtx.getLocalProperty(StreamExecution.QUERY_ID_KEY());
        String batchId = taskCtx.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY());
        SolaceInputPartitionNew solaceInputPartition = (SolaceInputPartitionNew) partition;
        log.info("SolaceSparkConnector - Creating reader for input partition reader factory with query id {}, batch id {}, task id {} and partition id {}", queryId, batchId, taskCtx.taskAttemptId(), taskCtx.partitionId());
        return new SolaceInputPartitionReaderNew(solaceInputPartition, includeHeaders, properties, taskCtx);
    }
}
