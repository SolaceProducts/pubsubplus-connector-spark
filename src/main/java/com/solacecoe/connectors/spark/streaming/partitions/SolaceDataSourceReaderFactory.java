package com.solacecoe.connectors.spark.streaming.partitions;

import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.streaming.MicroBatchExecution;
import org.apache.spark.sql.execution.streaming.StreamExecution;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceDataSourceReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LogManager.getLogger(SolaceDataSourceReaderFactory.class);
    private final boolean includeHeaders;
    private final Map<String, String> properties;
    private final String checkpointLocation;
    private final CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    private final SerializableConfiguration serializableConfiguration;
    public SolaceDataSourceReaderFactory(boolean includeHeaders, Map<String, String> properties, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints, String checkpointLocation, SerializableConfiguration serializableConfiguration) {
        this.includeHeaders = includeHeaders;
        this.properties = properties;
        this.checkpoints = checkpoints;
        this.checkpointLocation = checkpointLocation;
        this.serializableConfiguration = serializableConfiguration;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            TaskContext taskCtx = TaskContext.get();
            String queryId = taskCtx.getLocalProperty(StreamExecution.QUERY_ID_KEY());
            String batchId = taskCtx.getLocalProperty(MicroBatchExecution.BATCH_ID_KEY());
            SolaceInputPartition solaceInputPartition = (SolaceInputPartition) partition;
            log.info("SolaceSparkConnector - Creating reader for input partition reader factory with query id {}, batch id {}, task id {} and partition id {}", queryId, batchId, taskCtx.taskAttemptId(), taskCtx.partitionId());
            return new SolaceInputPartitionReader(solaceInputPartition, includeHeaders, properties, taskCtx, checkpoints, checkpointLocation, serializableConfiguration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
