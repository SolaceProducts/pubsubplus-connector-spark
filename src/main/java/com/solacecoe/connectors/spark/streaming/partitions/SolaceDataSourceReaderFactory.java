package com.solacecoe.connectors.spark.streaming.partitions;

import com.solacecoe.connectors.spark.streaming.offset.SolaceSparkPartitionCheckpoint;
import com.solacecoe.connectors.spark.streaming.properties.SparkStreamingTaskProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceDataSourceReaderFactory implements PartitionReaderFactory {

    private static final Logger log = LoggerFactory.getLogger(SolaceDataSourceReaderFactory.class);
    private final boolean includeHeaders;
    private final boolean isDatabricks;
    private final boolean isUCVolume;
    private final Map<String, String> properties;
    private final String checkpointLocation;
    private final CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;
    public SolaceDataSourceReaderFactory(boolean includeHeaders, boolean isDatabricks, boolean isUCVolume, Map<String, String> properties, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints, String checkpointLocation) {
        this.includeHeaders = includeHeaders;
        this.isDatabricks = isDatabricks;
        this.isUCVolume = isUCVolume;
        this.properties = properties;
        this.checkpoints = checkpoints;
        this.checkpointLocation = checkpointLocation;
        log.info("SolaceSparkConnector - Initializing Partition reader factory");
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            TaskContext taskCtx = TaskContext.get();
            String queryId = taskCtx.getLocalProperty(SparkStreamingTaskProperties.QUERY_ID_KEY);
            String batchId = taskCtx.getLocalProperty(SparkStreamingTaskProperties.BATCH_ID_KEY);
            SolaceInputPartition solaceInputPartition = (SolaceInputPartition) partition;
            log.info("SolaceSparkConnector - Creating reader for input partition reader factory with query id {}, batch id {}, task id {} and partition id {}", queryId, batchId, taskCtx.taskAttemptId(), taskCtx.partitionId());
            return new SolaceInputPartitionReader(solaceInputPartition, includeHeaders, isDatabricks, isUCVolume, properties, taskCtx, checkpoints, checkpointLocation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
