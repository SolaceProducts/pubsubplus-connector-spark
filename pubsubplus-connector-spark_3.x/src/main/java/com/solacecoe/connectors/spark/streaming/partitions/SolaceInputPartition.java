package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.ExecutorCacheTaskLocation;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.BlockManagerMaster;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SolaceInputPartition implements InputPartition, Serializable {

    private final static Logger log = LogManager.getLogger(SolaceInputPartition.class);
    private final int partitionHashCode;
    private final String id;
    private final int offsetId;
    private final List<String> executorList;
    public SolaceInputPartition(int partitionHashCode, int offsetId, List<String> executorList) {
        this.partitionHashCode = partitionHashCode;
        this.id = Integer.toString(partitionHashCode);
        this.executorList = executorList;
        log.info("SolaceSparkConnector - Initializing Solace Input partition with id {}", id);
        this.offsetId = offsetId;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations for input partition {}", id);
        Optional<String> executorLocation = this.getExecutorLocation(this.executorList, this.partitionHashCode);
        return executorLocation.map(s -> new String[]{s}).orElseGet(() -> new String[]{""});
    }

    public String getId() {
        return id;
    }

    public int getOffsetId() {
        return offsetId;
    }

    // Equivalent of floorMod function
    public static int floorMod(long a, int b) {
        return (int)((a % b + b) % b);
    }

    private Optional<String> getExecutorLocation(List<String> executorLocations, int partitionHashCode) {
        int numExecutors = executorLocations.size();

        if (numExecutors > 0) {
            int executorIndex = floorMod(partitionHashCode, numExecutors);
            log.info("SolaceSparkConnector - Preferred location for partition {} is at executor {}", partitionHashCode, executorLocations.get(executorIndex));
            return Optional.of(executorLocations.get(executorIndex));
        } else {
            log.info("SolaceSparkConnector - No Executors present");
            return Optional.empty();
        }
    }
}
