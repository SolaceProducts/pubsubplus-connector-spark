package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public class SolaceInputPartition implements InputPartition, Serializable {

    private final static Logger log = LogManager.getLogger(SolaceInputPartition.class);
    private final List<String> executorLocations;
    private final int partitionHashCode;
    private final String id;
    private final int offsetId;
    public SolaceInputPartition(int partitionHashCode, int offsetId, List<String> executorLocations) {
        this.partitionHashCode = partitionHashCode;
        this.id = Integer.toString(partitionHashCode);
        log.info("SolaceSparkConnector - Initializing Solace Input partition with id {}", id);
        this.offsetId = offsetId;
        this.executorLocations = executorLocations;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations");
        Optional<String> executorLocation = getExecutorLocation(this.executorLocations, this.partitionHashCode);
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

    public static Optional<String> getExecutorLocation(List<String> executorLocations, int partitionHashCode) {
        int numExecutors = executorLocations.size();

        if (numExecutors > 0) {
            int executorIndex = floorMod(partitionHashCode, numExecutors);
            log.info("Preferred location for partition {} is at executor {}", partitionHashCode, executorLocations.get(executorIndex));
            return Optional.of(executorLocations.get(executorIndex));
        } else {
            return Optional.empty();
        }
    }
}
