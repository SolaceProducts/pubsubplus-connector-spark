package com.solacecoe.connectors.spark.streaming.partitions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class SolaceInputPartition implements InputPartition, Serializable {

    private static final Logger log = LoggerFactory.getLogger(SolaceInputPartition.class);
    private final String id;
    private final String preferredLocation;
    public SolaceInputPartition(int partitionHashCode, String preferredLocation) {
        this.id = Integer.toString(partitionHashCode);
        this.preferredLocation = preferredLocation;
        log.info("SolaceSparkConnector - Initializing Solace Input partition with id {}", id);
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations for input partition {}", id);
        return new String[]{preferredLocation};
    }

    public String getId() {
        return id;
    }

    public String getPreferredLocation() {
        return preferredLocation;
    }
}
