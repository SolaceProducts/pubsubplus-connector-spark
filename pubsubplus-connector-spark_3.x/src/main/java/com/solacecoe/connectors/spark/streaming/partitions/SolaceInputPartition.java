package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class SolaceInputPartition implements InputPartition, Serializable {

    private final static Logger log = LogManager.getLogger(SolaceInputPartition.class);
    private final String location;
    private final String id;
    private final int offsetId;
    public SolaceInputPartition(String id, int offsetId, String location) {
        log.info("SolaceSparkConnector - Initializing Solace Input partition with id {}", id);
        this.id = id;
        this.offsetId = offsetId;
        this.location = location;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations");
        return new String[]{this.location};
    }

    public String getId() {
        return id;
    }

    public int getOffsetId() {
        return offsetId;
    }
}
