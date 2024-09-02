package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class SolaceInputPartitionNew implements InputPartition, Serializable {

    private final static Logger log = LogManager.getLogger(SolaceInputPartitionNew.class);
    private final String location;
    private final int id;
    private final int offsetId;
    public SolaceInputPartitionNew(int id, int offsetId, String location) {
        log.info("SolaceSparkConnector - Initializing Solace Input partition");
        this.id = id;
        this.offsetId = offsetId;
        this.location = location;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations");
        return new String[]{this.location};
    }

    public int getId() {
        return id;
    }

    public int getOffsetId() {
        return offsetId;
    }
}
