package com.solacecoe.connectors.spark.streaming;


import com.solacecoe.connectors.spark.SolaceRecord;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceInputPartition implements InputPartition {

    private static Logger log = LogManager.getLogger(SolaceInputPartition.class);
    private final String location;
    private final int id;
    private final CopyOnWriteArrayList<SolaceRecord> solaceRecords;

    public SolaceInputPartition(int id, String location, CopyOnWriteArrayList<SolaceRecord> solaceRecords) {
        log.info("SolaceSparkConnector - Initializing Solace Input partition");
        this.id = id;
        this.location = location;
        this.solaceRecords = solaceRecords;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations");
        return new String[]{this.location};
    }

    public CopyOnWriteArrayList<SolaceRecord> getValues() {
        return this.solaceRecords;
    }

    public int getId() {
        return id;
    }

}
