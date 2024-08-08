package com.solacecoe.connectors.spark.streaming.partitions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;
import java.util.Map;

public class SolaceInputPartitionNew implements InputPartition, Serializable {

    private final static Logger log = LogManager.getLogger(SolaceInputPartitionNew.class);
    private final String location;
    private final int id;
//    private final LinkedBlockingQueue<SolaceMessage> queue;
    public SolaceInputPartitionNew(int id, String location, Map<String, String> properties) {
        log.info("SolaceSparkConnector - Initializing Solace Input partition");
        this.id = id;
        this.location = location;
//        this.queue = queue;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations");
        return new String[]{this.location};
    }

    public int getId() {
        return id;
    }

//    public LinkedBlockingQueue<SolaceMessage> getQueue() {
//        return queue;
//    }
}
