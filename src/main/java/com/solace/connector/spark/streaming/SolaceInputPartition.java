package com.solace.connector.spark.streaming;


import com.solace.connector.spark.SolaceRecord;
import com.solace.connector.spark.streaming.solace.AppSingleton;
import com.solace.connector.spark.streaming.solace.SolaceMessage;
import org.apache.spark.sql.connector.read.InputPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SolaceInputPartition implements InputPartition {

    private static Logger log = LoggerFactory.getLogger(SolaceInputPartition.class);
    private int value = 0;
    private String location;

    private List<SolaceRecord> solaceRecords;
//    int end = 0;
    public SolaceInputPartition(int start, String location, List<SolaceRecord> solaceRecords) {
        log.info("SolaceSparkConnector - Initializing Solace Input partition");
        this.value = start;
        this.location = location;
        this.solaceRecords = solaceRecords;
//        this.end = end;
    }

    @Override
    public String[] preferredLocations() {
        log.info("SolaceSparkConnector - Getting preferred locations");
        return new String[]{this.location};
    }

    public List<SolaceRecord> getValues() {
        return this.solaceRecords;
    }
}
