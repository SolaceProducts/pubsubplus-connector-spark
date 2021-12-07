package com.solace.connector.spark;

import org.apache.spark.sql.connector.read.InputPartition;

import java.util.List;

public class SolaceInputPartition implements InputPartition {

    private List<SolaceRecord> payload;
    private String location;
    public SolaceInputPartition(List<SolaceRecord> data, String location){
        this.payload = data;
        this.location = location;
    }

    @Override
    public String[] preferredLocations() {
        return new String[]{this.location};
    }

    public List<SolaceRecord> getValues() {
        return this.payload;
    }
}
