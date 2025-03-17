package com.solacecoe.connectors.spark.streaming.offset;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.SerializedOffset;

import java.util.concurrent.CopyOnWriteArrayList;

public class SolaceSourceOffset extends Offset {
    private int offset;
    private final CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints;

    public SolaceSourceOffset(int offset, CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoints) {
        this.offset = offset;
        this.checkpoints = checkpoints;
    }
    
    @Override
    public String json() {
        String checkpointsJson = new Gson().toJson(checkpoints);
        return "{\"offset\":" + offset + ",\"checkpoints\":" + checkpointsJson + "}";
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> getCheckpoints() {
        return checkpoints;
    }
}
