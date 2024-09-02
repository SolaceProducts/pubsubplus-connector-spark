package com.solacecoe.connectors.spark.streaming.offset;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.spark.sql.connector.read.streaming.Offset;

public class SolaceSparkOffset extends Offset {
    private final int offset;
    private String queryId = "";
    private String batchId = "";
    private String stageId = "";
    private String partitionId = "";
    private String messageIDs = "";
    public SolaceSparkOffset(int offset, String messageIDs) {
        this.offset = offset;
        this.messageIDs = messageIDs;
    }

    public SolaceSparkOffset(int offset, String queryId, String batchId, String stageId, String partitionId, String messageIDs) {
        this.offset = offset;
        this.queryId = queryId;
        this.batchId = batchId;
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.messageIDs = messageIDs;
    }
    @Override
    public String json() {
        Gson gson = new GsonBuilder().create();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("offset", offset);
        jsonObject.addProperty("queryId", queryId);
        jsonObject.addProperty("batchId", batchId);
        jsonObject.addProperty("stageId", stageId);
        jsonObject.addProperty("partitionId", partitionId);
        String offsetString = gson.toJson(jsonObject);
        if(!messageIDs.isEmpty()) {
            jsonObject.addProperty("messageIDs", messageIDs);
            offsetString = gson.toJson(jsonObject);
        }

        return offsetString;
    }

    public String getMessageIDs() {
        return messageIDs;
    }

    public int getOffset() {
        return offset;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getStageId() {
        return stageId;
    }

    public String getPartitionId() {
        return partitionId;
    }

    //    @Override
//    public String toString() {
//        return "BasicOffset[" + offset + "]";
//    }
//
//    @Override
//    public boolean equals(Object obj) {
//        BasicOffset basicOffset = (BasicOffset) obj;
//        if (this.offset == basicOffset.offset)
//            return true;
//        else
//            return false;
//    }
}
