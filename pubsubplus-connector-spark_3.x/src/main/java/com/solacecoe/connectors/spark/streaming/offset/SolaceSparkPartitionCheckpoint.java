package com.solacecoe.connectors.spark.streaming.offset;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SolaceSparkPartitionCheckpoint {
    private int startOffset = 0;
    private int endOffset = 0;
//    private String queryId = "";
//    private String batchId = "";
//    private String stageId = "";
    private String partitionId = "";
    private String messageIDs = "";
    public SolaceSparkPartitionCheckpoint(int offset, String messageIDs) {
        this.messageIDs = messageIDs;
    }

//    public SolaceSparkOffset(int offset, String queryId, String batchId, String stageId, String partitionId, String messageIDs) {
//        this.offset = offset;
//        this.queryId = queryId;
//        this.batchId = batchId;
//        this.stageId = stageId;
//        this.partitionId = partitionId;
//        this.messageIDs = messageIDs;
//    }

    public SolaceSparkPartitionCheckpoint(int startOffset, int endOffset, String messageIDs, String partitionId) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.messageIDs = messageIDs;
        this.partitionId = partitionId;
    }

    public String json() {
        Gson gson = new GsonBuilder().create();
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("startOffset", startOffset);
        jsonObject.addProperty("endOffset", endOffset);
        String offsetString = gson.toJson(jsonObject);
        if(messageIDs != null && !messageIDs.isEmpty()) {
            jsonObject.addProperty("messageIDs", messageIDs);
            offsetString = gson.toJson(jsonObject);
        }

        return offsetString;
    }

    public String getMessageIDs() {
        return messageIDs;
    }


    public int getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(int startOffset) {
        this.startOffset = startOffset;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(int endOffset) {
        this.endOffset = endOffset;
    }

    public void setMessageIDs(String messageIDs) {
        this.messageIDs = messageIDs;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    //    public String getQueryId() {
//        return queryId;
//    }
//
//    public String getBatchId() {
//        return batchId;
//    }
//
//    public String getStageId() {
//        return stageId;
//    }
//
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
