package com.solacecoe.connectors.spark.streaming.offset;

public class SolaceSparkPartitionCheckpoint {
    private int startOffset = 0;
    private int endOffset = 0;
    private String partitionId = "";
    private String messageIDs = "";

    public SolaceSparkPartitionCheckpoint(int startOffset, int endOffset, String messageIDs, String partitionId) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.messageIDs = messageIDs;
        this.partitionId = partitionId;
    }

    public String getMessageIDs() {
        return messageIDs;
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

    public String getPartitionId() {
        return partitionId;
    }
}
