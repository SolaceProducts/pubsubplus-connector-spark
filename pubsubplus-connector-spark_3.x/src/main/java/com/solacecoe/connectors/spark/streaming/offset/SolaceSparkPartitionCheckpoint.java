package com.solacecoe.connectors.spark.streaming.offset;

import java.io.Serializable;

public class SolaceSparkPartitionCheckpoint implements Serializable {
    private String partitionId = "";
    private String messageIDs = "";

    public SolaceSparkPartitionCheckpoint(String messageIDs, String partitionId) {
        this.messageIDs = messageIDs;
        this.partitionId = partitionId;
    }

    public String getMessageIDs() {
        return messageIDs;
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
