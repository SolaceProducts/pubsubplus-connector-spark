package com.solacecoe.connectors.spark.streaming.write;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.solace.utils.SolacePublishStatus;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.Serializable;

public class SolaceDataWriterCommitMessage implements WriterCommitMessage, Serializable {
    private SolacePublishStatus solacePublishStatus;
    private String reason;

    public SolaceDataWriterCommitMessage(SolacePublishStatus solacePublishStatus, String reason) {
        this.solacePublishStatus = solacePublishStatus;
        this.reason = reason;
    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this, SolaceDataWriterCommitMessage.class);
    }
}
