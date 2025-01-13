package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.google.gson.Gson;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.Serializable;

public class SolaceWriterCommitMessage implements WriterCommitMessage, Serializable {
    private SolacePublishStatus solacePublishStatus;
    private String reason;

    public SolaceWriterCommitMessage(SolacePublishStatus solacePublishStatus, String reason) {
        this.solacePublishStatus = solacePublishStatus;
        this.reason = reason;
    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this, SolaceWriterCommitMessage.class);
    }
}
