package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.google.gson.Gson;

import java.io.Serializable;

public class SolaceAbortMessage implements Serializable {
    private final SolacePublishStatus solacePublishStatus;
    private final String reason;

    public SolaceAbortMessage(SolacePublishStatus solacePublishStatus, String reason) {
        this.solacePublishStatus = solacePublishStatus;
        this.reason = reason;
    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this, SolaceAbortMessage.class);
    }
}
