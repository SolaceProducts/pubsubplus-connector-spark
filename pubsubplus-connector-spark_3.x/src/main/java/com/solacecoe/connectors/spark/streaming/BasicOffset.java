package com.solacecoe.connectors.spark.streaming;

import org.apache.spark.sql.connector.read.streaming.Offset;

public class BasicOffset extends Offset {
    private final int offset;
    private String messageIDs = "NA";
    private static String previousMessageIDs = "";
    public BasicOffset(int offset, String messageIDs) {
        this.offset = offset;
        this.messageIDs = messageIDs;
    }
    @Override
    public String json() {
        String offsetString = "{\"offset\":" + offset + ", \"messageIDs\":\"" + previousMessageIDs + "\"}";
        if(!messageIDs.isEmpty()) {
            previousMessageIDs = messageIDs;
            offsetString = "{\"offset\":" + offset + ", \"messageIDs\":\"" + messageIDs + "\"}";
        }

        return offsetString;
    }

    public String getMessageIDs() {
        return messageIDs;
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
