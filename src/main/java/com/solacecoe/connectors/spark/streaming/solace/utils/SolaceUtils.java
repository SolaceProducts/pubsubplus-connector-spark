package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTException;

public class SolaceUtils {

    public static String getMessageID(BytesXMLMessage message, String solaceOffsetIndicator) throws SDTException {
        switch (solaceOffsetIndicator) {
            case "CORRELATION_ID":
                if(message.getCorrelationId() == null || message.getCorrelationId().length() == 0) {
                    throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator CORRELATION_ID is null or empty");
                }
                return message.getCorrelationId();
            case "APPLICATION_MESSAGE_ID":
                if(message.getApplicationMessageId() == null || message.getApplicationMessageId().length() == 0) {
                    throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator APPLICATION_MESSAGE_ID is null or empty");
                }
                return message.getApplicationMessageId();
//            case "SEQUENCE_NUMBER":
//                return Long.toString(message.getSequenceNumber());
            default:
                if(solaceOffsetIndicator.equals("MESSAGE_ID")) {
                    return message.getReplicationGroupMessageId().toString();
                }

                if(message.getProperties() != null && message.getProperties().containsKey(solaceOffsetIndicator)) {
                    if(message.getProperties().get(solaceOffsetIndicator) == null || message.getProperties().get(solaceOffsetIndicator).toString().length() == 0) {
                        throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator " + solaceOffsetIndicator + " is null or empty");
                    }
                    return message.getProperties().get(solaceOffsetIndicator).toString();
                } else {
                    throw new RuntimeException("SolaceSparkConnector - Unable to find Solace Offset Indicator in message headers");
                }
        }
    }
}
