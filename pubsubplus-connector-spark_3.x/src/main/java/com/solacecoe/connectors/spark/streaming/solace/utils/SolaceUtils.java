package com.solacecoe.connectors.spark.streaming.solace.utils;

import com.solacecoe.connectors.spark.streaming.properties.SolaceSparkStreamingProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTException;

public class SolaceUtils {

    public static String getMessageID(BytesXMLMessage message, String solaceOffsetIndicator) throws SDTException {
        switch (solaceOffsetIndicator) {
            case "CORRELATION_ID":
                if(message.getCorrelationId() == null || message.getCorrelationId().isEmpty()) {
                    throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator CORRELATION_ID is null or empty");
                }
                return message.getCorrelationId();
            case "APPLICATION_MESSAGE_ID":
                if(message.getApplicationMessageId() == null || message.getApplicationMessageId().isEmpty()) {
                    throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator APPLICATION_MESSAGE_ID is null or empty");
                }
                return message.getApplicationMessageId();
//            case "SEQUENCE_NUMBER":
//                return Long.toString(message.getSequenceNumber());
            default:
                if(solaceOffsetIndicator.equals(SolaceSparkStreamingProperties.OFFSET_INDICATOR_DEFAULT)) {
                    return message.getReplicationGroupMessageId().toString();
                }

                if(message.getProperties() != null && message.getProperties().containsKey(solaceOffsetIndicator)) {
                    if(message.getProperties().get(solaceOffsetIndicator) == null || message.getProperties().get(solaceOffsetIndicator).toString().isEmpty()) {
                        throw new RuntimeException("SolaceSparkConnector - Configured Offset Indicator " + solaceOffsetIndicator + " is null or empty");
                    }
                    return message.getProperties().get(solaceOffsetIndicator).toString();
                } else {
                    throw new RuntimeException("SolaceSparkConnector - Unable to find Solace Offset Indicator in message headers");
                }
        }
    }
}
