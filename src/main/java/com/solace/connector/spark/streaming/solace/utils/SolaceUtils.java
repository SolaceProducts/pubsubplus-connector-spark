package com.solace.connector.spark.streaming.solace.utils;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTException;

public class SolaceUtils {

    public static String getMessageID(BytesXMLMessage message, String solaceOffsetIndicator) throws SDTException {
        switch (solaceOffsetIndicator) {
            case "CORRELATION_ID":
                return message.getCorrelationId();
            case "APPLICATION_MESSAGE_ID":
                return message.getApplicationMessageId();
//            case "SEQUENCE_NUMBER":
//                return Long.toString(message.getSequenceNumber());
            default:
                if(solaceOffsetIndicator.equals("MESSAGE_ID")) {
                    return message.getMessageId();
                }

                if(message.getProperties().containsKey(solaceOffsetIndicator)) {
                    return message.getProperties().get(solaceOffsetIndicator).toString();
                } else {
                    throw new RuntimeException("SolaceSparkConnector - Unable to find Solace Offset Indicator in message headers");
                }
        }
    }
}
