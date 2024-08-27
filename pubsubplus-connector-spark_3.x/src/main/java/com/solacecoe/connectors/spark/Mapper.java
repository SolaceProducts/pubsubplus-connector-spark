package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class Mapper implements InboundMessageMapper<SolaceRecord>, Serializable {
    private static final long serialVersionUID = 42L;
    private final String solaceOffsetIndicator;

    public Mapper(String solaceOffsetIndicator) {
        this.solaceOffsetIndicator = solaceOffsetIndicator;
    }

    @Override
    public SolaceRecord map(BytesXMLMessage msg) throws Exception {
        Map<String, Object> properties = null;
        SDTMap map = msg.getProperties();
        if (map != null) {
            properties = new HashMap<>();
            for (String key : map.keySet()) {
                properties.put(key, map.get(key));
            }
        }
        byte[] msgData = new byte[0];
        if (msg.getContentLength() != 0) {
            msgData = msg.getBytes();
        }
        if (msg.getAttachmentContentLength() != 0) {
            msgData = msg.getAttachmentByteBuffer().array();
        }

        String messageID = SolaceUtils.getMessageID(msg, this.solaceOffsetIndicator);
        String partitionKey = "";
        if(msg.getProperties() != null && msg.getProperties().containsKey(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY)) {
            partitionKey = msg.getProperties().getString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY);
        }
        // log.info("SolaceSparkConnector - Received Message ID String in Input partition - " + msg.getMessageId());
        return new SolaceRecord(
                partitionKey,
                msg.getDestination().getName(),
                msg.getExpiration(),
                messageID,
                msg.getPriority(),
                msg.getRedelivered(),
                // null means no replyto property
                (msg.getReplyTo() != null) ? msg.getReplyTo().getName() : null,
                msg.getReceiveTimestamp(),
                // 0 means no SenderTimestamp
                (msg.getSenderTimestamp() != null) ? msg.getSenderTimestamp() : 0,
                msg.getSequenceNumber(),
                msg.getTimeToLive(),
                properties,
                msgData);
    }
}

