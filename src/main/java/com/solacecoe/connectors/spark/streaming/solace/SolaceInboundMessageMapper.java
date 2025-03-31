package com.solacecoe.connectors.spark.streaming.solace;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class SolaceInboundMessageMapper implements InboundMessageMapper<SolaceRecord>, Serializable {
    private static final long serialVersionUID = 42L;
    private final String solaceOffsetIndicator;

    public SolaceInboundMessageMapper(String solaceOffsetIndicator) {
        this.solaceOffsetIndicator = solaceOffsetIndicator;
    }

    @Override
    public SolaceRecord map(BytesXMLMessage msg) throws Exception {
        Object2ObjectMap<String, Object> properties = null;
        SDTMap map = msg.getProperties();
        if (map != null) {
            properties = new Object2ObjectOpenHashMap<>();
            for (String key : map.keySet()) {
                properties.put(key, map.get(key));
            }
        }

        byte[] msgData = new byte[0];
        if(msg instanceof TextMessage) {
            TextMessage textMessage = ((TextMessage) msg);
            if(textMessage.getText() != null) {
                msgData = textMessage.getText().getBytes(StandardCharsets.UTF_8);
            } else if(textMessage.getContentLength() != 0) {
                msgData = textMessage.getBytes();
            } else if (textMessage.getAttachmentContentLength() != 0) {
                msgData = textMessage.getAttachmentByteBuffer().array();
            }
        } else {
            if (msg.getContentLength() != 0) {
                msgData = msg.getBytes();
            }
            if (msg.getAttachmentContentLength() != 0) {
                msgData = msg.getAttachmentByteBuffer().array();
            }
        }

        String messageID = SolaceUtils.getMessageID(msg, this.solaceOffsetIndicator);
        String partitionKey = "";
        if(msg.getProperties() != null && msg.getProperties().containsKey(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY)) {
            partitionKey = msg.getProperties().getString(XMLMessage.MessageUserPropertyConstants.QUEUE_PARTITION_KEY);
        }

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

