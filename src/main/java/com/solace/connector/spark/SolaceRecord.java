package com.solace.connector.spark;

import com.solace.connector.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ReplicationGroupMessageId;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.impl.ReplicationGroupMessageIdImpl;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SolaceRecord implements Serializable {
    private static final Logger log = Logger.getLogger(SolaceRecord.class);
    private static final long serialVersionUID = 42L;

    // Application properties
    private final Map<String, Object> properties;
    private final byte[] text;

    private final String destination;
    private final long expiration;
    private final String messageId;
    private final int priority;
    private final boolean redelivered;
    private final String replyTo;
    private final long receiveTimestamp;
    private final long senderTimestamp;
    private final Long sequenceNumber;
    private final long timeToLive;

    /**
     * Define a new Solace text record.
     */
    public SolaceRecord(String destination, long expiration, String messageId,
                            int priority, boolean redelivered, String replyTo, long receiveTimestamp,
                            long senderTimestamp, Long sequenceNumber, long timeToLive,
                            Map<String, Object> properties, byte[] text) {
        this.destination = destination;
        this.expiration = expiration;
        this.messageId = messageId;
        this.priority = priority;
        this.redelivered = redelivered;
        this.replyTo = replyTo;
        this.receiveTimestamp = receiveTimestamp;
        this.senderTimestamp = senderTimestamp;
        this.sequenceNumber = sequenceNumber;
        this.timeToLive = timeToLive;
        this.properties = properties;
        this.text = text;
    }

    /**
     * Return the text record destination.
     */
    public String getDestination() {
        return destination;
    }

    /**
     * Return the text record expiration.
     */
    public long getExpiration() {
        return expiration;
    }

    /**
     * Return the text record messageId.
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Return the text record priority.
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Return the text record properties.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * Return the text record redelivered.
     */
    public boolean isRedelivered() {
        return redelivered;
    }

    /**
     * Return the text record receiveTimestamp.
     */
    public long getReceiveTimestamp() {
        return receiveTimestamp;
    }

    /**
     * Return the text record replyTo.
     */
    public String getReplyTo() {
        return replyTo;
    }

    /**
     * Return the text record senderId.
     */
    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Return the text record senderTimestamp.
     */
    public long getSenderTimestamp() {
        return senderTimestamp;
    }

    /**
     * Return the text record serialversionuid.
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * Return the text record payload content.
     */
    public byte[] getPayload() {
        return text;
    }

    /**
     * Return the the text record timeToLive.
     */
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, expiration, messageId, priority,
                redelivered, replyTo, receiveTimestamp, senderTimestamp,
                sequenceNumber, timeToLive, properties, text);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SolaceRecord) {
            SolaceRecord other = (SolaceRecord) obj;

            boolean p2p = false;
            if (properties != null) {
                p2p = properties.equals(other.properties);
            } else {
                if (other.properties == null) {
                    p2p = true;
                }
            }

            return p2p && destination.equals(other.destination)
                    && redelivered == other.redelivered
                    && expiration == other.expiration
                    && priority == other.priority
                    && text.equals(other.text);

        } else {
            return false;
        }
    }

    public static Mapper getMapper(String solaceOffsetIndicator) {
        return new Mapper(solaceOffsetIndicator);
    }

    public static class Mapper implements InboundMessageMapper<SolaceRecord> {
        private static final long serialVersionUID = 42L;
        private String solaceOffsetIndicator;

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
            // log.info("SolaceSparkConnector - Received Message ID String in Input partition - " + msg.getMessageId());
            return new SolaceRecord(
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

    @FunctionalInterface
    public interface InboundMessageMapper<T> extends Serializable {
        T map(BytesXMLMessage message) throws Exception;
    }
}