package com.solacecoe.connectors.spark;

import com.solacecoe.connectors.spark.streaming.solace.utils.SolaceUtils;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

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

    private final String partitionKey;

    /**
     * Define a new Solace text record.
     */
    public SolaceRecord(String partitionKey, String destination, long expiration, String messageId,
                            int priority, boolean redelivered, String replyTo, long receiveTimestamp,
                            long senderTimestamp, Long sequenceNumber, long timeToLive,
                            Map<String, Object> properties, byte[] text) {
        this.partitionKey = partitionKey;
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


    public String getPartitionKey() {
        return partitionKey;
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
}