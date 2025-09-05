package com.solacecoe.connectors.spark.streaming.offset;

import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SolaceMessageTracker implements Serializable {
    private static String lastBatchId = "";
    private static final Logger logger = LogManager.getLogger(SolaceMessageTracker.class);
    private static ConcurrentHashMap<String, CopyOnWriteArrayList<SolaceMessage>> messages = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> lastProcessedMessageId = new ConcurrentHashMap<>();

    public static List<String> getIds() {
        return Collections.list(lastProcessedMessageId.keys());
    }
    public static String getProcessedMessagesIDs(String uniqueId) {
        if(lastProcessedMessageId.containsKey(uniqueId)) {
            return lastProcessedMessageId.get(uniqueId);
        }
        return null;
    }

    public static void removeProcessedMessagesIDs(String uniqueId) {
        lastProcessedMessageId.remove(uniqueId);
    }

    public static void addMessage(String uniqueId, SolaceMessage message) {
        CopyOnWriteArrayList<SolaceMessage> messageList = new CopyOnWriteArrayList<>();
        if(messages.containsKey(uniqueId)) {
            messageList = messages.get(uniqueId);
        }
        messageList.add(message);
        messages.put(uniqueId, messageList);
    }

    public static void ackMessages(String uniqueId) {
        if(messages.containsKey(uniqueId)) {
            messages.get(uniqueId).forEach(message -> {
                try {
                    message.bytesXMLMessage.ackMessage();
                } catch (IllegalStateException e) {
                    logger.error("SolaceSparkConnector - Exception encountered while acknowledging message to Solace. This may be due to the connection closing from inactivity in a long-running cluster. This can be safely ignored, as messages will be redelivered.", e);
                }
            });
            logger.info("SolaceSparkConnector - Acknowledged {} messages ", messages.get(uniqueId).size());
            messages.remove(uniqueId);
        }
    }

    public static CopyOnWriteArrayList<SolaceMessage> getMessages(String uniqueId) {
        return messages.get(uniqueId);
    }

    public static void addMessageID(String uniqueId, String messageId) {
        SolaceMessageTracker.lastProcessedMessageId.put(uniqueId, messageId);
    }

    public static boolean containsMessageID(String messageId) {
        return lastProcessedMessageId.values().stream().anyMatch(id -> id.equals(messageId));
    }

    public static void reset() {
        messages = new ConcurrentHashMap<>();
        lastProcessedMessageId = new ConcurrentHashMap<>();
        logger.info("SolaceSparkConnector - Cleared all messages from Offset Manager");
    }

    public static void resetId(String uniqueId) {
        messages.remove(uniqueId);
        lastProcessedMessageId.remove(uniqueId);
        logger.info("SolaceSparkConnector - Cleared all messages from Offset Manager for {}", uniqueId);
    }

    public static String getLastBatchId() {
        return lastBatchId;
    }

    public static void setLastBatchId(String lastBatchId) {
        SolaceMessageTracker.lastBatchId = lastBatchId;
    }
}
