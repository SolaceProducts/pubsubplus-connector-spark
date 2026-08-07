package com.solacecoe.connectors.spark.streaming.offset;

import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SolaceMessageTracker implements Serializable {
    private static ConcurrentHashMap<String, String> lastBatchId = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(SolaceMessageTracker.class);
    private static ConcurrentHashMap<String, CopyOnWriteArrayList<SolaceMessage>> messages = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> lastProcessedMessageId = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, CopyOnWriteArrayList<String>> processedMessageIds = new ConcurrentHashMap<>();

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
        processedMessageIds.remove(uniqueId);
    }

    public static void addMessage(String uniqueId, SolaceMessage message) {
        CopyOnWriteArrayList<SolaceMessage> messageList = new CopyOnWriteArrayList<>();
        if(messages.containsKey(uniqueId)) {
            messageList = messages.get(uniqueId);
        }
        messageList.addIfAbsent(message);
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
        lastProcessedMessageId.put(uniqueId, messageId);
        CopyOnWriteArrayList<String> idList = new CopyOnWriteArrayList<>();
        if(processedMessageIds.containsKey(uniqueId)) {
            idList = processedMessageIds.get(uniqueId);
        }
        idList.addIfAbsent(messageId);
        processedMessageIds.put(uniqueId, idList);
    }

//    public static boolean isMessageProcessed(String messageId) {
//        return lastProcessedMessageId.values().stream().anyMatch(id -> id.equals(messageId));
//    }

    public static boolean isMessageProcessed(String uniqueId, String messageId) {
        if(processedMessageIds.containsKey(uniqueId)) {
            return processedMessageIds.get(uniqueId).stream().anyMatch(messageId::equals);
        }

        return false;
    }

    public static void reset() {
        messages = new ConcurrentHashMap<>();
        lastProcessedMessageId = new ConcurrentHashMap<>();
        processedMessageIds.clear();
        logger.info("SolaceSparkConnector - Cleared all messages from Offset Manager");
    }

    public static void resetId(String uniqueId) {
        messages.remove(uniqueId);
        lastProcessedMessageId.remove(uniqueId);
        processedMessageIds.remove(uniqueId);
        logger.info("SolaceSparkConnector - Cleared all messages from Offset Manager for {}", uniqueId);
    }

    public static String getLastBatchId(String uniqueId) {
        return lastBatchId.get(uniqueId);
    }

    public static void setLastBatchId(String uniqueId, String batchId) {
        lastBatchId.put(uniqueId, batchId);
    }
}
