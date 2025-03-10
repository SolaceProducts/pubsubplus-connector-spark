package com.solacecoe.connectors.spark.streaming.offset;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SolaceMessageTracker implements Serializable {
    private static final Logger logger = LogManager.getLogger(SolaceMessageTracker.class);
    private static ConcurrentHashMap<String, CopyOnWriteArrayList<SolaceMessage>> messages = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> lastProcessedMessageId = new ConcurrentHashMap<>();
    private static CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoint = new CopyOnWriteArrayList<>();

    public static String getProcessedMessagesIDs(String uniqueId) {
        if(lastProcessedMessageId.containsKey(uniqueId)) {
            return lastProcessedMessageId.get(uniqueId);
        }
        return null;
    }

    public static void removeProcessedMessagesIDs(String uniqueId) {
        lastProcessedMessageId.remove(uniqueId);
    }

    public static void addMessage(String id, SolaceMessage message) {
        CopyOnWriteArrayList<SolaceMessage> messageList = new CopyOnWriteArrayList<>();
        if(messages.containsKey(id)) {
            messageList = messages.get(id);
        }
        messageList.add(message);
        messages.put(id, messageList);
    }

    public static void ackMessages(String uniqueId) {
        if(messages.containsKey(uniqueId)) {
            messages.get(uniqueId).forEach(message -> message.bytesXMLMessage.ackMessage());
            messages.remove(uniqueId);
        }
    }

    public static void addMessageID(String id, String messageId) {
        SolaceMessageTracker.lastProcessedMessageId.put(id, messageId);
    }

    public static boolean containsMessageID(String messageId) {
        return lastProcessedMessageId.values().stream().anyMatch(id -> id.equals(messageId));
    }

    public static void reset() {
        messages = new ConcurrentHashMap<>();
        lastProcessedMessageId = new ConcurrentHashMap<>();
        checkpoint = new CopyOnWriteArrayList<>();
        logger.info("SolaceSparkConnector - Cleared all messages from Offset Manager");
    }

    public static void updateCheckpoint(SolaceSparkPartitionCheckpoint solaceSparkPartitionCheckpoint) {
        SolaceSparkPartitionCheckpoint existingCheckpoint = checkpoint.stream().filter(entry -> entry.getPartitionId().equals(solaceSparkPartitionCheckpoint.getPartitionId())).findFirst().orElse(null);
        if(existingCheckpoint != null) {
            checkpoint.remove(existingCheckpoint);
        }
        checkpoint.add(solaceSparkPartitionCheckpoint);
    }

    public static SolaceSparkPartitionCheckpoint getCheckpoint(String partitionId) {
        return checkpoint.stream().filter(entry -> entry.getPartitionId().equals(partitionId)).findFirst().orElse(null);
    }

    public static String getCheckpoint() {
        return new Gson().toJson(checkpoint);
    }
}
