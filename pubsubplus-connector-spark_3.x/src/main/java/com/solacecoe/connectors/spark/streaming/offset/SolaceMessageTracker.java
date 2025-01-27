package com.solacecoe.connectors.spark.streaming.offset;

import com.google.gson.Gson;
import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SolaceMessageTracker implements Serializable {
    private static final Logger logger = LogManager.getLogger(SolaceMessageTracker.class);
    private static ConcurrentHashMap<String, CopyOnWriteArrayList<SolaceMessage>> messages = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, CopyOnWriteArrayList<String>> messageIDs = new ConcurrentHashMap<>();
    private static final CopyOnWriteArrayList<SolaceSparkPartitionCheckpoint> checkpoint = new CopyOnWriteArrayList<>();

    public static String getProcessedMessagesIDs(String uniqueId) {
        if(messageIDs.containsKey(uniqueId)) {
            return String.join(",", messageIDs.get(uniqueId));
        }
        return null;
    }

    public static void removeProcessedMessagesIDs(String uniqueId) {
        messageIDs.remove(uniqueId);
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
            messages.get(uniqueId).forEach(message -> {
                message.bytesXMLMessage.ackMessage();
            });
            messages.remove(uniqueId);
        }
    }

    public static void addMessageID(String id, String messageId) {
        CopyOnWriteArrayList<String> messageIDList = new CopyOnWriteArrayList<>();
        if (messageIDs.containsKey(id)) {
            messageIDList = SolaceMessageTracker.messageIDs.get(id);
        }
        messageIDList.add(messageId);
        SolaceMessageTracker.messageIDs.put(id, messageIDList);
    }

    public static List<String> getMessageIDs(String uniqueId) {
        if(messageIDs.containsKey(uniqueId)) {
            return messageIDs.get(uniqueId);
        }

        return new ArrayList<>();
    }

    public static boolean containsMessageID(String messageId) {
        return messageIDs.values().stream().anyMatch(list -> list.contains(messageId));
    }

    public static void reset() {
        messages = new ConcurrentHashMap<>();
        messageIDs = new ConcurrentHashMap<>();
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

//    public static ConcurrentHashMap<String, String> getMessages() {
//        return messages;
//    }
}
