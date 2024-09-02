package com.solacecoe.connectors.spark.streaming.offset;

import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class SolaceSparkOffsetManager implements Serializable {
    private static final ConcurrentHashMap<String, List<SolaceMessage>> messages = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<String>> messageIDs = new ConcurrentHashMap<>();

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
        List<SolaceMessage> messageList = new ArrayList<>();
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
        List<String> messageIDList = new ArrayList<>();
        if (messageIDs.containsKey(id)) {
            messageIDList = SolaceSparkOffsetManager.messageIDs.get(id);
        }
        messageIDList.add(messageId);
        SolaceSparkOffsetManager.messageIDs.put(id, messageIDList);
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

//    public static ConcurrentHashMap<String, String> getMessages() {
//        return messages;
//    }
}
