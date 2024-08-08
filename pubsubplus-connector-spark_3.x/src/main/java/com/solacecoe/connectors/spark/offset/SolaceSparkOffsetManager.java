package com.solacecoe.connectors.spark.offset;

import com.solacecoe.connectors.spark.streaming.solace.SolaceMessage;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class SolaceSparkOffsetManager {
    private final static ConcurrentHashMap<String, SolaceMessage> messages = new ConcurrentHashMap<>();

    public static String getProcessedMessages() {
        return String.join(",", new ArrayList<>(messages.keySet()));
    }

    public static void addMessage(String messageID, SolaceMessage message) {
        messages.put(messageID, message);
    }

    public static boolean containsKey(String messageID) {
        return messages.containsKey(messageID);
    }

    public static ConcurrentHashMap<String, SolaceMessage> getMessages() {
        return messages;
    }
}
