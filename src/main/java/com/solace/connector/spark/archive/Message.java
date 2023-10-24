package com.solace.connector.spark.archive;

import com.solacesystems.jcsmp.BytesXMLMessage;

import java.io.Serializable;
import java.time.Instant;

public class Message implements Serializable {
    public transient BytesXMLMessage message;
    public transient Instant time;

    public Message(BytesXMLMessage message, Instant time) {
        this.message = message;
        this.time = time;
    }
}
