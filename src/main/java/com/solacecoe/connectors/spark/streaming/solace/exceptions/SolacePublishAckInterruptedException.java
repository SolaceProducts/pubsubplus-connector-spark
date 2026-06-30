package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolacePublishAckInterruptedException extends RuntimeException {
    public SolacePublishAckInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
