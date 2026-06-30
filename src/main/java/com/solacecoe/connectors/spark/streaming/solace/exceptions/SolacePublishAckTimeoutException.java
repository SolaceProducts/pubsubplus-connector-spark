package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolacePublishAckTimeoutException extends RuntimeException {
    public SolacePublishAckTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
