package com.solacecoe.connectors.spark.streaming.solace.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolacePublishAckTimeoutException extends RuntimeException {
    private static final Logger log = LoggerFactory.getLogger(SolacePublishAckTimeoutException.class);
    public SolacePublishAckTimeoutException(String message, Throwable cause) {
        super(cause);
        log.error(message, cause);
    }
}
