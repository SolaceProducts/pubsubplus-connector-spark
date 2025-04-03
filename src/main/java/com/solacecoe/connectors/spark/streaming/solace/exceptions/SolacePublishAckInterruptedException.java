package com.solacecoe.connectors.spark.streaming.solace.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolacePublishAckInterruptedException extends RuntimeException {
    private static final Logger log = LoggerFactory.getLogger(SolacePublishAckInterruptedException.class);
    public SolacePublishAckInterruptedException(String message, Throwable cause) {
        super(cause);
        log.error(message, cause);
    }
}
