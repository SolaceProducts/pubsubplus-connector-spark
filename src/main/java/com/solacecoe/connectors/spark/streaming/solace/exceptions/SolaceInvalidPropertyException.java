package com.solacecoe.connectors.spark.streaming.solace.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceInvalidPropertyException extends RuntimeException{
    private final transient Logger log = LoggerFactory.getLogger(SolaceInvalidPropertyException.class);
    public SolaceInvalidPropertyException(String message) {
        super(message);
        log.error(message);
    }
}
