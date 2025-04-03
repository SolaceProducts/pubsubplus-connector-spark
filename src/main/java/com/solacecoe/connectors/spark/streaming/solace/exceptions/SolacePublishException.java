package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolacePublishException extends RuntimeException{
    public SolacePublishException(String cause) {
        super(cause);
    }

    public SolacePublishException(Throwable cause) {
        super(cause);
    }
}
