package com.solacecoe.connectors.spark.streaming.solace.exceptions;

public class SolacePublishAbortException extends RuntimeException{
    public SolacePublishAbortException(Throwable cause) {
        super(cause);
    }

    public SolacePublishAbortException(String cause) {
        super(cause);
    }
}
